# 10 — Async Buffered Pipeline Stage (Lossy)

Covers: **non-blocking channel operations** and **buffering to decouple stages**.
Extends the pipeline from #09: the `square` stage is now artificially slow, so
the generator must not block on it indefinitely. A fixed-size buffer absorbs
bursts, and when it's full the generator **drops** the item (via a non-blocking
`select`/`default`) and counts the loss instead of stalling.

## Problem

Reuse the `generate → square → filterEven → collect` shape, but:

- **`square` is slow** — it sleeps `slowPerItem` per value before emitting the
  square, simulating an expensive transform.
- **The generator must never block indefinitely.** It feeds a **buffered**
  channel of a fixed size. When that buffer is full (because the slow stage is
  behind), the generator performs a **non-blocking send**: if the send would
  block, it **drops** the value and increments a counter, then moves on to the
  next number.
- **Loss is visible.** The number of dropped items is reported, so data is never
  silently lost.

This trades completeness for liveness: under overload the generator sheds load
rather than wedging the whole pipeline.

## API to implement (in `solution.go`)

```go
// LossyResult is the outcome of one run of the lossy pipeline.
type LossyResult struct {
	Output    []int // even squares that made it all the way through, in input order
	Processed int   // count of numbers that passed the buffer and were squared
	Dropped   int   // count of numbers dropped because the buffer was full
}

// Run streams 1..n from the generator into a buffered channel of size bufSize.
// The square stage sleeps slowPerItem per value. When the buffer is full the
// generator drops the value (non-blocking send) and counts it, instead of
// blocking. Returns the collected even squares plus processed/dropped counts.
//
// Invariant: Processed + Dropped == n (every generated number is either
// processed or dropped, exactly once).
func Run(n, bufSize int, slowPerItem time.Duration) LossyResult
```

## Requirements

1. **Generator bounded by a fixed buffer** — the generator → square channel is
   `make(chan int, bufSize)`. The generator never blocks indefinitely on it.
2. **Non-blocking send handles the full case** — use
   `select { case ch <- v: ...; default: dropped++ }` (or equivalent) so a full
   buffer causes a drop, not a stall.
3. **Drops are counted and reported** — `Dropped` reflects exactly how many
   generated numbers were shed; nothing is lost silently.
4. **Accounting is exact** — `Processed + Dropped == n`. Every survivor is
   squared and flows downstream; `Output` is the even squares among the
   survivors, in input order.
5. **Clean termination** — each stage closes its output once its input drains;
   no goroutine leaks; the run finishes promptly even when `square` is slow
   (because the generator drops rather than waits).
6. **Race- & deadlock-free** — clean under `-race`; never hangs.

## Gotchas to avoid

- **Busy-spinning on `select`/`default`:** a loop that keeps *retrying the same
  send* with a `default` branch and no backoff burns a CPU core hot. Advance to
  the next item on a drop (one new value per iteration), or add a small backoff —
  don't spin on the same value.
- **Silent data loss:** dropping items with no counter means you can't tell how
  much was lost or whether the buffer is sized right. Always surface the drop
  count (here, `Dropped`).
- **Blocking send defeats the point:** a plain `ch <- v` (no `select`/`default`)
  makes the generator wait on the slow stage — the pipeline then runs at the slow
  stage's pace and `Dropped` is always 0. The whole exercise is the non-blocking
  send.
- **Forgetting to close outputs / leaking goroutines:** as in #09, each stage
  must `close` its output when its input drains so downstream terminates.

## Running

```bash
go test -race ./10-async-buffered-stage/...
go vet ./10-async-buffered-stage/...
```
