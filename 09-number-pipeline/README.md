# 09 — Number Processing Pipeline

Covers: the **pipeline pattern** — a chain of stages, each its own goroutine,
connected by channels, where every stage closes its output channel once its
input is closed and drained, so termination flows cleanly from the source to the
final consumer.

## Problem

Build a 3-stage number pipeline:

```
generate(n) ──chan──► square ──chan──► filterEven ──chan──► consumer
  (1..n)            (x => x*x)       (keep evens)         (collect)
```

1. **generate** — emits the integers `1, 2, …, n` onto its output channel.
2. **square** — reads each number and emits its square.
3. **filterEven** — reads each squared value and emits only the even ones.

A final consumer drains the last stage. Each stage runs in its **own
goroutine**, reads from its input channel with `for v := range in`, and **closes
its output channel exactly once** when its input is exhausted. Closing each stage
the moment its input drains is what lets the whole pipeline shut down on its own.

## API to implement (in `solution.go`)

```go
// generate returns a channel that emits 1..n and is then closed.
func generate(n int) <-chan int

// square returns a channel of the squares of the values read from in, closed
// once in is closed and drained.
func square(in <-chan int) <-chan int

// filterEven returns a channel of only the even values read from in, closed once
// in is closed and drained.
func filterEven(in <-chan int) <-chan int

// Pipeline wires generate -> square -> filterEven and returns the collected
// output (the even squares of 1..n, in order).
func Pipeline(n int) []int
```

Each of `generate`, `square`, and `filterEven` launches one goroutine that owns
(and closes) its output channel. `Pipeline` connects them and drains the final
stage.

## Requirements

1. **Each stage a separate goroutine** — `generate`, `square`, and `filterEven`
   each start exactly one goroutine that produces onto a channel they return
   immediately (they do not block the caller).
2. **Each stage closes its own output once** — when a stage's input channel is
   closed and fully drained, the stage closes its output channel exactly once
   (idiomatically via `defer close(out)`), and only that stage closes it.
3. **A stage owns only its output** — a stage never closes its *input*; the
   upstream owner does that. No channel is closed twice.
4. **Clean termination** — with finite input the entire pipeline drains and all
   goroutines exit; nothing leaks or blocks.
5. **Correct, ordered output** — `Pipeline(n)` returns the even squares of
   `1..n` in increasing input order. (Squares of even numbers are even; squares
   of odd numbers are odd — so the result is `4, 16, 36, …`.)
6. **Race- & deadlock-free** — clean under `-race`; never hangs.

## Gotchas to avoid

- **Forgetting to close an output channel:** if a stage stops sending but never
  closes its output, the downstream `for range` blocks forever waiting for more
  values → the pipeline never terminates. Use `defer close(out)` in each stage's
  goroutine.
- **Double-closing a channel:** closing a channel that's already closed panics.
  Each channel has exactly one owner (the stage that produces it); only that
  owner closes it, exactly once. A stage must not close its input.
- **Returning before the goroutine starts producing:** a stage should create its
  output channel, launch the goroutine, and return the channel immediately — not
  do the work synchronously and not return a nil channel.
- **Leaking on early exit:** (not required here since input is finite and fully
  consumed) but note that abandoning a stage mid-stream without draining would
  block its upstream — full consumption keeps it clean.

## Running

```bash
go test -race ./09-number-pipeline/...
go vet ./09-number-pipeline/...
```
