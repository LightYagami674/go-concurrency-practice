# 07 — API Rate Limiter (Token Bucket)

Covers: rate limiting with a **token-bucket channel** — a buffered channel of
tokens, refilled by a background goroutine on a timer loop, where callers block
until a token is available.

## Problem

Build a rate limiter that allows at most **R calls per second**, with an optional
**burst of B**. Tokens live in a buffered channel; a caller takes one token per
call and **blocks** when none are available. A background goroutine refills
tokens on a steady cadence so the sustained rate settles at R/sec, while up to B
calls can proceed immediately after an idle period (the burst).

For this exercise, drive the refill loop with **plain `time.Sleep`**, not
`time.Ticker` (tickers are a later topic). The refill goroutine must be
**stoppable** so it doesn't leak when the limiter is no longer needed.

## API to implement (in `solution.go`)

```go
// RateLimiter allows at most r calls/second with a burst of up to b, using a
// token-bucket channel refilled by a background goroutine.
type RateLimiter struct { /* ... */ }

// NewRateLimiter returns a limiter permitting a sustained rate of r tokens per
// second and a burst capacity of b. r >= 1 and b >= 1. The bucket starts full
// (b tokens), so the first b calls return immediately. A background goroutine
// begins refilling at one token every 1/r seconds.
func NewRateLimiter(r, b int) *RateLimiter

// Wait blocks until a token is available, then consumes it and returns.
func (rl *RateLimiter) Wait()

// Stop shuts down the refill goroutine cleanly. After Stop, no more tokens are
// added. Call it exactly once when the limiter is no longer needed.
func (rl *RateLimiter) Stop()
```

## Requirements

1. **Token-bucket channel** — tokens are held in a buffered channel whose
   **buffer size equals the burst B**. `Wait` receives from it; the refiller
   sends to it.
2. **Burst** — the bucket starts full, so up to **B** calls proceed immediately
   after construction (or after an idle gap that let the bucket refill).
3. **Sustained rate ≤ R/sec** — over a sustained run the throughput converges to
   R tokens/second; the refiller adds one token roughly every `1/R` seconds and
   never lets the bucket exceed B (extra tokens are dropped, not queued).
4. **Blocking** — when the bucket is empty, `Wait` blocks until the refiller adds
   a token; no busy-wait, no spinning.
5. **Clean shutdown / no leak** — `Stop` terminates the refill goroutine; after
   it returns (allowing up to one refill interval) the goroutine is gone. No
   goroutine leak.
6. **Race- & deadlock-free** — safe under `-race` with many concurrent callers.

## Gotchas to avoid

- **Wrong buffer size:** the token channel's buffer size **is** the burst size.
  An unbuffered channel gives zero burst (every call waits for a fresh token); an
  oversized buffer lets too many calls through at once. Size it to B.
- **No shutdown path:** a refill goroutine that loops forever with no way to stop
  leaks — it keeps running (and holding the limiter alive) after you're done.
  Give it a `done`/`stop` channel and check it each iteration.
- **Refiller blocking on a full bucket:** if the refiller does a blocking send
  into a full channel, it stalls and can wedge shutdown. Use a non-blocking send
  (`select { case ch <- token: default: }`) so a full bucket just drops the
  token.
- **Overfilling past burst:** adding tokens without respecting capacity lets the
  bucket exceed B, breaking the burst guarantee. The bounded buffer plus
  non-blocking send keeps it capped.

## Running

```bash
go test -race ./07-api-rate-limiter/...
go vet ./07-api-rate-limiter/...
```
