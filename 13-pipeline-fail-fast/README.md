# 13 — Pipeline with Fail-Fast Error Handling

Extend a parallel-worker pipeline so that any item-level error in any worker
stops the whole pipeline as fast as possible and returns that error to the
caller — using a dedicated error channel plus a `done` channel. No `context`
package yet.

## Signatures

```go
// Process fans inputs out to workers goroutines, each calling transform.
// If transform returns an error for any item, Process cancels the remaining
// work by closing a done channel, and returns the first error received.
// If no error occurs, Process returns all transformed strings (order not
// guaranteed) and a nil error.
//
// workers must be >= 1.
func Process(inputs []string, workers int, transform func(string) (string, error)) ([]string, error)
```

## Constraints

- On error: Process must return **the first** error; it must not hang waiting
  for in-flight workers that already finished.
- On error: every worker goroutine must exit (no leaks after Process returns).
- On success: all transformed strings are returned with a nil error.
- The done channel must be closed (not sent on) to broadcast cancellation to
  all workers simultaneously.
- Must be race-free under `-race`.

## Concepts

Error propagation and fail-fast cancellation in a pipeline, done channel,
buffered error channel.

## Gotchas

- Multiple workers hitting errors simultaneously all try to send on the error
  channel — use a buffered error channel of size 1, or a `sync.Once` around
  the close, so the first error wins and the rest are discarded.
- A worker that never selects on `done` will block trying to send on a full
  results channel long after cancellation, causing a goroutine leak.
- Closing `done` inside a `select` default branch instead of a `sync.Once`
  can cause a double-close panic if two workers error at the same time.
