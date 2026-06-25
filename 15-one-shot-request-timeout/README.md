# 15 — One-Shot Request Timeout

Simulate a request whose response arrives on a channel after a random delay.
If it doesn't arrive within duration D, give up and report a timeout — using
`time.NewTimer` and `select`. No `context` package yet.

## Signatures

```go
type RequestResult struct {
    Value    string // populated when TimedOut == false
    TimedOut bool
}

// FetchWithTimeout calls fetch in a background goroutine. It selects between
// the result and the timer:
//   - result arrives first: call timer.Stop(), return RequestResult{Value: v}.
//   - timer fires first:    return RequestResult{TimedOut: true}.
//
// The background goroutine spawned for fetch is allowed to outlive
// FetchWithTimeout when a timeout occurs (fetch may be blocking on I/O with
// no cancellation mechanism), but must eventually exit on its own.
func FetchWithTimeout(fetch func() string, timeout time.Duration) RequestResult
```

## Constraints

- When the result wins, `timer.Stop()` must be called to free the timer resource.
- When the timer wins, the function returns immediately without waiting for fetch.
- Must clearly distinguish "got result" (`TimedOut == false`) from "timed out"
  (`TimedOut == true`).
- Must be race-free under `-race`.

## Concepts

`time.NewTimer`, `select`, resource cleanup.

## Gotchas

- Not calling `timer.Stop()` when the result arrives first — the timer fires
  uselessly and the GC can't collect it until it fires.
- Using `time.After` in a loop — each call allocates a new timer that cannot
  be stopped, accumulating until the GC collects them after they fire.
- Blocking on the fetch goroutine after a timeout — `FetchWithTimeout` should
  return immediately; the goroutine finishing later is acceptable because fetch
  has no cancellation channel.
