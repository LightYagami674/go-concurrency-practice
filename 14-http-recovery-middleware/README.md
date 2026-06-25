# 14 — HTTP Server with Recovery Middleware

Build a tiny HTTP server with a couple of handlers, one of which panics on
certain input. Write middleware that recovers panics per-request, logs the
stack trace, returns a 500, and keeps the server running for subsequent
requests.

## Signatures

```go
// RecoveryMiddleware wraps next. If the handler panics, the middleware
// recovers, writes the stack trace to log, returns HTTP 500 to the client,
// and allows the server to continue serving subsequent requests.
func RecoveryMiddleware(log io.Writer, next http.Handler) http.Handler

// NewServer returns an http.Handler with two routes:
//   GET /hello  — always responds 200 with body "hello"
//   GET /panic  — panics with the string "intentional panic"
// All routes are wrapped with RecoveryMiddleware(log, ...).
func NewServer(log io.Writer) http.Handler
```

## Constraints

- A request to `/panic` must return HTTP 500; the server must then serve
  subsequent `/hello` requests normally (status 200).
- The stack trace must be written to the provided `io.Writer`.
- `recover()` must be called inside a `defer` in the **same goroutine** as
  the handler — Go's HTTP server already gives each request its own goroutine,
  so the middleware's defer is in the right goroutine.
- Must be race-free under `-race`.

## Concepts

panic / recover / defer, per-request goroutine isolation, HTTP middleware.

## Gotchas

- `recover()` only works in a deferred function in the **same goroutine** as
  the panic — a panic in a goroutine spawned inside the handler won't be caught
  by the middleware's defer.
- Recovery doesn't undo shared-state mutations that happened before the panic.
- Writing the response body after a `WriteHeader` call is a no-op — call
  `WriteHeader(500)` before writing the error body.
