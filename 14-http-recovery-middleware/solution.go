package httprecoverymiddleware

import (
	"io"
	"net/http"
)

// RecoveryMiddleware wraps next. If next panics, it recovers, writes the
// stack trace to log, and responds with HTTP 500.
//
// Suggested shape:
//   return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
//       defer func() {
//           if rec := recover(); rec != nil {
//               fmt.Fprintf(log, "panic: %v\n%s", rec, debug.Stack())
//               http.Error(w, "internal server error", http.StatusInternalServerError)
//           }
//       }()
//       next.ServeHTTP(w, r)
//   })
func RecoveryMiddleware(log io.Writer, next http.Handler) http.Handler {
	panic("not implemented")
}

// NewServer returns an http.Handler with routes /hello and /panic,
// all wrapped with RecoveryMiddleware.
//
// Routes:
//   GET /hello  → 200 "hello"
//   GET /panic  → panics with "intentional panic"
func NewServer(log io.Writer) http.Handler {
	panic("not implemented")
}
