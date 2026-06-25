package oneshotrequesttimeout

import "time"

// RequestResult is the outcome of FetchWithTimeout.
type RequestResult struct {
	Value    string // populated when TimedOut == false
	TimedOut bool
}

// FetchWithTimeout calls fetch in a background goroutine and races it against
// a timer of duration timeout.
//
// Suggested shape:
//   ch    := make(chan string, 1)   // buffered so the goroutine never leaks on timeout
//   timer := time.NewTimer(timeout)
//   go func() { ch <- fetch() }()
//   select {
//   case v := <-ch:
//       timer.Stop()
//       return RequestResult{Value: v}
//   case <-timer.C:
//       return RequestResult{TimedOut: true}
//   }
func FetchWithTimeout(fetch func() string, timeout time.Duration) RequestResult {
	panic("not implemented")
}
