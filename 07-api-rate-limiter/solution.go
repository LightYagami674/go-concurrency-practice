package apiratelimiter

// RateLimiter allows at most r calls/second with a burst of up to b, using a
// token-bucket channel refilled by a background goroutine.
//
// TODO: implement. Suggested fields:
//   - tokens chan struct{} // buffered to capacity b (the burst); Wait receives
//   - done   chan struct{} // closed by Stop to end the refill goroutine
//   - (the per-token interval is time.Second / time.Duration(r))
type RateLimiter struct {
	// TODO: fields.
}

// NewRateLimiter returns a limiter permitting a sustained rate of r tokens per
// second and a burst capacity of b. r >= 1 and b >= 1. The bucket starts full
// (b tokens), so the first b calls return immediately. A background goroutine
// begins refilling at one token every 1/r seconds.
//
// TODO: make tokens = make(chan struct{}, b); pre-fill it with b tokens; start
// the refill goroutine; return the limiter.
func NewRateLimiter(r, b int) *RateLimiter {
	panic("not implemented")
}

// refill is the background loop. It sleeps one interval, then tries to add a
// single token with a NON-BLOCKING send (drop if the bucket is full), and exits
// when done is closed.
//
// TODO: for { check done -> return; time.Sleep(interval); select { case tokens
// <- struct{}{}: default: } }
func (rl *RateLimiter) refill(interval int64) {
	panic("not implemented")
}

// Wait blocks until a token is available, then consumes it and returns.
//
// TODO: <-rl.tokens
func (rl *RateLimiter) Wait() {
	panic("not implemented")
}

// Stop shuts down the refill goroutine cleanly. After Stop, no more tokens are
// added. Call it exactly once when the limiter is no longer needed.
//
// TODO: close(rl.done)
func (rl *RateLimiter) Stop() {
	panic("not implemented")
}
