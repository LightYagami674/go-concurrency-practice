package apiratelimiter

import "time"

// RateLimiter allows at most r calls/second with a burst of up to b, using a
// token-bucket channel refilled by a background goroutine.
//
// TODO: implement. Suggested fields:
//   - tokens chan struct{} // buffered to capacity b (the burst); Wait receives
//   - done   chan struct{} // closed by Stop to end the refill goroutine
//   - (the per-token interval is time.Second / time.Duration(r))
type RateLimiter struct {
	// TODO: fields.
	tokens chan struct{}
	done   chan struct{}
	r      int
}

// NewRateLimiter returns a limiter permitting a sustained rate of r tokens per
// second and a burst capacity of b. r >= 1 and b >= 1. The bucket starts full
// (b tokens), so the first b calls return immediately. A background goroutine
// begins refilling at one token every 1/r seconds.
//
// TODO: make tokens = make(chan struct{}, b); pre-fill it with b tokens; start
// the refill goroutine; return the limiter.
func NewRateLimiter(r, b int) *RateLimiter {
	tokens := make(chan struct{}, b)
	done := make(chan struct{})

	for range b {
		tokens <- struct{}{}
	}

	rl := RateLimiter{
		tokens: tokens,
		done:   done,
		r:      r,
	}

	interval := 1.0 / float64(r)
	ns := int64(interval * float64(time.Second))
	go rl.refill(ns)

	return &rl
}

// refill is the background loop. It sleeps one interval, then tries to add a
// single token with a NON-BLOCKING send (drop if the bucket is full), and exits
// when done is closed.
//
// TODO: for { check done -> return; time.Sleep(interval); select { case tokens
// <- struct{}{}: default: } }
func (rl *RateLimiter) refill(interval int64) {
	for {
		time.Sleep(time.Duration(interval))
		select {
		case _, ok := <-rl.done:
			if !ok {
				return
			}
		default:
			select {
			case rl.tokens <- struct{}{}:
			default:
			}
		}
	}
}

// Wait blocks until a token is available, then consumes it and returns.
//
// TODO: <-rl.tokens
func (rl *RateLimiter) Wait() {
	<-rl.tokens
}

// Stop shuts down the refill goroutine cleanly. After Stop, no more tokens are
// added. Call it exactly once when the limiter is no longer needed.
//
// TODO: close(rl.done)
func (rl *RateLimiter) Stop() {
	close(rl.done)
}
