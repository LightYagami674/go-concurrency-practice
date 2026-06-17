package apiratelimiter

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// runWithTimeout fails instead of hanging forever if Wait blocks with no token
// ever arriving (e.g. a refiller that never runs, or a deadlocked Stop).
func runWithTimeout(t *testing.T, timeout time.Duration, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		fn()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatalf("test did not finish within %v (no tokens / deadlock?)", timeout)
	}
}

// TestBurstIsImmediate verifies the bucket starts full: the first b calls return
// essentially instantly (far faster than the sustained rate would allow).
func TestBurstIsImmediate(t *testing.T) {
	const (
		r = 50 // 20ms per token
		b = 5
	)
	rl := NewRateLimiter(r, b)
	defer rl.Stop()

	runWithTimeout(t, 5*time.Second, func() {
		start := time.Now()
		for i := 0; i < b; i++ {
			rl.Wait()
		}
		elapsed := time.Since(start)

		// At the sustained rate, b calls would take ~b/r = 100ms. The burst must
		// be much faster since the bucket starts full.
		sustained := time.Duration(b) * time.Second / time.Duration(r)
		if elapsed > sustained/2 {
			t.Fatalf("burst of %d took %v, want well under %v (bucket not pre-filled?)", b, elapsed, sustained/2)
		}
	})
}

// TestSustainedRate checks that, once the burst is spent, throughput is limited
// to ~R/sec: completing N calls takes at least (N-b)/r seconds.
func TestSustainedRate(t *testing.T) {
	const (
		r = 50 // 20ms per token
		b = 5
		n = 20 // 15 calls beyond the burst => >= 15/50 = 300ms minimum
	)
	rl := NewRateLimiter(r, b)
	defer rl.Stop()

	runWithTimeout(t, 10*time.Second, func() {
		start := time.Now()
		for i := 0; i < n; i++ {
			rl.Wait()
		}
		elapsed := time.Since(start)

		// Lower bound: the (n-b) non-burst tokens each need a refill interval.
		minExpected := time.Duration(n-b) * time.Second / time.Duration(r)
		// Allow scheduling slack on the low side; if it finishes much faster the
		// limiter is letting calls through above the rate.
		if elapsed < time.Duration(float64(minExpected)*0.8) {
			t.Fatalf("%d calls finished in %v, faster than the rate allows (min ~%v) — rate exceeded",
				n, elapsed, minExpected)
		}
		// Sanity upper bound: shouldn't be absurdly slow (catches over-throttling).
		if elapsed > minExpected*4+time.Second {
			t.Fatalf("%d calls took %v, far slower than expected ~%v — over-throttled", n, elapsed, minExpected)
		}
	})
}

// TestWaitBlocksWhenEmpty drains the burst, then verifies the next Wait blocks
// until the refiller delivers a token (rather than returning immediately).
func TestWaitBlocksWhenEmpty(t *testing.T) {
	const (
		r = 20 // 50ms per token
		b = 2
	)
	rl := NewRateLimiter(r, b)
	defer rl.Stop()

	// Spend the burst.
	for i := 0; i < b; i++ {
		rl.Wait()
	}

	returned := make(chan struct{})
	go func() {
		rl.Wait() // bucket empty: must block until a refill arrives
		close(returned)
	}()

	interval := time.Second / time.Duration(r)
	// It must NOT return well before one refill interval.
	select {
	case <-returned:
		t.Fatal("Wait returned immediately on an empty bucket (no blocking / burst too large)")
	case <-time.After(interval / 2):
	}
	// It MUST return once a refill (or two) has had time to land.
	select {
	case <-returned:
	case <-time.After(interval*4 + time.Second):
		t.Fatal("Wait never unblocked (refiller not running?)")
	}
}

// TestConcurrentCallers hammers Wait from many goroutines. Under -race this must
// be clean, total throughput stays bounded by the rate, and nothing deadlocks.
func TestConcurrentCallers(t *testing.T) {
	const (
		r        = 100 // 10ms per token
		b        = 10
		callers  = 50
		perCall  = 4
		expected = callers * perCall // 200 tokens
	)
	rl := NewRateLimiter(r, b)
	defer rl.Stop()

	var got int64
	start := time.Now()
	runWithTimeout(t, 20*time.Second, func() {
		var wg sync.WaitGroup
		for c := 0; c < callers; c++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < perCall; i++ {
					rl.Wait()
					atomic.AddInt64(&got, 1)
				}
			}()
		}
		wg.Wait()
	})
	elapsed := time.Since(start)

	if got != expected {
		t.Fatalf("completed %d calls, want %d", got, expected)
	}
	// Lower bound on time: (expected - b) tokens at the sustained rate.
	minExpected := time.Duration(expected-b) * time.Second / time.Duration(r)
	if elapsed < time.Duration(float64(minExpected)*0.8) {
		t.Fatalf("%d concurrent calls finished in %v, faster than rate allows (min ~%v)",
			expected, elapsed, minExpected)
	}
}

// waitForGoroutineCount polls until NumGoroutine drops to <= target or the
// timeout elapses, returning the last observed count.
func waitForGoroutineCount(target int, timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	for {
		runtime.GC()
		n := runtime.NumGoroutine()
		if n <= target || time.Now().After(deadline) {
			return n
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestStopNoGoroutineLeak verifies the refill goroutine exits after Stop.
func TestStopNoGoroutineLeak(t *testing.T) {
	// Let any goroutines from earlier subtests settle first.
	base := waitForGoroutineCount(runtime.NumGoroutine(), 2*time.Second)

	rl := NewRateLimiter(100, 5)
	rl.Wait()

	if n := runtime.NumGoroutine(); n <= base {
		t.Fatalf("expected a background goroutine after NewRateLimiter: had %d, base %d", n, base)
	}

	rl.Stop()

	// After Stop (allowing up to a refill interval), the refiller must be gone.
	if n := waitForGoroutineCount(base, 3*time.Second); n > base {
		t.Fatalf("refill goroutine leaked: %d goroutines after Stop, base %d", n, base)
	}
}
