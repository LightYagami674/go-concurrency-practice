package asyncbufferedstage

import (
	"math"
	"runtime"
	"testing"
	"time"
)

// runWithTimeout fails instead of hanging forever if the pipeline deadlocks
// (e.g. a stage never closes its output, or the generator blocks on a full
// buffer instead of dropping).
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
		t.Fatalf("Run did not finish within %v (generator blocked / output never closed?)", timeout)
	}
}

// assertOutputValid checks Output is a strictly increasing sequence of even
// perfect squares, each the square of an even k in 1..n, with no duplicates.
func assertOutputValid(t *testing.T, n int, out []int) {
	t.Helper()
	prev := -1
	for i, v := range out {
		if v <= prev {
			t.Fatalf("Output not strictly increasing at index %d: %v", i, out)
		}
		prev = v
		if v%2 != 0 {
			t.Fatalf("Output[%d] = %d is odd; even squares only", i, v)
		}
		k := int(math.Round(math.Sqrt(float64(v))))
		if k*k != v {
			t.Fatalf("Output[%d] = %d is not a perfect square", i, v)
		}
		if k%2 != 0 {
			t.Fatalf("Output[%d] = %d is the square of odd %d (should come from even k)", i, v, k)
		}
		if k < 1 || k > n {
			t.Fatalf("Output[%d] = %d comes from k=%d outside 1..%d", i, v, k, n)
		}
	}
}

// TestNoDropsWhenBufferLargeEnough: with a buffer >= n the generator never has
// to drop, so Dropped == 0, Processed == n, and Output is the full set of even
// squares of 1..n — even though the square stage is slow.
func TestNoDropsWhenBufferLargeEnough(t *testing.T) {
	const n = 50
	var res LossyResult
	runWithTimeout(t, 10*time.Second, func() {
		res = Run(n, n, 1*time.Millisecond)
	})

	if res.Dropped != 0 {
		t.Fatalf("Dropped = %d, want 0 (buffer was large enough to hold everything)", res.Dropped)
	}
	if res.Processed != n {
		t.Fatalf("Processed = %d, want %d", res.Processed, n)
	}
	if res.Processed+res.Dropped != n {
		t.Fatalf("Processed+Dropped = %d, want %d", res.Processed+res.Dropped, n)
	}

	// Full expected set of even squares.
	var want []int
	for i := 1; i <= n; i++ {
		if (i*i)%2 == 0 {
			want = append(want, i*i)
		}
	}
	if len(res.Output) != len(want) {
		t.Fatalf("len(Output) = %d, want %d (%v vs %v)", len(res.Output), len(want), res.Output, want)
	}
	for i := range want {
		if res.Output[i] != want[i] {
			t.Fatalf("Output[%d] = %d, want %d", i, res.Output[i], want[i])
		}
	}
}

// TestDropsUnderOverload: a tiny buffer plus a slow square stage and many inputs
// forces drops. We can't predict the exact count, but the accounting must be
// exact and some items must be dropped.
func TestDropsUnderOverload(t *testing.T) {
	const n = 2000
	var res LossyResult
	runWithTimeout(t, 10*time.Second, func() {
		res = Run(n, 1, 1*time.Millisecond)
	})

	if res.Processed+res.Dropped != n {
		t.Fatalf("Processed+Dropped = %d, want %d (accounting wrong)", res.Processed+res.Dropped, n)
	}
	if res.Dropped == 0 {
		t.Fatalf("Dropped = 0 under overload; expected drops (is the send actually non-blocking?)")
	}
	if res.Processed < 1 {
		t.Fatalf("Processed = %d, want at least 1", res.Processed)
	}
	assertOutputValid(t, n, res.Output)
	// Output can only contain even squares among the processed survivors.
	if len(res.Output) > res.Processed {
		t.Fatalf("len(Output) = %d exceeds Processed = %d", len(res.Output), res.Processed)
	}
}

// TestGeneratorDoesNotBlock proves liveness: with a slow square stage, a
// blocking send would make Run take ~n*slowPerItem. The non-blocking generator
// must finish far faster because it drops instead of waiting.
func TestGeneratorDoesNotBlock(t *testing.T) {
	const (
		n    = 3000
		slow = 1 * time.Millisecond
	)
	blockingWouldTake := time.Duration(n) * slow // ~3s if it blocked on every item

	start := time.Now()
	var res LossyResult
	runWithTimeout(t, 10*time.Second, func() {
		res = Run(n, 2, slow)
	})
	elapsed := time.Since(start)

	if elapsed > blockingWouldTake/4 {
		t.Fatalf("Run took %v; a non-blocking generator should finish well under %v (blocked on the slow stage?)",
			elapsed, blockingWouldTake/4)
	}
	if res.Processed+res.Dropped != n {
		t.Fatalf("Processed+Dropped = %d, want %d", res.Processed+res.Dropped, n)
	}
	if res.Dropped == 0 {
		t.Fatalf("Dropped = 0; with buffer 2 and a slow stage, drops were expected")
	}
}

func TestEmptyInput(t *testing.T) {
	runWithTimeout(t, 5*time.Second, func() {
		res := Run(0, 4, time.Millisecond)
		if res.Processed != 0 || res.Dropped != 0 || len(res.Output) != 0 {
			t.Fatalf("Run(0,...) = %+v, want all zero/empty", res)
		}
	})
}

// TestNoGoroutineLeak runs the lossy pipeline repeatedly and confirms goroutines
// don't accumulate — every stage exits when its input drains.
func TestNoGoroutineLeak(t *testing.T) {
	base := settleGoroutines(runtime.NumGoroutine(), 2*time.Second)

	runWithTimeout(t, 20*time.Second, func() {
		for i := 0; i < 100; i++ {
			_ = Run(200, 4, 0) // slowPerItem 0: still exercises the full wiring
		}
	})

	if got := settleGoroutines(base, 3*time.Second); got > base {
		t.Fatalf("goroutines leaked: %d after runs, base %d", got, base)
	}
}

func settleGoroutines(target int, timeout time.Duration) int {
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
