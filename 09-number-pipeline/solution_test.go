package numberpipeline

import (
	"fmt"
	"runtime"
	"testing"
	"time"
)

// runWithTimeout fails instead of hanging forever if a stage forgets to close
// its output channel (the downstream range then blocks indefinitely).
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
		t.Fatalf("test did not finish within %v (a stage never closed its output?)", timeout)
	}
}

// wantEvenSquares returns the even squares of 1..n in order.
func wantEvenSquares(n int) []int {
	var want []int
	for i := 1; i <= n; i++ {
		sq := i * i
		if sq%2 == 0 {
			want = append(want, sq)
		}
	}
	return want
}

func TestPipelineValues(t *testing.T) {
	cases := []int{0, 1, 2, 3, 4, 5, 10, 100}
	for _, n := range cases {
		n := n
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			var got []int
			runWithTimeout(t, 10*time.Second, func() {
				got = Pipeline(n)
			})
			want := wantEvenSquares(n)
			if len(got) != len(want) {
				t.Fatalf("Pipeline(%d) returned %d values (%v), want %d (%v)", n, len(got), got, len(want), want)
			}
			for i := range want {
				if got[i] != want[i] {
					t.Fatalf("Pipeline(%d)[%d] = %d, want %d (full: %v vs %v)", n, i, got[i], want[i], got, want)
				}
			}
		})
	}
}

// TestPipelineOrdered confirms the output is in increasing input order: even
// squares only come from even inputs, so the sequence is 4, 16, 36, 64, ...
func TestPipelineOrdered(t *testing.T) {
	var got []int
	runWithTimeout(t, 10*time.Second, func() {
		got = Pipeline(20)
	})
	want := []int{4, 16, 36, 64, 100, 144, 196, 256, 324, 400}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got[%d] = %d, want %d (%v)", i, got[i], want[i], got)
		}
	}
}

// TestStagesCloseTheirOutputs exercises each stage in isolation: feeding a
// closed input must cause the stage's output channel to close (its range loop
// ends), otherwise this would hang.
func TestStagesCloseTheirOutputs(t *testing.T) {
	runWithTimeout(t, 5*time.Second, func() {
		// generate closes its output after emitting 1..n.
		got := drain(generate(3))
		if !equal(got, []int{1, 2, 3}) {
			t.Fatalf("generate(3) = %v, want [1 2 3]", got)
		}

		// square over a closed input must close its output.
		in := sliceToChan(2, 3, 4)
		sq := drain(square(in))
		if !equal(sq, []int{4, 9, 16}) {
			t.Fatalf("square = %v, want [4 9 16]", sq)
		}

		// filterEven over a closed input must close its output.
		in2 := sliceToChan(1, 2, 3, 4, 5, 6)
		fe := drain(filterEven(in2))
		if !equal(fe, []int{2, 4, 6}) {
			t.Fatalf("filterEven = %v, want [2 4 6]", fe)
		}
	})
}

// TestEmptyInputPropagates: an empty (immediately closed) source must drain
// through every stage and close the final output without blocking.
func TestEmptyInputPropagates(t *testing.T) {
	runWithTimeout(t, 5*time.Second, func() {
		got := Pipeline(0)
		if len(got) != 0 {
			t.Fatalf("Pipeline(0) = %v, want empty", got)
		}

		// Also a stage chained on an empty channel.
		empty := sliceToChan() // closed, no values
		out := drain(filterEven(square(empty)))
		if len(out) != 0 {
			t.Fatalf("empty chain = %v, want empty", out)
		}
	})
}

// TestNoGoroutineLeak runs the full pipeline many times and confirms goroutines
// don't accumulate — i.e. every stage's goroutine exits when its input drains.
func TestNoGoroutineLeak(t *testing.T) {
	base := settleGoroutines(runtime.NumGoroutine(), 2*time.Second)

	runWithTimeout(t, 20*time.Second, func() {
		for i := 0; i < 200; i++ {
			_ = Pipeline(50)
		}
	})

	if n := settleGoroutines(base, 3*time.Second); n > base {
		t.Fatalf("goroutines leaked: %d after running pipelines, base %d (a stage goroutine never exited?)", n, base)
	}
}

// --- small helpers -----------------------------------------------------------

func drain(ch <-chan int) []int {
	var out []int
	for v := range ch {
		out = append(out, v)
	}
	return out
}

func sliceToChan(vs ...int) <-chan int {
	ch := make(chan int)
	go func() {
		defer close(ch)
		for _, v := range vs {
			ch <- v
		}
	}()
	return ch
}

func equal(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
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
