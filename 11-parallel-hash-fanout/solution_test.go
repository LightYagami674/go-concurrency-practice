package parallelhashfanout

import (
	"crypto/sha256"
	"fmt"
	"runtime"
	"testing"
	"time"
)

func hexSHA256(s string) string {
	sum := sha256.Sum256([]byte(s))
	return fmt.Sprintf("%x", sum)
}

func fanOutWithTimeout(t *testing.T, timeout time.Duration, inputs []string, workers int) []HashResult {
	t.Helper()
	type out struct {
		res []HashResult
	}
	ch := make(chan out, 1)
	go func() {
		ch <- out{FanOutHashIn(inputs, workers)}
	}()
	select {
	case o := <-ch:
		return o.res
	case <-time.After(timeout):
		t.Fatalf("FanOutHashIn did not return within %v (worker leak / channel never closed?)", timeout)
		return nil
	}
}

// TestAllItemsPresent verifies no input is silently dropped.
func TestAllItemsPresent(t *testing.T) {
	inputs := make([]string, 100)
	for i := range inputs {
		inputs[i] = fmt.Sprintf("item-%d", i)
	}

	results := fanOutWithTimeout(t, 5*time.Second, inputs, 8)

	if len(results) != len(inputs) {
		t.Fatalf("got %d results, want %d", len(results), len(inputs))
	}

	seen := make(map[string]bool)
	for _, r := range results {
		seen[r.Input] = true
	}
	for _, item := range inputs {
		if !seen[item] {
			t.Errorf("input %q missing from results", item)
		}
	}
}

// TestCorrectHashes verifies each HashResult carries the right SHA-256 digest.
func TestCorrectHashes(t *testing.T) {
	inputs := []string{"alpha", "beta", "gamma", "delta", "epsilon"}

	results := fanOutWithTimeout(t, 5*time.Second, inputs, 3)

	for _, r := range results {
		want := hexSHA256(r.Input)
		if r.Hash != want {
			t.Errorf("Hash(%q) = %q, want %q", r.Input, r.Hash, want)
		}
	}
}

// TestSingleWorker ensures the implementation works with workers == 1.
func TestSingleWorker(t *testing.T) {
	inputs := []string{"a", "b", "c"}
	results := fanOutWithTimeout(t, 5*time.Second, inputs, 1)
	if len(results) != 3 {
		t.Fatalf("got %d results, want 3", len(results))
	}
}

// TestEmptyInput ensures an empty input slice returns an empty result without hanging.
func TestEmptyInput(t *testing.T) {
	results := fanOutWithTimeout(t, 5*time.Second, nil, 4)
	if len(results) != 0 {
		t.Fatalf("got %d results for empty input, want 0", len(results))
	}
}

// TestHighConcurrency hammers the function with many goroutines to surface races.
func TestHighConcurrency(t *testing.T) {
	inputs := make([]string, 500)
	for i := range inputs {
		inputs[i] = fmt.Sprintf("payload-%d", i)
	}

	results := fanOutWithTimeout(t, 10*time.Second, inputs, 32)

	if len(results) != len(inputs) {
		t.Fatalf("got %d results, want %d", len(results), len(inputs))
	}
	for _, r := range results {
		if r.Hash != hexSHA256(r.Input) {
			t.Errorf("wrong hash for %q", r.Input)
		}
	}
}

// TestNoGoroutineLeak confirms workers and the closer goroutine all exit.
func TestNoGoroutineLeak(t *testing.T) {
	base := settleGoroutines(runtime.NumGoroutine(), 2*time.Second)

	for i := 0; i < 50; i++ {
		inputs := make([]string, 40)
		for j := range inputs {
			inputs[j] = fmt.Sprintf("run%d-item%d", i, j)
		}
		fanOutWithTimeout(t, 5*time.Second, inputs, 4)
	}

	if got := settleGoroutines(base, 3*time.Second); got > base {
		t.Fatalf("goroutine leak: %d after runs, base %d", got, base)
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
