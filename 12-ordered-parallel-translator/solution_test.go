package orderedparalleltranslator

import (
	"fmt"
	"math/rand"
	"runtime"
	"strings"
	"testing"
	"time"
)

func translateWithTimeout(t *testing.T, timeout time.Duration, sentences []string, workers int, translate func(string) string) []string {
	t.Helper()
	ch := make(chan []string, 1)
	go func() {
		ch <- TranslateOrdered(sentences, workers, translate)
	}()
	select {
	case res := <-ch:
		return res
	case <-time.After(timeout):
		t.Fatalf("TranslateOrdered did not return within %v (deadlock / worker leak?)", timeout)
		return nil
	}
}

// identity translate: uppercase the sentence so we can verify correctness.
func upperCase(s string) string { return strings.ToUpper(s) }

// TestOrderPreservedUniformLatency checks basic ordering with uniform delay.
func TestOrderPreservedUniformLatency(t *testing.T) {
	sentences := make([]string, 20)
	for i := range sentences {
		sentences[i] = fmt.Sprintf("sentence %d", i)
	}

	results := translateWithTimeout(t, 10*time.Second, sentences, 5, upperCase)

	if len(results) != len(sentences) {
		t.Fatalf("len(results) = %d, want %d", len(results), len(sentences))
	}
	for i, r := range results {
		want := upperCase(sentences[i])
		if r != want {
			t.Errorf("results[%d] = %q, want %q", i, r, want)
		}
	}
}

// TestOrderPreservedRandomLatency is the critical test: workers finish in
// random order, but output must still match input order exactly.
func TestOrderPreservedRandomLatency(t *testing.T) {
	sentences := make([]string, 50)
	for i := range sentences {
		sentences[i] = fmt.Sprintf("item-%d", i)
	}

	rng := rand.New(rand.NewSource(42))
	var delays [50]time.Duration
	for i := range delays {
		delays[i] = time.Duration(rng.Intn(10)) * time.Millisecond
	}

	slowTranslate := func(s string) string {
		// extract index from "item-N"
		var idx int
		fmt.Sscanf(s, "item-%d", &idx)
		time.Sleep(delays[idx])
		return upperCase(s)
	}

	results := translateWithTimeout(t, 15*time.Second, sentences, 8, slowTranslate)

	if len(results) != len(sentences) {
		t.Fatalf("len(results) = %d, want %d", len(results), len(sentences))
	}
	for i, r := range results {
		want := upperCase(sentences[i])
		if r != want {
			t.Errorf("results[%d] = %q, want %q (ordering broken)", i, r, want)
		}
	}
}

// TestSingleWorker ensures correctness with exactly one worker (no concurrency edge cases).
func TestSingleWorker(t *testing.T) {
	sentences := []string{"alpha", "beta", "gamma"}
	results := translateWithTimeout(t, 5*time.Second, sentences, 1, upperCase)
	if len(results) != 3 {
		t.Fatalf("got %d results, want 3", len(results))
	}
	for i, r := range results {
		if r != upperCase(sentences[i]) {
			t.Errorf("results[%d] = %q, want %q", i, r, upperCase(sentences[i]))
		}
	}
}

// TestEmptyInput returns empty slice without hanging.
func TestEmptyInput(t *testing.T) {
	results := translateWithTimeout(t, 5*time.Second, nil, 4, upperCase)
	if len(results) != 0 {
		t.Fatalf("got %d results for empty input, want 0", len(results))
	}
}

// TestMoreWorkersThanInputs workers > len(sentences): some workers get nothing.
func TestMoreWorkersThanInputs(t *testing.T) {
	sentences := []string{"x", "y"}
	results := translateWithTimeout(t, 5*time.Second, sentences, 10, upperCase)
	if len(results) != 2 {
		t.Fatalf("got %d results, want 2", len(results))
	}
	for i, r := range results {
		if r != upperCase(sentences[i]) {
			t.Errorf("results[%d] = %q, want %q", i, r, upperCase(sentences[i]))
		}
	}
}

// TestNoGoroutineLeak confirms all goroutines exit after TranslateOrdered returns.
func TestNoGoroutineLeak(t *testing.T) {
	base := settleGoroutines(runtime.NumGoroutine(), 2*time.Second)

	for i := 0; i < 50; i++ {
		sentences := make([]string, 20)
		for j := range sentences {
			sentences[j] = fmt.Sprintf("run%d-s%d", i, j)
		}
		translateWithTimeout(t, 5*time.Second, sentences, 4, upperCase)
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
