package pipelinefailfast

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func processWithTimeout(t *testing.T, timeout time.Duration, inputs []string, workers int, transform func(string) (string, error)) ([]string, error) {
	t.Helper()
	type out struct {
		results []string
		err     error
	}
	ch := make(chan out, 1)
	go func() {
		r, e := Process(inputs, workers, transform)
		ch <- out{r, e}
	}()
	select {
	case o := <-ch:
		return o.results, o.err
	case <-time.After(timeout):
		t.Fatalf("Process did not return within %v (goroutine leak / deadlock?)", timeout)
		return nil, nil
	}
}

// TestHappyPath: no errors, all inputs transformed and returned.
func TestHappyPath(t *testing.T) {
	inputs := make([]string, 50)
	for i := range inputs {
		inputs[i] = fmt.Sprintf("item-%d", i)
	}
	upperTransform := func(s string) (string, error) { return strings.ToUpper(s), nil }

	results, err := processWithTimeout(t, 10*time.Second, inputs, 5, upperTransform)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != len(inputs) {
		t.Fatalf("got %d results, want %d", len(results), len(inputs))
	}
	seen := make(map[string]bool)
	for _, r := range results {
		seen[r] = true
	}
	for _, in := range inputs {
		if !seen[strings.ToUpper(in)] {
			t.Errorf("result for %q missing", in)
		}
	}
}

// TestFirstErrorReturned: a single failing item causes Process to return an error.
func TestFirstErrorReturned(t *testing.T) {
	sentinel := errors.New("transform failed")
	inputs := make([]string, 20)
	for i := range inputs {
		inputs[i] = fmt.Sprintf("item-%d", i)
	}
	transform := func(s string) (string, error) {
		if s == "item-5" {
			return "", sentinel
		}
		return strings.ToUpper(s), nil
	}

	_, err := processWithTimeout(t, 10*time.Second, inputs, 4, transform)

	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("got error %v, want %v", err, sentinel)
	}
}

// TestCancellationStopsWork: after an error, remaining transform calls should
// be minimal — the done channel must stop idle workers quickly.
func TestCancellationStopsWork(t *testing.T) {
	var callCount atomic.Int64
	errVal := errors.New("stop now")

	inputs := make([]string, 200)
	for i := range inputs {
		inputs[i] = fmt.Sprintf("item-%d", i)
	}

	transform := func(s string) (string, error) {
		callCount.Add(1)
		time.Sleep(5 * time.Millisecond) // slow enough to accumulate calls if not cancelled
		if s == "item-0" {
			return "", errVal
		}
		return s, nil
	}

	_, err := processWithTimeout(t, 15*time.Second, inputs, 8, transform)

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	// If cancellation works, far fewer than all 200 items are processed.
	if got := callCount.Load(); got >= int64(len(inputs)) {
		t.Errorf("transform called %d times; cancellation should have stopped it well before %d", got, len(inputs))
	}
}

// TestMultipleErrorsOnlyOneReturned: even if many workers error simultaneously,
// Process returns exactly one error (no panic from double channel close).
func TestMultipleErrorsOnlyOneReturned(t *testing.T) {
	errA := errors.New("error-A")
	inputs := make([]string, 100)
	for i := range inputs {
		inputs[i] = fmt.Sprintf("item-%d", i)
	}
	transform := func(s string) (string, error) {
		// every item fails
		return "", errA
	}

	_, err := processWithTimeout(t, 10*time.Second, inputs, 16, transform)

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	// Must not panic — if we reach here the double-close was handled.
}

// TestEmptyInput returns empty slice and nil error.
func TestEmptyInput(t *testing.T) {
	results, err := processWithTimeout(t, 5*time.Second, nil, 4, func(s string) (string, error) {
		return s, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("got %d results for empty input, want 0", len(results))
	}
}

// TestNoGoroutineLeak confirms all worker goroutines exit after Process returns,
// both on the happy path and on the error path.
func TestNoGoroutineLeak(t *testing.T) {
	base := settleGoroutines(runtime.NumGoroutine(), 2*time.Second)

	errVal := errors.New("boom")
	for i := 0; i < 30; i++ {
		inputs := make([]string, 40)
		for j := range inputs {
			inputs[j] = fmt.Sprintf("item-%d", j)
		}
		// alternate: happy run, then error run
		if i%2 == 0 {
			processWithTimeout(t, 5*time.Second, inputs, 4, func(s string) (string, error) { return s, nil })
		} else {
			processWithTimeout(t, 5*time.Second, inputs, 4, func(s string) (string, error) {
				if s == "item-0" {
					return "", errVal
				}
				return s, nil
			})
		}
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
