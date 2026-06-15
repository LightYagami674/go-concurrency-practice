package taskdispatcher

import (
	"fmt"
	"math/rand"
	"reflect"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// dispatchWithTimeout runs Dispatch but fails the test instead of hanging
// forever if the implementation deadlocks or returns early-and-blocks.
func dispatchWithTimeout(t *testing.T, tasks []Task, timeout time.Duration) []int {
	t.Helper()
	done := make(chan []int, 1)
	go func() {
		done <- Dispatch(tasks)
	}()
	select {
	case res := <-done:
		return res
	case <-time.After(timeout):
		t.Fatalf("Dispatch did not return within %v (deadlock or missing result?)", timeout)
		return nil
	}
}

func TestDispatchBasic(t *testing.T) {
	tasks := []Task{
		func() int { return 10 },
		func() int { return 20 },
		func() int { return 30 },
	}
	got := dispatchWithTimeout(t, tasks, 2*time.Second)
	want := []int{10, 20, 30}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestDispatchEmpty(t *testing.T) {
	got := dispatchWithTimeout(t, []Task{}, 2*time.Second)
	if len(got) != 0 {
		t.Fatalf("expected empty result, got %v", got)
	}
}

// TestDispatchIndexing catches the classic loop-variable capture bug: each task
// returns a value derived from its own index, so a buggy dispatcher that lets
// every goroutine share the final index would produce wrong/duplicated results.
func TestDispatchIndexing(t *testing.T) {
	const n = 1000
	tasks := make([]Task, n)
	for i := 0; i < n; i++ {
		i := i // each task should report its own index, squared
		tasks[i] = func() int { return i * i }
	}
	got := dispatchWithTimeout(t, tasks, 5*time.Second)
	if len(got) != n {
		t.Fatalf("got %d results, want %d", len(got), n)
	}
	for i := 0; i < n; i++ {
		if got[i] != i*i {
			t.Fatalf("results[%d] = %d, want %d (per-task indexing wrong — loop capture bug?)", i, got[i], i*i)
		}
	}
}

// TestDispatchOutOfOrderCompletion verifies indexing holds even when later
// tasks finish first. Earlier-indexed tasks sleep longer.
func TestDispatchOutOfOrderCompletion(t *testing.T) {
	const n = 50
	tasks := make([]Task, n)
	for i := 0; i < n; i++ {
		i := i
		tasks[i] = func() int {
			// Task 0 sleeps longest; task n-1 returns immediately.
			time.Sleep(time.Duration(n-i) * time.Millisecond)
			return i + 100
		}
	}
	got := dispatchWithTimeout(t, tasks, 10*time.Second)
	for i := 0; i < n; i++ {
		if got[i] != i+100 {
			t.Fatalf("results[%d] = %d, want %d", i, got[i], i+100)
		}
	}
}

// TestDispatchRunsAllTasks ensures exactly one goroutine runs per task and
// none are dropped — Dispatch must not return before all tasks complete.
func TestDispatchRunsAllTasks(t *testing.T) {
	const n = 500
	var ran int64
	tasks := make([]Task, n)
	for i := 0; i < n; i++ {
		tasks[i] = func() int {
			atomic.AddInt64(&ran, 1)
			return 1
		}
	}
	got := dispatchWithTimeout(t, tasks, 5*time.Second)
	if c := atomic.LoadInt64(&ran); c != n {
		t.Fatalf("only %d/%d tasks ran before Dispatch returned", c, n)
	}
	if len(got) != n {
		t.Fatalf("got %d results, want %d", len(got), n)
	}
}

// TestDispatchDeterministicAcrossGOMAXPROCS confirms the result is identical
// regardless of how many OS threads execute Go code in parallel.
func TestDispatchDeterministicAcrossGOMAXPROCS(t *testing.T) {
	makeTasks := func() []Task {
		const n = 300
		tasks := make([]Task, n)
		for i := 0; i < n; i++ {
			i := i
			tasks[i] = func() int {
				// A little jitter so scheduling order genuinely varies.
				time.Sleep(time.Duration(rand.Intn(2)) * time.Millisecond)
				return i*3 - 7
			}
		}
		return tasks
	}

	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(0)) // restore original

	var prev []int
	for _, procs := range []int{1, 4} {
		runtime.GOMAXPROCS(procs)
		got := dispatchWithTimeout(t, makeTasks(), 10*time.Second)
		if prev != nil && !reflect.DeepEqual(got, prev) {
			t.Fatalf("results differ across GOMAXPROCS settings:\nprev=%v\ngot =%v", prev, got)
		}
		prev = got
	}
}

// Example documents the expected behavior and runs as part of `go test`.
func ExampleDispatch() {
	tasks := []Task{
		func() int { return 1 },
		func() int { return 4 },
		func() int { return 9 },
	}
	fmt.Println(Dispatch(tasks))
	// Output: [1 4 9]
}
