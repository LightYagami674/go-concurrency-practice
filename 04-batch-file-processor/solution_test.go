package batchfileprocessor

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// runWithTimeout runs fn but fails the test instead of hanging forever if the
// implementation deadlocks — e.g. a missing wg.Done() on an early-return path
// leaves the WaitGroup counter above zero and Wait blocks indefinitely.
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
		t.Fatalf("ProcessAll did not finish within %v (missing Done, or Wait never returns?)", timeout)
	}
}

// okProcess is a trivial process function: size is the length of the name.
func okProcess(name string) (int, error) { return len(name), nil }

func TestProcessAllBasic(t *testing.T) {
	files := []string{"a.txt", "bb.txt", "ccc.txt"}
	var results []FileResult
	runWithTimeout(t, 5*time.Second, func() {
		results = ProcessAll(files, okProcess)
	})

	if len(results) != len(files) {
		t.Fatalf("len(results) = %d, want %d", len(results), len(files))
	}
	for i, f := range files {
		if results[i].Name != f {
			t.Errorf("results[%d].Name = %q, want %q", i, results[i].Name, f)
		}
		if results[i].Err != nil {
			t.Errorf("results[%d].Err = %v, want nil", i, results[i].Err)
		}
		if want := len(f); results[i].Size != want {
			t.Errorf("results[%d].Size = %d, want %d", i, results[i].Size, want)
		}
	}
}

// TestOrderPreserved makes earlier files sleep LONGER than later ones, so the
// goroutines finish in roughly reverse order. The result slice must still be in
// input order, which only holds if each goroutine writes its own index.
func TestOrderPreserved(t *testing.T) {
	const n = 25
	files := make([]string, n)
	for i := range files {
		files[i] = fmt.Sprintf("file-%02d", i)
	}

	process := func(name string) (int, error) {
		// Parse the index back out of the name and sleep inversely to it.
		var idx int
		fmt.Sscanf(name, "file-%d", &idx)
		time.Sleep(time.Duration(n-idx) * time.Millisecond)
		return idx, nil
	}

	var results []FileResult
	runWithTimeout(t, 10*time.Second, func() {
		results = ProcessAll(files, process)
	})

	if len(results) != n {
		t.Fatalf("len(results) = %d, want %d", len(results), n)
	}
	for i := range files {
		if results[i].Name != files[i] {
			t.Fatalf("results[%d].Name = %q, want %q (results not in input order)", i, results[i].Name, files[i])
		}
		if results[i].Size != i {
			t.Fatalf("results[%d].Size = %d, want %d (slot/index mismatch)", i, results[i].Size, i)
		}
	}
}

// TestErrorsCollected mixes successes and failures. Every goroutine must call
// Done even on the error path (else Wait hangs), and failed results must carry
// the error with Size 0 while successes carry their size.
func TestErrorsCollected(t *testing.T) {
	files := []string{"ok1", "bad1", "ok2", "bad2", "ok3"}
	failErr := errors.New("boom")

	process := func(name string) (int, error) {
		if name[0] == 'b' { // "bad..." files fail
			return 0, failErr
		}
		return 100, nil
	}

	var results []FileResult
	runWithTimeout(t, 5*time.Second, func() {
		results = ProcessAll(files, process)
	})

	if len(results) != len(files) {
		t.Fatalf("len(results) = %d, want %d", len(results), len(files))
	}
	for i, f := range files {
		r := results[i]
		if r.Name != f {
			t.Errorf("results[%d].Name = %q, want %q", i, r.Name, f)
		}
		if f[0] == 'b' {
			if !errors.Is(r.Err, failErr) {
				t.Errorf("results[%d] (%s): Err = %v, want %v", i, f, r.Err, failErr)
			}
			if r.Size != 0 {
				t.Errorf("results[%d] (%s): Size = %d, want 0 on error", i, f, r.Size)
			}
		} else {
			if r.Err != nil {
				t.Errorf("results[%d] (%s): Err = %v, want nil", i, f, r.Err)
			}
			if r.Size != 100 {
				t.Errorf("results[%d] (%s): Size = %d, want 100", i, f, r.Size)
			}
		}
	}
}

// TestEveryResultPopulated launches a large batch and asserts every slot was
// written exactly once. If wg.Add(1) is called INSIDE the goroutine, Wait can
// return before some goroutines run, leaving zero-value (empty Name) slots —
// this test catches that. The atomic write-count also catches double-writes.
func TestEveryResultPopulated(t *testing.T) {
	const n = 500
	files := make([]string, n)
	for i := range files {
		files[i] = fmt.Sprintf("f%04d", i)
	}

	var writes int64
	process := func(name string) (int, error) {
		atomic.AddInt64(&writes, 1)
		return 1, nil
	}

	var results []FileResult
	runWithTimeout(t, 10*time.Second, func() {
		results = ProcessAll(files, process)
	})

	if got := atomic.LoadInt64(&writes); got != n {
		t.Fatalf("process called %d times, want %d (one goroutine per file?)", got, n)
	}
	if len(results) != n {
		t.Fatalf("len(results) = %d, want %d", len(results), n)
	}
	for i := range files {
		if results[i].Name != files[i] {
			t.Fatalf("results[%d].Name = %q, want %q (slot never written — Add inside goroutine racing Wait?)",
				i, results[i].Name, files[i])
		}
	}
}

// TestActuallyConcurrent proves the files are processed in parallel (one
// goroutine each), not sequentially. With every file sleeping, the peak number
// of simultaneously-running process calls must equal len(files).
func TestActuallyConcurrent(t *testing.T) {
	const n = 20
	files := make([]string, n)
	for i := range files {
		files[i] = fmt.Sprintf("f%d", i)
	}

	var current, peak int64
	process := func(name string) (int, error) {
		c := atomic.AddInt64(&current, 1)
		for {
			p := atomic.LoadInt64(&peak)
			if c <= p || atomic.CompareAndSwapInt64(&peak, p, c) {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
		atomic.AddInt64(&current, -1)
		return 0, nil
	}

	runWithTimeout(t, 10*time.Second, func() {
		ProcessAll(files, process)
	})

	if got := atomic.LoadInt64(&peak); got != n {
		t.Fatalf("peak concurrency = %d, want %d (files processed sequentially or not one-goroutine-per-file?)", got, n)
	}
}

func TestEmptyInput(t *testing.T) {
	runWithTimeout(t, 5*time.Second, func() {
		results := ProcessAll(nil, okProcess)
		if len(results) != 0 {
			t.Fatalf("len(results) = %d, want 0 for empty input", len(results))
		}
	})
}

func TestSummarize(t *testing.T) {
	results := []FileResult{
		{Name: "a", Size: 10, Err: nil},
		{Name: "b", Size: 0, Err: errors.New("x")},
		{Name: "c", Size: 30, Err: nil},
		{Name: "d", Size: 0, Err: errors.New("y")},
		{Name: "e", Size: 5, Err: nil},
	}
	got := Summarize(results)
	want := Summary{Total: 5, Succeeded: 3, Failed: 2, TotalSize: 45}
	if got != want {
		t.Fatalf("Summarize = %+v, want %+v", got, want)
	}
}

func TestSummarizeEmpty(t *testing.T) {
	got := Summarize(nil)
	want := Summary{}
	if got != want {
		t.Fatalf("Summarize(nil) = %+v, want %+v", got, want)
	}
}

func ExampleSummarize() {
	files := []string{"a.txt", "fail.txt", "ccc.txt"}
	process := func(name string) (int, error) {
		if name == "fail.txt" {
			return 0, errors.New("disk error")
		}
		return len(name), nil
	}

	results := ProcessAll(files, process)
	s := Summarize(results)
	fmt.Printf("total=%d succeeded=%d failed=%d bytes=%d\n",
		s.Total, s.Succeeded, s.Failed, s.TotalSize)
	// Output:
	// total=3 succeeded=2 failed=1 bytes=12
}
