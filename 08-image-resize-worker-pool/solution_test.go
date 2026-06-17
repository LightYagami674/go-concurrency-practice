package imageresizeworkerpool

import (
	"sort"
	"sync/atomic"
	"testing"
	"time"
)

// runWithTimeout fails instead of hanging forever if the pool deadlocks (jobs
// channel never closed, or workers blocked on an undrained results channel).
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
		t.Fatalf("ResizePool did not finish within %v (deadlock?)", timeout)
	}
}

func makeJobs(n int) []Job {
	jobs := make([]Job, n)
	for i := range jobs {
		jobs[i] = Job{ID: i, Width: i + 1, Height: 2}
	}
	return jobs
}

// plainResize is the trivial resize: pixels = width * height.
func plainResize(j Job) Result {
	return Result{JobID: j.ID, Pixels: j.Width * j.Height}
}

// assertAllResults checks there is exactly one correct Result per input job.
func assertAllResults(t *testing.T, jobs []Job, got []Result) {
	t.Helper()
	if len(got) != len(jobs) {
		t.Fatalf("got %d results, want %d", len(got), len(jobs))
	}
	want := make(map[int]int, len(jobs)) // jobID -> pixels
	for _, j := range jobs {
		want[j.ID] = j.Width * j.Height
	}
	seen := make(map[int]bool, len(jobs))
	for _, r := range got {
		px, ok := want[r.JobID]
		if !ok {
			t.Fatalf("result for unknown job ID %d", r.JobID)
		}
		if seen[r.JobID] {
			t.Fatalf("duplicate result for job ID %d", r.JobID)
		}
		seen[r.JobID] = true
		if r.Pixels != px {
			t.Fatalf("job %d: Pixels = %d, want %d", r.JobID, r.Pixels, px)
		}
	}
	if len(seen) != len(jobs) {
		t.Fatalf("only %d distinct jobs produced results, want %d", len(seen), len(jobs))
	}
}

func TestResizePoolBasic(t *testing.T) {
	jobs := makeJobs(100)
	var got []Result
	runWithTimeout(t, 10*time.Second, func() {
		got = ResizePool(jobs, 4, plainResize)
	})
	assertAllResults(t, jobs, got)
}

// TestManyJobsFewWorkers stresses the drain/close path: far more jobs than
// workers, so workers must loop over the jobs channel and results must be
// drained concurrently. A missing close or undrained results channel hangs here.
func TestManyJobsFewWorkers(t *testing.T) {
	jobs := makeJobs(10000)
	var got []Result
	runWithTimeout(t, 20*time.Second, func() {
		got = ResizePool(jobs, 8, plainResize)
	})
	assertAllResults(t, jobs, got)
}

// TestExactlyWWorkers verifies the pool runs at most W jobs at once and, with
// enough jobs, actually reaches W concurrent workers — i.e. exactly W workers,
// not one-per-job and not fewer.
func TestExactlyWWorkers(t *testing.T) {
	const (
		workers = 5
		nJobs   = 200
	)
	jobs := makeJobs(nJobs)

	var current, peak int64
	resize := func(j Job) Result {
		c := atomic.AddInt64(&current, 1)
		for {
			p := atomic.LoadInt64(&peak)
			if c <= p || atomic.CompareAndSwapInt64(&peak, p, c) {
				break
			}
		}
		time.Sleep(2 * time.Millisecond) // hold the slot so workers overlap
		atomic.AddInt64(&current, -1)
		return plainResize(j)
	}

	var got []Result
	runWithTimeout(t, 20*time.Second, func() {
		got = ResizePool(jobs, workers, resize)
	})
	assertAllResults(t, jobs, got)

	if p := atomic.LoadInt64(&peak); p > workers {
		t.Fatalf("peak concurrency = %d, exceeds W = %d (too many workers / one-per-job?)", p, workers)
	} else if p != workers {
		t.Fatalf("peak concurrency = %d, want exactly W = %d (fewer workers than requested?)", p, workers)
	}
}

// TestWorkersClampedToJobs: when workers > len(jobs), the pool must still finish
// (and obviously can't run more jobs at once than exist).
func TestWorkersClampedToJobs(t *testing.T) {
	const (
		workers = 50
		nJobs   = 3
	)
	jobs := makeJobs(nJobs)

	var peak, current int64
	resize := func(j Job) Result {
		c := atomic.AddInt64(&current, 1)
		for {
			p := atomic.LoadInt64(&peak)
			if c <= p || atomic.CompareAndSwapInt64(&peak, p, c) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
		atomic.AddInt64(&current, -1)
		return plainResize(j)
	}

	var got []Result
	runWithTimeout(t, 10*time.Second, func() {
		got = ResizePool(jobs, workers, resize)
	})
	assertAllResults(t, jobs, got)

	if p := atomic.LoadInt64(&peak); p > int64(nJobs) {
		t.Fatalf("peak concurrency = %d, cannot exceed job count %d", p, nJobs)
	}
}

func TestEmptyJobs(t *testing.T) {
	runWithTimeout(t, 5*time.Second, func() {
		got := ResizePool(nil, 4, plainResize)
		if len(got) != 0 {
			t.Fatalf("got %d results, want 0 for empty jobs", len(got))
		}
	})
}

func TestSingleWorker(t *testing.T) {
	jobs := makeJobs(50)
	var got []Result
	runWithTimeout(t, 10*time.Second, func() {
		got = ResizePool(jobs, 1, plainResize)
	})
	assertAllResults(t, jobs, got)

	// With one worker the result IDs are a permutation of 0..n-1; check the set.
	ids := make([]int, len(got))
	for i, r := range got {
		ids[i] = r.JobID
	}
	sort.Ints(ids)
	for i := range ids {
		if ids[i] != i {
			t.Fatalf("ids[%d] = %d, want %d", i, ids[i], i)
		}
	}
}
