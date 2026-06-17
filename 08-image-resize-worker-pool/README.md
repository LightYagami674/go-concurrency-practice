# 08 — Image Resize Worker Pool

Covers: the **worker-pool pattern** — a fixed number of worker goroutines
pulling jobs from a channel, writing results to another channel, with a
`sync.WaitGroup` tracking when every worker has finished so the results channel
can be closed exactly once.

## Problem

Build a fixed-size pool of **W** workers that process "image resize" jobs. Jobs
arrive on a `jobs` channel; each worker pulls a job, does the (simulated) resize,
and sends the outcome on a `results` channel. The work is fanned out across
exactly W workers and fanned back in through `results`.

The lifecycle must be clean:

```
submit all jobs ──► close(jobs)
        │
        ▼
   W workers range over jobs ──► send on results
        │
        ▼
  all workers done (WaitGroup) ──► close(results)
        │
        ▼
   caller ranges over results ──► collected
```

The two classic ways to wedge this pattern are (1) never closing `jobs`, so
workers block forever waiting for more, and (2) letting workers block on
`results` because nothing is draining it. Your design must avoid both.

## API to implement (in `solution.go`)

```go
// Job is a unit of resize work.
type Job struct {
	ID     int
	Width  int // target width
	Height int // target height
}

// Result is the outcome of one Job.
type Result struct {
	JobID  int
	Pixels int // Width * Height of the resized image
}

// ResizeFunc performs the (simulated) resize for a single job. A worker calls it
// once per job; a typical body is a short sleep plus
// `Result{JobID: job.ID, Pixels: job.Width * job.Height}`.
type ResizeFunc func(job Job) Result

// ResizePool fans `jobs` out across exactly `workers` goroutines, calls `resize`
// on each job, and returns all results. It returns only after every job has been
// processed.
//
// The returned slice contains exactly one Result per input job (order
// unspecified — results come back as workers finish).
func ResizePool(jobs []Job, workers int, resize ResizeFunc) []Result
```

The "resize" itself is trivial work supplied by the caller; the exercise is the
concurrency structure around it.

## Requirements

1. **Exactly W workers** — launch precisely `workers` goroutines, no more, no
   fewer (clamp sensibly if `workers` exceeds the job count, but never spawn one
   goroutine per job).
2. **Jobs channel closed once** — feed all jobs onto a `jobs` channel and
   `close` it exactly once after the last job is sent, by a single sender.
3. **Workers exit on close + drain** — each worker does `for job := range
   jobsCh` so it processes every buffered job and then exits when the channel is
   closed and empty.
4. **Results collected without deadlock** — `results` must be drained
   concurrently with the workers producing (don't try to collect only after
   `wg.Wait()` if `results` is unbuffered/small — that deadlocks). Close
   `results` exactly once, after all workers finish, then range over it.
5. **Every job produces exactly one result** — `len(returned) == len(jobs)`, one
   Result per Job, none lost or duplicated.
6. **Race- & deadlock-free** — clean under `-race`; never hangs.

## Gotchas to avoid

- **Forgetting to close `jobs`:** if the jobs channel is never closed, `for range
  jobsCh` never ends and the workers (and `wg.Wait()`) hang forever. Close it
  once, after submitting all jobs.
- **Unbuffered `results` with no reader:** if every worker sends on `results`
  while the only code that would read it runs *after* `wg.Wait()`, the workers
  block on send, `wg.Wait()` never returns, and you deadlock. Drain `results` in
  a separate goroutine (and `close` it via a coordinator: `go func(){ wg.Wait();
  close(results) }()`), or give `results` enough buffer.
- **Closing `results` from a worker:** multiple workers closing (or closing while
  another worker still sends) panics. Exactly one coordinator closes it, after
  all workers are done.
- **One goroutine per job:** that's not a pool. The point is a *fixed* W workers
  sharing the jobs channel.

## Running

```bash
go test -race ./08-image-resize-worker-pool/...
go vet ./08-image-resize-worker-pool/...
```
