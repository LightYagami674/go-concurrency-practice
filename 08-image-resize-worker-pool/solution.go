package imageresizeworkerpool

import "sync"

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
// Result{JobID: job.ID, Pixels: job.Width * job.Height}.
type ResizeFunc func(job Job) Result

// ResizePool fans `jobs` out across exactly `workers` goroutines, calls `resize`
// on each job, and returns all results. It returns only after every job has been
// processed.
//
// The returned slice contains exactly one Result per input job (order
// unspecified — results come back as workers finish).
//
// TODO: implement. Suggested shape:
//   - jobsCh := make(chan Job); resultsCh := make(chan Result)
//   - feeder: go func(){ for _, j := range jobs { jobsCh <- j }; close(jobsCh) }()
//   - var wg sync.WaitGroup; start exactly `workers` workers:
//     wg.Add(1); go func(){ defer wg.Done(); for j := range jobsCh {
//     resultsCh <- resize(j) } }()
//   - coordinator: go func(){ wg.Wait(); close(resultsCh) }()  // the ONLY close
//   - collect (main path): for r := range resultsCh { append }
//   - clamp workers to len(jobs) when it's larger; handle len(jobs)==0.
func ResizePool(jobs []Job, workers int, resize ResizeFunc) []Result {
	jobsCh := make(chan Job)
	resultsCh := make(chan Result, workers)
	result := make([]Result, 0)

	wg := sync.WaitGroup{}

	for range workers {
		wg.Go(func() {
			for job := range jobsCh {
				resultsCh <- resize(job)
			}
		})
	}

	go func() {
		for _, job := range jobs {
			jobsCh <- job
		}

		close(jobsCh)
	}()

	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	for res := range resultsCh {
		result = append(result, res)
	}

	return result
}
