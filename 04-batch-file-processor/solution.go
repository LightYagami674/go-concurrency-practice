package batchfileprocessor

import "sync"

// FileResult is the outcome of processing a single file.
type FileResult struct {
	Name string // the file name (from the input slice)
	Size int    // bytes processed; 0 on error
	Err  error  // non-nil if processing failed
}

// Summary aggregates the outcome of a batch.
type Summary struct {
	Total     int // number of files processed
	Succeeded int // files whose process call returned nil error
	Failed    int // files whose process call returned an error
	TotalSize int // sum of Size across the succeeded files
}

// ProcessAll processes every file in files concurrently, using exactly one
// goroutine per file, and blocks (via sync.WaitGroup) until all of them finish.
//
// process performs the work for a single file and returns the number of bytes
// processed, or an error. ProcessAll calls it once per file, concurrently.
//
// The returned slice is the same length as files and in the SAME ORDER:
// results[i] corresponds to files[i].
//
// TODO: implement.
//   - Pre-size the results slice to len(files) so each goroutine can write its
//     own slot results[i] without a lock.
//   - Add to the WaitGroup BEFORE each `go` (never inside the goroutine).
//   - Make `defer wg.Done()` the first line of the goroutine so it fires on
//     every exit path, including when process returns an error.
//   - Wait for all goroutines before returning.
func ProcessAll(files []string, process func(name string) (int, error)) []FileResult {
	wg := sync.WaitGroup{}
	n := len(files)
	fileRes := make([]FileResult, n)

	for idx, f := range files {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res := FileResult{
				Name: f,
			}
			res.Size, res.Err = process(f)
			fileRes[idx] = res
		}()
	}

	wg.Wait()
	return fileRes
}

// Summarize aggregates results into a Summary. It is a pure function — no
// concurrency — and is called after ProcessAll returns.
//
// TODO: implement. Count successes vs failures and sum Size over the successes.
func Summarize(results []FileResult) Summary {
	totalSize, failed := 0, 0

	for _, r := range results {
		if r.Err != nil {
			failed++
		} else {
			totalSize += r.Size
		}
	}

	return Summary{
		Total:     len(results),
		Succeeded: len(results) - failed,
		Failed:    failed,
		TotalSize: totalSize,
	}
}
