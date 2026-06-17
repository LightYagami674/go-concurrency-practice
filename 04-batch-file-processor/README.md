# 04 — Batch File Processor

Covers: `sync.WaitGroup` — launching one goroutine per unit of work, waiting for
all of them to finish before continuing, and collecting their results without a
data race.

## Problem

You are given a list of N "files" to process. Process them **concurrently —
exactly one goroutine per file** — and have the caller block until every
goroutine has finished before producing a summary.

Each file's work is simulated by a caller-supplied `process` function (typically
a short `time.Sleep` followed by returning a byte count, or an error). Your job
is the orchestration: spin up the goroutines, wait for all of them with a
`sync.WaitGroup`, and gather the per-file results safely.

The returned results must be in the **same order as the input** slice, even
though the goroutines finish in arbitrary order.

## API to implement (in `solution.go`)

```go
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
func ProcessAll(files []string, process func(name string) (int, error)) []FileResult

// Summarize aggregates results into a Summary. It is a pure function — no
// concurrency — and is called after ProcessAll returns.
func Summarize(results []FileResult) Summary
```

## Requirements

1. **One goroutine per file** — launch exactly `len(files)` goroutines, each
   responsible for a single file. Do not process files sequentially.
2. **Correct WaitGroup usage** — call `wg.Add` **before** launching each
   goroutine (not inside it), pair every launch with exactly one `Done`, and
   `Wait` for all of them before returning.
3. **`defer wg.Done()`** — the `Done` must run on **every** exit path of the
   goroutine, including when `process` returns an error early. Use
   `defer wg.Done()` as the first line of the goroutine body.
4. **Results collected safely** — each goroutine writes only its own slot
   (`results[i]`) or sends on a channel; no two goroutines touch the same memory
   without synchronization. The whole thing must be clean under `-race`.
5. **Order preserved** — `results[i]` is the outcome of `files[i]` regardless of
   which goroutine finished first.
6. **Empty input** — `ProcessAll(nil, ...)` returns an empty (non-nil-safe)
   slice and does not block.

## Gotchas to avoid

- **`wg.Add(1)` inside the goroutine:** the `Add` then races with `Wait`. If the
  scheduler runs `Wait` before a goroutine reaches its `Add`, `Wait` returns
  early and you read incomplete results. Always `Add` in the launching loop,
  before `go`.
- **Missing `Done` on an early-return / panic path:** if `process` returns an
  error and you `return` without calling `Done`, the `WaitGroup` counter never
  reaches zero and `Wait` blocks forever. `defer wg.Done()` at the top of the
  goroutine guarantees it fires on every path.
- **Capturing the loop variable:** make sure each goroutine sees its own index
  and file name, not a shared, mutating loop variable.
- **Unsynchronized shared collection:** appending to one shared slice or writing
  one shared map from every goroutine is a data race. Give each goroutine a
  disjoint index, or funnel results through a channel.

## Running

```bash
go test -race ./04-batch-file-processor/...
go vet ./04-batch-file-processor/...
```
