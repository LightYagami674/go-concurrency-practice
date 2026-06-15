# 01 — Concurrent Task Dispatcher

Covers: basic goroutines · channels for coordination · concurrency vs. parallelism

## Problem

Build a dispatcher that takes a list of N independent **tasks** (functions that
compute and return a result), runs **each task in its own goroutine**, and
collects all the results back in the caller using **channels**. Each result must
land at the slice index matching its task, even though tasks may finish in any
order.

Then run the program under `GOMAXPROCS=1` and `GOMAXPROCS=4` and confirm the
output is identical both times.

## API to implement (in `solution.go`)

```go
// Task is a unit of independent work that computes and returns a result.
type Task func() int

// Dispatch runs every task in its own goroutine and returns a slice of results
// where results[i] is the value returned by tasks[i].
//
// Results are gathered via channels. Dispatch must not return until every task
// has finished. Ordering of completion is arbitrary; ordering of the returned
// slice is not — it must align with the input.
func Dispatch(tasks []Task) []int
```

## Requirements

1. **One goroutine per task** — launch exactly `len(tasks)` goroutines.
2. **Channels only** — collect results over channels. Do **not** use
   `sync.WaitGroup` (that comes in a later problem).
3. **Correct per-task indexing** — `results[i]` must correspond to `tasks[i]`,
   regardless of the order in which goroutines finish.
4. **Deterministic results** — the returned slice must be identical whether the
   program runs with `GOMAXPROCS=1` or `GOMAXPROCS=4`.
5. **No early return** — `Dispatch` must wait for all tasks; the caller must
   never observe a missing or zero result for a completed task.

## Gotchas to avoid

- **Loop-variable capture:** launching `go func(){ ... i ... }()` inside a
  `for i := range tasks` loop where the closure reads `i` directly. Classically
  every goroutine sees the final value of `i`. (Go 1.22+ changed loop-variable
  scoping, but the dispatcher must be correct by construction — pass the index
  explicitly or send it over the channel.)
- **Returning before goroutines finish:** if you don't receive exactly N results
  before returning, you'll drop results or read zero values.

## Concurrency vs. parallelism

`GOMAXPROCS` controls how many OS threads can execute Go code simultaneously
(parallelism). The program is *structured* concurrently regardless. A correct
solution produces the same results either way — only the timing/interleaving
changes, never the answer.

## Running

```bash
go test -race ./01-concurrent-task-dispatcher/...

# Confirm determinism across parallelism settings:
GOMAXPROCS=1 go test -race -run TestDispatch ./01-concurrent-task-dispatcher/...
GOMAXPROCS=4 go test -race -run TestDispatch ./01-concurrent-task-dispatcher/...
```
