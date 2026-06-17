# Go Concurrency Practice

A collection of self-contained Go concurrency problems, each solved from scratch
and verified with tests run under the **race detector**. The goal is to build
deep, hands-on fluency with Go's concurrency primitives and patterns — not just
to use them, but to understand the failure modes (data races, deadlocks, lost
updates, goroutine leaks) and how the Go memory model prevents them.

Every problem lives in its own folder with a written problem statement, an
implementation, and a thorough concurrent test suite designed to catch races and
deadlocks rather than just happy-path values.

## What's covered

The repo is a growing set of problems. Each lives in its own numbered folder —
browse them in order, or jump to a folder's `README.md` for the full problem
statement. Broadly, the problems work through:

- **Synchronization primitives** — `sync.Mutex`, `sync.RWMutex`, `sync.WaitGroup`,
  `sync.Cond`, and `sync/atomic`.
- **Channel-based patterns** — fan-out/fan-in, producer–consumer, worker pools,
  multi-stage pipelines, and non-blocking operations with `select`/`default`.
- **Coordination & lifecycle** — clean shutdown, close-once ownership, and
  leak-free background goroutines.

New problems are added over time, each targeting a specific concept and the
failure modes that come with it.

## Concurrency primitives & patterns demonstrated

- **Goroutines & channels** — fan-out/fan-in, pipelines, producer–consumer.
- **`sync` package** — `Mutex`, `RWMutex`, `WaitGroup`, `Cond`, and `sync/atomic`.
- **Channel patterns** — buffered vs. unbuffered, `select` with `default` for
  non-blocking ops, close-once ownership, draining on shutdown.
- **Coordination & lifecycle** — single-coordinator channel closing, WaitGroup
  completion handshakes, stoppable background goroutines (no leaks).
- **Correctness under the memory model** — establishing happens-before with
  synchronization, avoiding lost updates and torn reads, and understanding why
  "it worked on my machine" is not safety.

## Repository layout

```
go-concurrency-practice/
├── go.mod                      # module: go-concurrency-practice (Go 1.25.4)
└── NN-problem-slug/            # one folder per problem
    ├── README.md               # the problem statement, API, and gotchas
    ├── solution.go             # the implementation
    └── solution_test.go        # concurrent tests (race/deadlock coverage)
```

Each `README.md` states the API to implement, the concurrency guarantees being
tested (race-freedom, ordering, deadlock-freedom), and the classic gotchas the
problem is designed to expose.

## Running the tests

Concurrency tests are always run with the **race detector** enabled:

```bash
# one problem
go test -race ./03-page-view-counter/...

# everything
go test -race ./...

# static analysis (copied locks, printf mistakes, etc.)
go vet ./...
```

The tests deliberately drive high goroutine counts, interleave readers and
writers, and use timeouts so a deadlock fails fast instead of hanging. They are
written to **fail** against an unsynchronized or incorrect implementation and
pass only when the solution is genuinely race- and deadlock-free.

## Why this repo

Concurrency is where a lot of "it compiles and seems to work" code is actually
broken. These exercises focus on writing code that is provably correct under
concurrent load — guarding shared state, coordinating shutdown cleanly, and
reasoning about ordering — which is exactly the kind of rigor production Go
services require.
