# go-concurrency-practice

A practice repo for solving Go concurrency problems. Each problem lives in its own
self-contained folder with a problem statement, a solution stub, and tests.

## Repo layout

```
go-concurrency-practice/
├── go.mod                  # module: go-concurrency-practice (Go 1.25.4)
├── CLAUDE.md
└── <NN-problem-slug>/      # one folder per problem
    ├── README.md           # the problem statement
    ├── solution.go         # the implementation (starts EMPTY of logic)
    └── solution_test.go    # tests that verify the solution
```

## Conventions for each problem folder

- **Folder name:** zero-padded number + kebab-case slug, e.g. `01-rate-limiter`,
  `02-worker-pool`. The number reflects the order problems are added.
- **Package name:** the slug in lowercase with no separators, e.g. folder
  `01-rate-limiter` → `package ratelimiter`. Both `solution.go` and
  `solution_test.go` share this package.
- **`README.md`:** the full problem statement, including the function/type
  signatures the solution must expose, constraints, and any concurrency
  guarantees being tested (race-freedom, ordering, deadlock-freedom, etc.).
- **`solution.go`:** declares the package and the exported signatures from the
  README, but the function bodies are left **empty / stubbed** (e.g. `panic("not
  implemented")` or zero-value returns). This is what the user fills in. Do NOT
  implement the solution unless explicitly asked.
- **`solution_test.go`:** complete, runnable tests written up front. They must
  fail (or panic) against the empty stub and pass once the solution is correct.
  Cover concurrency behavior, not just happy-path values.

## Workflow

1. User provides a problem statement.
2. Create a new numbered folder with `README.md`, an empty `solution.go` stub, and
   a complete `solution_test.go`.
3. User implements `solution.go`.
4. Run the tests to confirm they pass.

## Testing

Always run concurrency tests with the race detector:

```bash
go test -race ./<NN-problem-slug>/...   # one problem
go test -race ./...                     # everything
```

Prefer table-driven tests, use `sync.WaitGroup`/channels to drive concurrent
load, and include at least one test that would catch a data race or deadlock
(e.g. high goroutine counts, `-race`, and a timeout via `context` or
`time.After` so a deadlock fails fast instead of hanging).
