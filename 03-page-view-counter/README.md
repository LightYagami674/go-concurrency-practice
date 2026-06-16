# 03 — Thread-Safe Page View Counter

Covers: `sync.Mutex` / `sync.RWMutex`, critical sections, and the happens-before
guarantees that synchronization provides under the Go memory model.

## Problem

Build a page-view counter that many goroutines (simulating incoming HTTP
requests) update concurrently. Internally it is a `map[string]int` mapping a page
path to the number of times it has been viewed. Every increment and every read
must be safe under the race detector.

Reads are expected to greatly outnumber writes in a real analytics service, so
the counter should use a `sync.RWMutex`: multiple readers may hold the lock
simultaneously, but a writer needs exclusive access.

## API to implement (in `solution.go`)

```go
// Counter is a concurrency-safe map of page path -> view count.
type Counter struct { /* ... */ }

// NewCounter returns an empty, ready-to-use Counter.
func NewCounter() *Counter

// Inc records one view of page. Safe to call from many goroutines.
func (c *Counter) Inc(page string)

// Get returns the current view count for page (0 if never seen).
func (c *Counter) Get(page string) int

// Snapshot returns a COPY of all counts. Mutating the returned map must not
// affect the Counter, and concurrent Inc calls must not race with the copy.
func (c *Counter) Snapshot() map[string]int

// Total returns the sum of all page view counts.
func (c *Counter) Total() int
```

## Requirements

1. **Race-free** — all map updates and all reads (`Get`, `Snapshot`, `Total`)
   are safe under `-race`, with any number of concurrent goroutines.
2. **RWMutex used correctly** — writers (`Inc`) take the write lock
   (`Lock`/`Unlock`); pure readers (`Get`, `Snapshot`, `Total`) take the read
   lock (`RLock`/`RUnlock`). Never read the map while holding only a read lock if
   you also mutate it.
3. **Snapshot is a real copy** — `Snapshot` returns a fresh map; mutating it does
   not change the Counter, and a concurrent `Inc` does not corrupt the copy.
4. **No copying the Counter by value** — methods are on a pointer receiver and a
   `Counter` value (which embeds a mutex) is never copied. `go vet` must stay
   clean (no `copylocks` warnings).

## Bonus (commented-out illustration only)

In `solution.go`, include a **commented-out** "naive" version that updates a
shared count with no synchronization, plus a comment explaining why it is broken
under the Go memory model — even though it often *appears* to work on a single
core or in a tight loop. This code is documentation, not part of the test path;
do not make it runnable.

## Gotchas to avoid

- **Early return without unlock:** forgetting `defer mu.Unlock()` (or
  `RUnlock`) on a path that returns early, leaving the lock held forever →
  deadlock on the next caller.
- **Copying a struct that contains a mutex:** passing/returning a `Counter` by
  value copies the embedded `sync.RWMutex`, which is a bug (`go vet` flags it).
  Always use `*Counter`.
- **"It works in practice":** assuming a `time.Sleep`, a tight loop, or low
  contention makes unsynchronized access safe. The memory model gives **no**
  happens-before guarantee without synchronization; the compiler and CPU may
  reorder or cache writes, so a read can observe a stale or torn value.

## Running

```bash
go test -race ./03-page-view-counter/...
go vet ./03-page-view-counter/...
```
