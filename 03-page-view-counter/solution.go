package pageviewcounter

import "sync"

// Counter is a concurrency-safe map of page path -> view count.
//
// TODO: implement. Guard the map with a sync.RWMutex: writers take the write
// lock, pure readers take the read lock. Always use a *Counter (never copy a
// Counter by value — it embeds a mutex).
type Counter struct {
	mu     sync.RWMutex
	counts map[string]int
}

// NewCounter returns an empty, ready-to-use Counter.
func NewCounter() *Counter {
	// TODO: initialize the map so Inc can write to it.
	panic("not implemented")
}

// Inc records one view of page. Safe to call from many goroutines.
func (c *Counter) Inc(page string) { panic("not implemented") }

// Get returns the current view count for page (0 if never seen).
func (c *Counter) Get(page string) int { panic("not implemented") }

// Snapshot returns a COPY of all counts. Mutating the returned map must not
// affect the Counter, and concurrent Inc calls must not race with the copy.
func (c *Counter) Snapshot() map[string]int { panic("not implemented") }

// Total returns the sum of all page view counts.
func (c *Counter) Total() int { panic("not implemented") }

// --- Bonus: why the "naive" version is broken --------------------------------
//
// The version below looks like it should work — it even prints a plausible total
// when you run it once on your laptop. It is nonetheless WRONG. Do not use it.
//
//	type naiveCounter struct {
//		counts map[string]int // shared, no lock
//	}
//
//	func (c *naiveCounter) Inc(page string) {
//		c.counts[page]++ // read-modify-write on a shared map, unsynchronized
//	}
//
//	func (c *naiveCounter) Get(page string) int {
//		return c.counts[page]
//	}
//
// Why it's broken under the Go memory model:
//
//  1. Concurrent map access is undefined behavior. Two goroutines writing the
//     map at once (or one writing while another reads) can corrupt its internal
//     structure; the Go runtime may even panic with "concurrent map writes".
//
//  2. `c.counts[page]++` is a read-modify-write, not an atomic step. Two
//     goroutines can both read 7, both compute 8, and both store 8 — one
//     increment is silently lost (a lost update / data race).
//
//  3. Without a synchronizing operation (mutex, channel, atomic) there is NO
//     happens-before relationship between the write in one goroutine and the
//     read in another. The compiler and CPU are free to reorder, cache, or tear
//     these accesses, so a reader may observe a stale value indefinitely.
//
// "It worked when I tested it" is not safety: a tight loop, a time.Sleep, or a
// single core can hide the race, but the program is still incorrect and may fail
// on a different machine, compiler version, or scheduling interleaving. Only an
// explicit synchronization operation establishes the ordering the reader relies
// on — which is exactly what the RWMutex above provides.
