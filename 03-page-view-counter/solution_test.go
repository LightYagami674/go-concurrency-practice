package pageviewcounter

import (
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"
)

// runWithTimeout runs fn but fails the test instead of hanging forever if the
// implementation deadlocks (e.g. a missing Unlock on an early return).
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
		t.Fatalf("test did not finish within %v (deadlock or held lock?)", timeout)
	}
}

func TestIncAndGetBasic(t *testing.T) {
	c := NewCounter()
	if got := c.Get("/home"); got != 0 {
		t.Fatalf("Get on unseen page = %d, want 0", got)
	}
	c.Inc("/home")
	c.Inc("/home")
	c.Inc("/about")
	if got := c.Get("/home"); got != 2 {
		t.Fatalf("/home = %d, want 2", got)
	}
	if got := c.Get("/about"); got != 1 {
		t.Fatalf("/about = %d, want 1", got)
	}
}

func TestTotal(t *testing.T) {
	c := NewCounter()
	for i := 0; i < 5; i++ {
		c.Inc("/a")
	}
	for i := 0; i < 3; i++ {
		c.Inc("/b")
	}
	if got := c.Total(); got != 8 {
		t.Fatalf("Total = %d, want 8", got)
	}
}

// TestConcurrentInc hammers a small set of pages from many goroutines. Under
// -race this catches an unsynchronized map write or a lost-update read-modify-
// write. The final counts must be exact (no increments lost).
func TestConcurrentInc(t *testing.T) {
	c := NewCounter()
	const (
		goroutines = 100
		perG       = 1000
	)
	pages := []string{"/home", "/about", "/pricing", "/contact"}

	runWithTimeout(t, 10*time.Second, func() {
		var wg sync.WaitGroup
		for g := 0; g < goroutines; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				for i := 0; i < perG; i++ {
					c.Inc(pages[(g+i)%len(pages)])
				}
			}(g)
		}
		wg.Wait()
	})

	// Every goroutine did perG increments, distributed round-robin across the
	// pages, so the grand total must be exact even if per-page split varies.
	wantTotal := goroutines * perG
	if got := c.Total(); got != wantTotal {
		t.Fatalf("Total = %d, want %d (lost updates?)", got, wantTotal)
	}

	// Cross-check: sum of Snapshot values equals Total.
	sum := 0
	for _, v := range c.Snapshot() {
		sum += v
	}
	if sum != wantTotal {
		t.Fatalf("sum of Snapshot = %d, want %d", sum, wantTotal)
	}
}

// TestConcurrentReadWrite runs readers and writers simultaneously. With a
// correct RWMutex this neither races nor deadlocks.
func TestConcurrentReadWrite(t *testing.T) {
	c := NewCounter()
	runWithTimeout(t, 10*time.Second, func() {
		var wg sync.WaitGroup
		// Writers.
		for g := 0; g < 20; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 2000; i++ {
					c.Inc("/hot")
				}
			}()
		}
		// Readers interleaved with writes.
		for g := 0; g < 20; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 2000; i++ {
					_ = c.Get("/hot")
					_ = c.Total()
					_ = c.Snapshot()
				}
			}()
		}
		wg.Wait()
	})

	if got := c.Get("/hot"); got != 20*2000 {
		t.Fatalf("/hot = %d, want %d", got, 20*2000)
	}
}

// TestSnapshotIsolation verifies Snapshot returns an independent copy: mutating
// it must not affect the Counter, and a later Inc must not change the snapshot.
func TestSnapshotIsolation(t *testing.T) {
	c := NewCounter()
	c.Inc("/x")
	c.Inc("/x")

	snap := c.Snapshot()
	if snap["/x"] != 2 {
		t.Fatalf("snapshot /x = %d, want 2", snap["/x"])
	}

	// Mutate the snapshot; the Counter must be unaffected.
	snap["/x"] = 999
	snap["/injected"] = 1
	if got := c.Get("/x"); got != 2 {
		t.Fatalf("mutating snapshot changed Counter: /x = %d, want 2", got)
	}
	if got := c.Get("/injected"); got != 0 {
		t.Fatalf("mutating snapshot leaked key into Counter: /injected = %d, want 0", got)
	}

	// Mutate the Counter; the earlier snapshot must be unaffected.
	c.Inc("/x")
	if snap["/x"] != 999 {
		t.Fatalf("Inc changed an already-taken snapshot: /x = %d, want 999", snap["/x"])
	}
}

// TestSnapshotConcurrentWithInc ensures copying the map while writes happen is
// race-free (a naive range-copy without the lock would be caught by -race).
func TestSnapshotConcurrentWithInc(t *testing.T) {
	c := NewCounter()
	runWithTimeout(t, 10*time.Second, func() {
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < 5000; i++ {
				c.Inc(fmt.Sprintf("/p%d", i%50))
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < 5000; i++ {
				_ = c.Snapshot()
			}
		}()
		wg.Wait()
	})
}

func ExampleCounter() {
	c := NewCounter()
	c.Inc("/home")
	c.Inc("/home")
	c.Inc("/about")

	snap := c.Snapshot()
	pages := make([]string, 0, len(snap))
	for p := range snap {
		pages = append(pages, p)
	}
	sort.Strings(pages)
	for _, p := range pages {
		fmt.Printf("%s=%d\n", p, snap[p])
	}
	fmt.Println("total", c.Total())
	// Output:
	// /about=1
	// /home=2
	// total 3
}
