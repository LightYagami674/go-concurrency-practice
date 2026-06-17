package boundedbuffer

import (
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// runWithTimeout runs fn but fails instead of hanging forever if the buffer
// deadlocks (e.g. Put never signals consumers, or a missing for-loop strands a
// waiter).
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
		t.Fatalf("test did not finish within %v (deadlock?)", timeout)
	}
}

func TestPutGetFIFOSingle(t *testing.T) {
	b := NewBoundedBuffer(4)
	runWithTimeout(t, 5*time.Second, func() {
		for i := 0; i < 4; i++ {
			b.Put(i)
		}
		if got := b.Len(); got != 4 {
			t.Fatalf("Len = %d, want 4", got)
		}
		for i := 0; i < 4; i++ {
			if got := b.Get(); got != i {
				t.Fatalf("Get #%d = %d, want %d (not FIFO)", i, got, i)
			}
		}
		if got := b.Len(); got != 0 {
			t.Fatalf("Len after draining = %d, want 0", got)
		}
	})
}

// TestPutBlocksWhenFull fills the buffer, then verifies a further Put blocks
// until a Get frees a slot — and that it does NOT exceed capacity meanwhile.
func TestPutBlocksWhenFull(t *testing.T) {
	b := NewBoundedBuffer(2)
	b.Put(1)
	b.Put(2)

	putReturned := make(chan struct{})
	go func() {
		b.Put(3) // must block: buffer is full
		close(putReturned)
	}()

	// The blocked Put must not complete while the buffer stays full.
	select {
	case <-putReturned:
		t.Fatal("Put returned while buffer was full (capacity not respected / not blocking)")
	case <-time.After(150 * time.Millisecond):
	}
	if got := b.Len(); got != 2 {
		t.Fatalf("Len while Put blocked = %d, want 2 (capacity exceeded?)", got)
	}

	// Free a slot; the blocked Put should now proceed.
	if got := b.Get(); got != 1 {
		t.Fatalf("Get = %d, want 1", got)
	}
	select {
	case <-putReturned:
	case <-time.After(2 * time.Second):
		t.Fatal("Put did not unblock after a Get freed a slot (missing signal to producers?)")
	}

	// Remaining items, in FIFO order, are 2 then 3.
	if got := b.Get(); got != 2 {
		t.Fatalf("Get = %d, want 2", got)
	}
	if got := b.Get(); got != 3 {
		t.Fatalf("Get = %d, want 3", got)
	}
}

// TestGetBlocksWhenEmpty verifies a Get on an empty buffer blocks until a Put
// supplies a value.
func TestGetBlocksWhenEmpty(t *testing.T) {
	b := NewBoundedBuffer(2)

	got := make(chan int, 1)
	go func() {
		got <- b.Get() // must block: buffer is empty
	}()

	select {
	case v := <-got:
		t.Fatalf("Get returned %d from an empty buffer (not blocking)", v)
	case <-time.After(150 * time.Millisecond):
	}

	b.Put(42)
	select {
	case v := <-got:
		if v != 42 {
			t.Fatalf("Get = %d, want 42", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Get did not unblock after a Put (missing signal to consumers?)")
	}
}

// TestConcurrentExactlyOnce runs many producers and consumers through a small
// buffer. Every produced value must be consumed exactly once, and capacity must
// never be exceeded (a watcher polls Len throughout).
func TestConcurrentExactlyOnce(t *testing.T) {
	const (
		capacity    = 8
		producers   = 16
		consumers   = 16
		perProducer = 500
	)
	total := producers * perProducer
	if total%consumers != 0 {
		t.Fatalf("test setup: total %d not divisible by consumers %d", total, consumers)
	}
	perConsumer := total / consumers

	b := NewBoundedBuffer(capacity)

	// Watcher: assert capacity is never exceeded while the test runs.
	stopWatch := make(chan struct{})
	var overflow int64
	go func() {
		for {
			select {
			case <-stopWatch:
				return
			default:
				if b.Len() > capacity {
					atomic.StoreInt64(&overflow, 1)
				}
			}
		}
	}()

	results := make(chan int, total)
	runWithTimeout(t, 30*time.Second, func() {
		var wg sync.WaitGroup

		// Producers: producer p emits values p*perProducer .. p*perProducer+perProducer-1,
		// so every value across the whole run is unique.
		for p := 0; p < producers; p++ {
			wg.Add(1)
			go func(p int) {
				defer wg.Done()
				base := p * perProducer
				for i := 0; i < perProducer; i++ {
					b.Put(base + i)
				}
			}(p)
		}

		// Consumers: each consumes a fixed share so the counts balance exactly.
		for c := 0; c < consumers; c++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < perConsumer; i++ {
					results <- b.Get()
				}
			}()
		}

		wg.Wait()
	})
	close(stopWatch)
	close(results)

	if atomic.LoadInt64(&overflow) != 0 {
		t.Fatalf("Len exceeded capacity %d at some point", capacity)
	}
	if got := b.Len(); got != 0 {
		t.Fatalf("buffer not drained: Len = %d, want 0", got)
	}

	seen := make([]bool, total)
	count := 0
	for v := range results {
		if v < 0 || v >= total {
			t.Fatalf("consumed out-of-range value %d", v)
		}
		if seen[v] {
			t.Fatalf("value %d consumed more than once", v)
		}
		seen[v] = true
		count++
	}
	if count != total {
		t.Fatalf("consumed %d values, want %d", count, total)
	}
	for v, ok := range seen {
		if !ok {
			t.Fatalf("value %d was never consumed", v)
		}
	}
}

// TestFIFOSingleProducerConsumer asserts strict FIFO ordering when there is a
// single producer and single consumer sharing a tiny buffer (so Put/Get
// genuinely interleave and block on each other).
func TestFIFOSingleProducerConsumer(t *testing.T) {
	const n = 2000
	b := NewBoundedBuffer(1)

	out := make([]int, 0, n)
	runWithTimeout(t, 15*time.Second, func() {
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < n; i++ {
				b.Put(i)
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < n; i++ {
				out = append(out, b.Get())
			}
		}()
		wg.Wait()
	})

	if !sort.IntsAreSorted(out) {
		t.Fatal("output not in FIFO order")
	}
	for i := 0; i < n; i++ {
		if out[i] != i {
			t.Fatalf("out[%d] = %d, want %d", i, out[i], i)
		}
	}
}

func TestCapacityOne(t *testing.T) {
	b := NewBoundedBuffer(1)
	runWithTimeout(t, 5*time.Second, func() {
		b.Put(7)
		if got := b.Len(); got != 1 {
			t.Fatalf("Len = %d, want 1", got)
		}
		if got := b.Get(); got != 7 {
			t.Fatalf("Get = %d, want 7", got)
		}
		if got := b.Len(); got != 0 {
			t.Fatalf("Len = %d, want 0", got)
		}
	})
}
