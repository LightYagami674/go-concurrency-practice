package logaggregator

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// runWithTimeout runs fn but fails instead of hanging forever if the pipeline
// deadlocks (e.g. the consumer never starts, or the channel is never closed).
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
		t.Fatalf("Aggregate did not finish within %v (deadlock or channel never closed?)", timeout)
	}
}

// makeProducers builds `count` producers; producer p emits `perProducer` lines
// of the form "p<p>-<seq>".
func makeProducers(count, perProducer int) []Producer {
	producers := make([]Producer, count)
	for p := 0; p < count; p++ {
		p := p
		producers[p] = func(emit func(line string)) {
			for i := 0; i < perProducer; i++ {
				emit(fmt.Sprintf("p%d-%d", p, i))
			}
		}
	}
	return producers
}

func TestAggregateCollectsEverything(t *testing.T) {
	const (
		producers   = 8
		perProducer = 1000
		bufSize     = 16
	)
	total := producers * perProducer

	var got []string
	runWithTimeout(t, 20*time.Second, func() {
		got = Aggregate(makeProducers(producers, perProducer), bufSize)
	})

	if len(got) != total {
		t.Fatalf("collected %d lines, want %d (consumer dropped or duplicated lines?)", len(got), total)
	}

	// Every expected line must appear exactly once.
	seen := make(map[string]int, total)
	for _, line := range got {
		seen[line]++
	}
	for p := 0; p < producers; p++ {
		for i := 0; i < perProducer; i++ {
			line := fmt.Sprintf("p%d-%d", p, i)
			switch seen[line] {
			case 1:
				// ok
			case 0:
				t.Fatalf("line %q missing from output", line)
			default:
				t.Fatalf("line %q appeared %d times, want 1", line, seen[line])
			}
		}
	}
}

// TestPerProducerOrder verifies that a single producer's own lines arrive in the
// order it emitted them. Sends from one goroutine into a channel preserve order,
// so the consumer must observe each producer's seq numbers ascending.
func TestPerProducerOrder(t *testing.T) {
	const (
		producers   = 6
		perProducer = 500
		bufSize     = 4
	)

	var got []string
	runWithTimeout(t, 20*time.Second, func() {
		got = Aggregate(makeProducers(producers, perProducer), bufSize)
	})

	last := make(map[int]int, producers) // producer -> last seq seen
	for p := 0; p < producers; p++ {
		last[p] = -1
	}
	for _, line := range got {
		// line == "p<p>-<seq>"
		body := strings.TrimPrefix(line, "p")
		parts := strings.SplitN(body, "-", 2)
		if len(parts) != 2 {
			t.Fatalf("malformed line %q", line)
		}
		pid, err1 := strconv.Atoi(parts[0])
		seq, err2 := strconv.Atoi(parts[1])
		if err1 != nil || err2 != nil {
			t.Fatalf("malformed line %q", line)
		}
		if seq <= last[pid] {
			t.Fatalf("producer %d lines out of order: saw seq %d after %d", pid, seq, last[pid])
		}
		last[pid] = seq
	}
	for p := 0; p < producers; p++ {
		if last[p] != perProducer-1 {
			t.Fatalf("producer %d last seq = %d, want %d", p, last[p], perProducer-1)
		}
	}
}

// TestDrainsBufferedTail stresses the shutdown path: many lines are emitted in a
// final burst so that, when the last producer finishes, the channel buffer is
// likely still full. The consumer must drain that tail rather than exit early.
func TestDrainsBufferedTail(t *testing.T) {
	const (
		producers   = 4
		perProducer = 2000
		bufSize     = 256 // large buffer => a big tail to drain at shutdown
	)
	total := producers * perProducer

	for trial := 0; trial < 5; trial++ {
		var got []string
		runWithTimeout(t, 20*time.Second, func() {
			got = Aggregate(makeProducers(producers, perProducer), bufSize)
		})
		if len(got) != total {
			t.Fatalf("trial %d: collected %d lines, want %d (buffered tail dropped?)", trial, len(got), total)
		}
	}
}

// TestSingleConsumerInvariant has each producer emit a unique value and checks
// the multiset is exactly correct — i.e. no line is lost to a premature close or
// double-consumed. (Names every line uniquely across the whole run.)
func TestUniqueValuesExactlyOnce(t *testing.T) {
	const (
		producers   = 10
		perProducer = 300
		bufSize     = 1
	)
	total := producers * perProducer

	ps := make([]Producer, producers)
	for p := 0; p < producers; p++ {
		p := p
		ps[p] = func(emit func(line string)) {
			base := p * perProducer
			for i := 0; i < perProducer; i++ {
				emit(strconv.Itoa(base + i))
			}
		}
	}

	var got []string
	runWithTimeout(t, 20*time.Second, func() {
		got = Aggregate(ps, bufSize)
	})

	if len(got) != total {
		t.Fatalf("collected %d, want %d", len(got), total)
	}
	nums := make([]int, len(got))
	for i, s := range got {
		n, err := strconv.Atoi(s)
		if err != nil {
			t.Fatalf("bad line %q", s)
		}
		nums[i] = n
	}
	sort.Ints(nums)
	for i := 0; i < total; i++ {
		if nums[i] != i {
			t.Fatalf("value mismatch after sort: nums[%d] = %d, want %d (lost/duplicated line)", i, nums[i], i)
		}
	}
}

func TestNoProducers(t *testing.T) {
	runWithTimeout(t, 5*time.Second, func() {
		got := Aggregate(nil, 4)
		if len(got) != 0 {
			t.Fatalf("len = %d, want 0 for no producers", len(got))
		}
	})
}

func TestProducersThatEmitNothing(t *testing.T) {
	// Producers that never emit must still let the pipeline shut down cleanly.
	ps := []Producer{
		func(emit func(string)) {},
		func(emit func(string)) { emit("only") },
		func(emit func(string)) {},
	}
	runWithTimeout(t, 5*time.Second, func() {
		got := Aggregate(ps, 2)
		if len(got) != 1 || got[0] != "only" {
			t.Fatalf("got %v, want [only]", got)
		}
	})
}

// TestConsumerStartsConcurrently ensures producers and consumer run together: a
// small buffer with far more lines than bufSize can only complete if the
// consumer is draining while producers send. The atomic counter confirms all
// emits happened.
func TestConsumerStartsConcurrently(t *testing.T) {
	const (
		producers   = 3
		perProducer = 5000
		bufSize     = 2
	)
	total := producers * perProducer

	var emitted int64
	ps := make([]Producer, producers)
	for p := 0; p < producers; p++ {
		p := p
		ps[p] = func(emit func(string)) {
			for i := 0; i < perProducer; i++ {
				atomic.AddInt64(&emitted, 1)
				emit(fmt.Sprintf("p%d-%d", p, i))
			}
		}
	}

	var got []string
	runWithTimeout(t, 20*time.Second, func() {
		got = Aggregate(ps, bufSize)
	})

	if e := atomic.LoadInt64(&emitted); e != int64(total) {
		t.Fatalf("emitted %d, want %d", e, total)
	}
	if len(got) != total {
		t.Fatalf("collected %d, want %d", len(got), total)
	}
}
