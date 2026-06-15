package bankledger

import (
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"
)

// generateOps builds a deterministic sequence of signed operations: a positive
// value is a deposit, a negative value is a withdrawal of its magnitude.
func generateOps(n int, seed int64) []int64 {
	r := rand.New(rand.NewSource(seed))
	ops := make([]int64, n)
	for i := range ops {
		v := int64(r.Intn(1000) + 1) // 1..1000
		if r.Intn(2) == 0 {
			v = -v
		}
		ops[i] = v
	}
	return ops
}

func sum(ops []int64) int64 {
	var s int64
	for _, v := range ops {
		s += v
	}
	return s
}

// drive applies the operations to a ledger from `workers` goroutines running
// concurrently, then returns the final balance. A WaitGroup is used here (in the
// test harness) only to know when every call has been issued — the ledger itself
// must not rely on it.
func drive(l Ledger, ops []int64, workers int) int64 {
	var wg sync.WaitGroup
	chunk := (len(ops) + workers - 1) / workers
	for w := 0; w < workers; w++ {
		start := w * chunk
		end := start + chunk
		if start >= len(ops) {
			break
		}
		if end > len(ops) {
			end = len(ops)
		}
		wg.Add(1)
		go func(part []int64) {
			defer wg.Done()
			for _, v := range part {
				if v >= 0 {
					l.Deposit(v)
				} else {
					l.Withdraw(-v)
				}
			}
		}(ops[start:end])
	}
	wg.Wait()
	return l.Balance()
}

const initialBalance = 1000

func TestMutexLedger(t *testing.T) {
	ops := generateOps(5000, 1)
	l := NewMutexLedger(initialBalance)
	defer l.Close()

	got := drive(l, ops, 50)
	want := int64(initialBalance) + sum(ops)
	if got != want {
		t.Fatalf("MutexLedger final balance = %d, want %d", got, want)
	}
}

func TestChannelLedger(t *testing.T) {
	ops := generateOps(5000, 1)
	l := NewChannelLedger(initialBalance)
	defer l.Close()

	got := drive(l, ops, 50)
	want := int64(initialBalance) + sum(ops)
	if got != want {
		t.Fatalf("ChannelLedger final balance = %d, want %d", got, want)
	}
}

// TestBothArchitecturesAgree feeds the identical operation sequence through both
// implementations and requires the same final balance.
func TestBothArchitecturesAgree(t *testing.T) {
	ops := generateOps(8000, 42)
	want := int64(initialBalance) + sum(ops)

	m := NewMutexLedger(initialBalance)
	defer m.Close()
	mGot := drive(m, ops, 64)

	c := NewChannelLedger(initialBalance)
	defer c.Close()
	cGot := drive(c, ops, 64)

	if mGot != cGot {
		t.Fatalf("architectures disagree: mutex=%d channel=%d", mGot, cGot)
	}
	if mGot != want {
		t.Fatalf("final balance = %d, want %d", mGot, want)
	}
}

// TestConcurrentReadsAndWrites hammers the ledger with writers while readers
// call Balance() at the same time. Under -race this catches a Balance() that
// reads the shared variable without holding the lock.
func TestConcurrentReadsAndWrites(t *testing.T) {
	for _, tc := range []struct {
		name string
		make func() Ledger
	}{
		{"mutex", func() Ledger { return NewMutexLedger(initialBalance) }},
		{"channel", func() Ledger { return NewChannelLedger(initialBalance) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := tc.make()
			defer l.Close()

			var wg sync.WaitGroup
			// writers
			for w := 0; w < 16; w++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < 1000; i++ {
						l.Deposit(2)
						l.Withdraw(1)
					}
				}()
			}
			// readers running concurrently with writers
			for r := 0; r < 8; r++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < 1000; i++ {
						_ = l.Balance()
					}
				}()
			}
			wg.Wait()

			want := int64(initialBalance) + 16*1000*(2-1)
			if got := l.Balance(); got != want {
				t.Fatalf("%s final balance = %d, want %d", tc.name, got, want)
			}
		})
	}
}

// TestChannelLedgerNoLeak verifies the owner goroutine exits after Close().
func TestChannelLedgerNoLeak(t *testing.T) {
	// Let any goroutines from earlier tests settle.
	time.Sleep(50 * time.Millisecond)
	runtime.GC()
	base := runtime.NumGoroutine()

	for i := 0; i < 20; i++ {
		l := NewChannelLedger(0)
		drive(l, generateOps(100, int64(i)), 4)
		l.Close()
	}

	// Poll for the goroutine count to return to baseline.
	deadline := time.Now().Add(2 * time.Second)
	for {
		runtime.GC()
		n := runtime.NumGoroutine()
		if n <= base+1 { // allow a little slack for the test runtime
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutine leak: started at %d, now %d after closing 20 ledgers "+
				"(owner goroutines not exiting on Close?)", base, n)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
