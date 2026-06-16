package bankledger

import "sync"

// Ledger is a bank account processing concurrent deposits and withdrawals.
type Ledger interface {
	Deposit(amount int64)  // amount >= 0
	Withdraw(amount int64) // amount >= 0, subtracts from balance
	Balance() int64        // current balance, observing all prior calls
	Close()                // release resources; no calls allowed afterward
}

// Compile-time checks that both implementations satisfy Ledger.
var (
	_ Ledger = (*MutexLedger)(nil)
	_ Ledger = (*ChannelLedger)(nil)
)

// --- Shared-memory version ---------------------------------------------------

// MutexLedger guards a shared balance variable with a lock.
//
// TODO: implement. Protect the balance on BOTH reads and writes.
type MutexLedger struct {
	// TODO: fields (a mutex and the balance).
	Value int64
	mu    sync.Mutex
}

func NewMutexLedger(initial int64) *MutexLedger {
	return &MutexLedger{
		Value: initial,
		mu:    sync.Mutex{},
	}
}

func (m *MutexLedger) Deposit(amount int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.Value += amount
}

func (m *MutexLedger) Withdraw(amount int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.Value -= amount
}

func (m *MutexLedger) Balance() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.Value
}

func (m *MutexLedger) Close() {}

// --- Message-passing version -------------------------------------------------

// ChannelLedger hands the balance to a single owner goroutine and communicates
// with it over a channel. Exactly one goroutine ever touches the balance.
//
// TODO: implement. Start the owner goroutine in NewChannelLedger; close the
// request channel in Close() so the owner's receive loop terminates.
type Request struct {
	op    string
	val   int64
	resCh chan int64
}

type ChannelLedger struct {
	RequestCh chan Request
}

func NewChannelLedger(initial int64) *ChannelLedger {
	reqCh := make(chan Request)

	channelLedger := ChannelLedger{
		RequestCh: reqCh,
	}

	go func() {
		value := initial
		for req := range reqCh {
			op, val := req.op, req.val

			switch op {
			case "deposit":
				value += val

			case "withdraw":
				value -= val

			case "balance":
				req.resCh <- value
			}

		}
	}()

	return &channelLedger
}

func (l *ChannelLedger) Deposit(amount int64) {
	l.RequestCh <- Request{
		op:  "deposit",
		val: amount,
	}
}
func (l *ChannelLedger) Withdraw(amount int64) {
	l.RequestCh <- Request{
		op:  "withdraw",
		val: amount,
	}
}
func (l *ChannelLedger) Balance() int64 {
	resCh := make(chan int64)
	l.RequestCh <- Request{
		op:    "balance",
		resCh: resCh,
	}

	balance := <-resCh
	return balance
}

func (l *ChannelLedger) Close() {
	close(l.RequestCh)
}
