package bankledger

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
}

func NewMutexLedger(initial int64) *MutexLedger {
	panic("not implemented")
}

func (l *MutexLedger) Deposit(amount int64)  { panic("not implemented") }
func (l *MutexLedger) Withdraw(amount int64) { panic("not implemented") }
func (l *MutexLedger) Balance() int64        { panic("not implemented") }
func (l *MutexLedger) Close()                { panic("not implemented") }

// --- Message-passing version -------------------------------------------------

// ChannelLedger hands the balance to a single owner goroutine and communicates
// with it over a channel. Exactly one goroutine ever touches the balance.
//
// TODO: implement. Start the owner goroutine in NewChannelLedger; close the
// request channel in Close() so the owner's receive loop terminates.
type ChannelLedger struct {
	// TODO: fields (request channel, etc.).
}

func NewChannelLedger(initial int64) *ChannelLedger {
	panic("not implemented")
}

func (l *ChannelLedger) Deposit(amount int64)  { panic("not implemented") }
func (l *ChannelLedger) Withdraw(amount int64) { panic("not implemented") }
func (l *ChannelLedger) Balance() int64        { panic("not implemented") }
func (l *ChannelLedger) Close()                { panic("not implemented") }
