# 02 — Bank Ledger: Two Architectures

Covers: shared memory vs. message passing as two coordination strategies

## Problem

Implement a small bank account that processes a stream of deposit / withdrawal
requests arriving concurrently from many goroutines — **twice**, using two
different coordination strategies:

1. **Shared memory** (`MutexLedger`): a single balance variable guarded by a
   lock. Every goroutine touches the balance directly, but only while holding
   the lock.
2. **Message passing** (`ChannelLedger`): exactly **one** "owner" goroutine owns
   the balance. All other goroutines send deposit/withdrawal/query *requests* to
   it over a channel; no one else ever reads or writes the balance.

For the **same sequence of operations**, both versions must arrive at the
**same final balance**.

There is no overdraft rule — a withdrawal always succeeds and the balance may go
negative. The final balance is therefore exactly:

```
initial + Σ(deposits) − Σ(withdrawals)
```

## API to implement (in `solution.go`)

Both types satisfy a common interface so the tests can drive them identically:

```go
// Ledger is a bank account processing concurrent deposits and withdrawals.
type Ledger interface {
	Deposit(amount int64)   // amount >= 0
	Withdraw(amount int64)  // amount >= 0, subtracts from balance
	Balance() int64         // current balance, observing all prior calls
	Close()                 // release resources; no calls allowed afterward
}

// MutexLedger guards a shared balance variable with a sync.Mutex (or RWMutex).
type MutexLedger struct { /* ... */ }
func NewMutexLedger(initial int64) *MutexLedger

// ChannelLedger hands the balance to a single owner goroutine and talks to it
// over a channel.
type ChannelLedger struct { /* ... */ }
func NewChannelLedger(initial int64) *ChannelLedger
```

(Both constructors return a concrete type; both types must implement `Ledger`.)

## Requirements

1. **Same final balance** — for an identical input sequence, `MutexLedger` and
   `ChannelLedger` produce the same final balance, equal to the arithmetic above.
2. **Shared-memory version is consistent** — the balance is protected on **both**
   reads and writes. A `Balance()` running concurrently with `Deposit`/`Withdraw`
   must never race.
3. **Message-passing version has a single owner** — exactly one goroutine ever
   reads or writes the balance. `Deposit`/`Withdraw`/`Balance` only *send/receive
   messages*; they never touch the balance field directly.
4. **No goroutine leak** — after `Close()`, the owner goroutine in
   `ChannelLedger` must exit (close the request channel and let the owner's
   receive loop terminate). The goroutine count must return to baseline.
5. **Both pass `-race`.**

## Gotchas to avoid

- **Message passing:** forgetting to `close` the request channel in `Close()`,
  so the owner goroutine blocks forever on receive → **goroutine leak**. Range
  over the request channel and let `close` terminate the loop.
- **Shared memory:** locking the *write* but reading the balance without the
  lock (or vice versa). The race detector will catch a `Balance()` that reads
  unguarded while another goroutine writes.
- **Ordering for `Balance()` in the channel version:** route the balance query
  through the *same* request channel as deposits/withdrawals so FIFO ordering
  guarantees the query observes all previously-sent operations. Use a reply
  channel to get the value back.

## Running

```bash
go test -race ./02-bank-ledger/...
```
