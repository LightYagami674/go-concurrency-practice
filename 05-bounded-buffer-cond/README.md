# 05 — Bounded Buffer with Condition Variables

Covers: `sync.Cond` — coordinating goroutines with `Wait`, `Signal`, and
`Broadcast` on top of a `sync.Mutex`, with no busy-waiting.

## Problem

Implement a fixed-capacity FIFO buffer (a bounded queue) shared by multiple
producers and consumers:

- **Producers** call `Put`. If the buffer is **full**, the call **blocks** until
  space is available.
- **Consumers** call `Get`. If the buffer is **empty**, the call **blocks**
  until an item is available.

Blocking must be done by **waiting on a `sync.Cond`** — a goroutine that cannot
proceed sleeps and is woken by a signal from the other side. No polling, no
`time.Sleep`, no busy spin.

The buffer must behave correctly under any number of concurrent producers and
consumers: every item put is eventually delivered exactly once, FIFO order is
preserved, and the capacity is never exceeded.

## API to implement (in `solution.go`)

```go
// BoundedBuffer is a fixed-capacity FIFO queue that blocks producers when full
// and consumers when empty, using sync.Cond for waiting and signaling.
type BoundedBuffer struct { /* ... */ }

// NewBoundedBuffer returns a buffer that holds at most capacity items.
// capacity must be >= 1.
func NewBoundedBuffer(capacity int) *BoundedBuffer

// Put appends item, blocking while the buffer is full.
func (b *BoundedBuffer) Put(item int)

// Get removes and returns the oldest item, blocking while the buffer is empty.
func (b *BoundedBuffer) Get() int

// Len returns the current number of buffered items.
func (b *BoundedBuffer) Len() int
```

## Requirements

1. **`sync.Cond` + `sync.Mutex`** — guard the buffer state with a mutex and use
   one or more `sync.Cond` (sharing that mutex via `sync.NewCond(&mu)`) for
   blocking. A producer that fills the buffer signals waiting consumers; a
   consumer that frees a slot signals waiting producers.
2. **No busy-waiting** — a blocked `Put`/`Get` must actually park on
   `cond.Wait()` (which releases the lock while sleeping), not loop on the CPU or
   sleep-poll.
3. **Re-check in a `for` loop** — after `Wait()` returns, re-test the condition
   in a `for` (not an `if`) before proceeding. `Wait` can return due to a signal
   meant for another goroutine or a spurious wakeup, and the state may have
   changed again by the time this goroutine re-acquires the lock.
4. **Capacity respected** — `Len()` never exceeds `capacity`; `Put` never
   overwrites an occupied slot.
5. **FIFO & exactly-once** — items come out in the order they went in, and no
   item is lost or duplicated across many concurrent producers/consumers.
6. **Race- and deadlock-free** — clean under `-race`; a balanced set of
   producers and consumers always drains without hanging.

## Gotchas to avoid

- **Calling `Wait()` without holding the lock:** `cond.Wait()` requires the
  caller to hold `cond.L`; calling it unlocked panics ("sync: unlock of unlocked
  mutex"). Lock first, then loop-and-wait.
- **Using `if` instead of `for` around `Wait()`:** after waking you must
  re-check the predicate. With `Signal`/`Broadcast`, multiple goroutines may
  wake for one freed slot/item; an `if` lets one proceed on a condition that is
  no longer true (e.g. another consumer already took the item) → it reads from an
  empty buffer or overruns capacity.
- **Signaling the wrong side, or forgetting to signal:** if `Put` only ever
  signals consumers and `Get` never signals producers (or vice versa), the other
  side sleeps forever → deadlock. Each mutation must wake whoever could now make
  progress.
- **`Signal` when you needed `Broadcast`:** if multiple waiters could proceed
  (or different waiters wait on different predicates), waking just one can strand
  the rest. When unsure, `Broadcast` is the safe (if less efficient) choice.

## Running

```bash
go test -race ./05-bounded-buffer-cond/...
go vet ./05-bounded-buffer-cond/...
```
