package boundedbuffer

import "sync"

// BoundedBuffer is a fixed-capacity FIFO queue that blocks producers when full
// and consumers when empty, using sync.Cond for waiting and signaling.
//
// TODO: implement. Suggested fields:
//   - mu       sync.Mutex          // guards everything below
//   - notFull  *sync.Cond          // sync.NewCond(&mu); producers wait here
//   - notEmpty *sync.Cond          // sync.NewCond(&mu); consumers wait here
//   - items    []int               // FIFO storage (slice or ring buffer)
//   - capacity int
//
// You may share a single *sync.Cond for both sides and Broadcast, but two
// conds (one per side) with Signal is the idiomatic, efficient choice.
type BoundedBuffer struct {
	// TODO: fields.
	items    []int
	capacity int
	mu       sync.Mutex
	notFull  *sync.Cond
	notEmpty *sync.Cond
}

// NewBoundedBuffer returns a buffer that holds at most capacity items.
// capacity must be >= 1.
//
// TODO: initialize the mutex-backed cond(s) with sync.NewCond(&b.mu).
func NewBoundedBuffer(capacity int) *BoundedBuffer {
	bb := BoundedBuffer{
		capacity: capacity,
		items:    make([]int, 0, capacity),
	}

	bb.notEmpty = sync.NewCond(&bb.mu)
	bb.notFull = sync.NewCond(&bb.mu)
	return &bb
}

// Put appends item, blocking while the buffer is full.
//
// TODO: lock; `for len == capacity { notFull.Wait() }`; append; signal a
// consumer (notEmpty.Signal); unlock.
func (b *BoundedBuffer) Put(item int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for len(b.items) == b.capacity {
		b.notFull.Wait()
	}

	b.items = append(b.items, item)
	b.notEmpty.Signal()

}

// Get removes and returns the oldest item, blocking while the buffer is empty.
//
// TODO: lock; `for len == 0 { notEmpty.Wait() }`; pop the front; signal a
// producer (notFull.Signal); unlock and return the item.
func (b *BoundedBuffer) Get() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	for len(b.items) == 0 {
		b.notEmpty.Wait()
	}
	last := b.items[0]
	b.items = b.items[1:]

	b.notFull.Signal()

	return last
}

// Len returns the current number of buffered items.
//
// TODO: lock, read len, unlock.
func (b *BoundedBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	return len(b.items)
}
