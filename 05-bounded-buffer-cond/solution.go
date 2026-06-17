package boundedbuffer

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
}

// NewBoundedBuffer returns a buffer that holds at most capacity items.
// capacity must be >= 1.
//
// TODO: initialize the mutex-backed cond(s) with sync.NewCond(&b.mu).
func NewBoundedBuffer(capacity int) *BoundedBuffer {
	panic("not implemented")
}

// Put appends item, blocking while the buffer is full.
//
// TODO: lock; `for len == capacity { notFull.Wait() }`; append; signal a
// consumer (notEmpty.Signal); unlock.
func (b *BoundedBuffer) Put(item int) {
	panic("not implemented")
}

// Get removes and returns the oldest item, blocking while the buffer is empty.
//
// TODO: lock; `for len == 0 { notEmpty.Wait() }`; pop the front; signal a
// producer (notFull.Signal); unlock and return the item.
func (b *BoundedBuffer) Get() int {
	panic("not implemented")
}

// Len returns the current number of buffered items.
//
// TODO: lock, read len, unlock.
func (b *BoundedBuffer) Len() int {
	panic("not implemented")
}
