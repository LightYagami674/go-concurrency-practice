package asyncbufferedstage

import "time"

// LossyResult is the outcome of one run of the lossy pipeline.
type LossyResult struct {
	Output    []int // even squares that made it all the way through, in input order
	Processed int   // count of numbers that passed the buffer and were squared
	Dropped   int   // count of numbers dropped because the buffer was full
}

// Run streams 1..n from the generator into a buffered channel of size bufSize.
// The square stage sleeps slowPerItem per value. When the buffer is full the
// generator drops the value (non-blocking send) and counts it, instead of
// blocking. Returns the collected even squares plus processed/dropped counts.
//
// Invariant: Processed + Dropped == n (every generated number is either
// processed or dropped, exactly once).
//
// TODO: implement. Suggested shape:
//   - buf := make(chan int, bufSize)
//   - generator goroutine: for i := 1..n {
//         select { case buf <- i: ; default: dropped++ }
//     }; close(buf)
//     (count dropped with an int the goroutine owns; publish it when done, e.g.
//     via a done channel or an atomic, so Run can read it after the pipeline
//     ends.)
//   - square goroutine (SLOW): for v := range buf { time.Sleep(slowPerItem);
//         processed++; out <- v*v }; close(out)
//   - filterEven goroutine: for v := range out { if v%2==0 { evens <- v } };
//         close(evens)
//   - collect: for v := range evens { Output = append(...) }
//   - return LossyResult{Output, Processed, Dropped}
func Run(n, bufSize int, slowPerItem time.Duration) LossyResult {
	panic("not implemented")
}
