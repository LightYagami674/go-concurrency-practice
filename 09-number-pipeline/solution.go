package numberpipeline

// generate returns a channel that emits 1..n and is then closed.
//
// TODO: out := make(chan int); go func(){ defer close(out); for i := 1; i <= n;
// i++ { out <- i } }(); return out
func generate(n int) <-chan int {
	panic("not implemented")
}

// square returns a channel of the squares of the values read from in, closed
// once in is closed and drained.
//
// TODO: out := make(chan int); go func(){ defer close(out); for v := range in {
// out <- v * v } }(); return out
func square(in <-chan int) <-chan int {
	panic("not implemented")
}

// filterEven returns a channel of only the even values read from in, closed once
// in is closed and drained.
//
// TODO: out := make(chan int); go func(){ defer close(out); for v := range in {
// if v%2 == 0 { out <- v } } }(); return out
func filterEven(in <-chan int) <-chan int {
	panic("not implemented")
}

// Pipeline wires generate -> square -> filterEven and returns the collected
// output (the even squares of 1..n, in order).
//
// TODO: out := filterEven(square(generate(n))); var res []int; for v := range
// out { res = append(res, v) }; return res
func Pipeline(n int) []int {
	panic("not implemented")
}
