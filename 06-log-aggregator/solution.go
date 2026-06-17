package logaggregator

// Producer generates log lines by calling emit once per line.
type Producer func(emit func(line string))

// Aggregate runs every producer concurrently (one goroutine each); each line a
// producer emits is sent over a buffered channel of size bufSize to a single
// consumer goroutine that collects them.
//
// Aggregate returns the collected lines only after every producer has finished
// AND the channel has been fully drained. Every emitted line appears in the
// result exactly once. (Order across different producers is unspecified; a
// single producer's own lines appear in the order it emitted them.)
//
// TODO: implement. Suggested shape:
//   - ch := make(chan string, bufSize)
//   - var wg sync.WaitGroup; for each producer: wg.Add(1); go func(){ defer
//     wg.Done(); p(func(line string){ ch <- line }) }()
//   - coordinator: go func(){ wg.Wait(); close(ch) }()   // the ONLY close
//   - consumer (main path): for line := range ch { collect line }
//   - return the collected lines after the range loop ends.
func Aggregate(producers []Producer, bufSize int) []string {
	panic("not implemented")
}
