package parallelhashfanout

// HashResult is the output of hashing one input string.
type HashResult struct {
	Input string
	Hash  string // lowercase hex SHA-256 of Input
}

// FanOutHashIn fans out inputs to workers goroutines that each compute a
// SHA-256 hash, then fans all results back into a single slice.
// Output order need not match input order.
//
// Suggested shape:
//   - in  := make(chan string)        — shared work queue, closed after all items sent
//   - out := make(chan HashResult)    — merged results channel
//   - var wg sync.WaitGroup
//   - launch workers goroutines: each ranges over in, computes hash, sends to out
//   - launch a closer goroutine: wg.Wait(); close(out)
//   - feed in <- item for each input, then close(in)
//   - collect: for r := range out { results = append(results, r) }
//   - return results
func FanOutHashIn(inputs []string, workers int) []HashResult {
	panic("not implemented")
}
