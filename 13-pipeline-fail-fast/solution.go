package pipelinefailfast

// Process fans inputs out to workers goroutines each calling transform.
// The first error cancels remaining work via a done channel and is returned
// to the caller. On success all transformed strings are returned.
//
// Suggested shape:
//   - done    := make(chan struct{})
//   - errCh   := make(chan error, 1)      — buffered so the first error never blocks
//   - results := make(chan string, len(inputs))
//   - var once sync.Once                 — guards close(done) so double-close is impossible
//   - var wg sync.WaitGroup
//   - in := make(chan string)
//   - launch workers goroutines:
//       for item := range in {
//           select { case <-done: return; default: }
//           out, err := transform(item)
//           if err != nil {
//               once.Do(func() { close(done) })
//               select { case errCh <- err: default: }
//               return
//           }
//           select { case results <- out: case <-done: return }
//       }
//   - feed items into in, then close(in)
//   - wg.Wait(); close(results)
//   - drain results; return nil error on success, or <-errCh on failure
func Process(inputs []string, workers int, transform func(string) (string, error)) ([]string, error) {
	panic("not implemented")
}
