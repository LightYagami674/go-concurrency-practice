package orderedparalleltranslator

// TranslateOrdered fans out sentences to workers goroutines each calling
// translate, then reassembles results in original input order.
//
// Suggested shape:
//   - type job struct { index int; sentence string }
//   - type result struct { index int; output string }
//   - in  := make(chan job)          — shared work queue
//   - out := make(chan result)       — unordered results
//   - launch workers goroutines: each ranges over in, calls translate, sends result
//   - feed all jobs into in, then close(in)
//   - closer goroutine: wg.Wait(); close(out)
//   - collect into a pre-allocated []string of len(sentences), indexed by result.index
//   - return the slice
func TranslateOrdered(sentences []string, workers int, translate func(string) string) []string {
	panic("not implemented")
}
