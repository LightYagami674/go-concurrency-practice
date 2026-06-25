# 12 — Ordered Parallel Translator

Take the fan-out/fan-in pattern from problem 11 and apply it to "translate
sentence" work — but results **must come back in the same order as the inputs**
were submitted, even though workers finish at different times.

## Signatures

```go
// TranslateOrdered fans out sentences to workers goroutines, each calling
// translate(sentence). Results are reassembled into the original input order
// before being returned, regardless of the order in which workers finish.
//
// workers must be >= 1.
// translate may block for arbitrary durations (do not assume uniform latency).
func TranslateOrdered(sentences []string, workers int, translate func(string) string) []string
```

## Constraints

- Output slice length must equal input slice length.
- Output[i] must equal translate(sentences[i]) for every i.
- Must not assume workers finish in submission order.
- No goroutine must leak after TranslateOrdered returns.
- Must be race-free under `-race`.

## Concepts

Order-preserving fan-in, index-tagged work items, reorder buffer.

## Gotchas

- Naive round-robin reads from per-worker channels assumes lockstep completion —
  workers finish in an unpredictable order, so this deadlocks or misordered.
- Forgetting to tag work items with their original index makes reassembly
  impossible without re-scanning.
- Blocking the collector on index i while i+1 is already ready but buffered
  nowhere — needs a reorder buffer (e.g., a map or pre-allocated slice) so
  out-of-order arrivals don't stall the pipeline.
