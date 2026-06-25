# 11 — Parallel Hash Fan-Out/Fan-In

Fan out a stream of items to a pool of workers that compute SHA-256 hashes,
then fan all results back into a single output channel that the caller drains.

## Signatures

```go
type HashResult struct {
    Input string
    Hash  string // lowercase hex SHA-256 of Input
}

// FanOutHashIn fans out inputs to workers goroutines. Each worker reads from a
// shared input channel, computes the SHA-256 hex digest of the item, and sends
// a HashResult to a shared output channel. A merge goroutine (gated by a
// sync.WaitGroup) closes the output channel only after every worker is done.
//
// Output order need not match input order.
// workers must be >= 1.
func FanOutHashIn(inputs []string, workers int) []HashResult
```

## Constraints

- All input items must appear in the output (no items silently dropped).
- Each HashResult.Hash must be the correct lowercase hex SHA-256 of HashResult.Input.
- The caller must not see a closed output channel before all workers have finished
  (gate the close on the WaitGroup).
- No goroutine must leak after FanOutHashIn returns.
- Must be race-free under `-race`.

## Concepts

Fan-out / fan-in, shared input channel, sync.WaitGroup, channel merge.

## Gotchas

- Closing the merged output channel before all workers finish — always gate
  `close(out)` inside a goroutine that waits on the WaitGroup.
- Forgetting to close the input channel — workers range over it and will block
  forever if it is never closed.
