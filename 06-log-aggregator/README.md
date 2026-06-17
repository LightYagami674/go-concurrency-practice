# 06 — Log Aggregator

Covers: the **producer–consumer pattern** — fanning many producers into one
buffered channel, a single consumer draining it, and a clean shutdown driven by
`sync.WaitGroup` plus a single channel `close`.

## Problem

Build a log aggregator. Several **producer** goroutines each generate log lines
and send them onto a shared **buffered channel** (the queue). A **single
consumer** drains that channel and collects every line. When all producers have
finished, the pipeline shuts down cleanly: the channel is closed exactly once,
the consumer drains any remaining buffered lines, and then it exits.

This is the canonical "fan-in" shape:

```
producer 1 ┐
producer 2 ┼──►  buffered chan  ──►  single consumer  ──►  collected lines
producer 3 ┘
```

The hard part is the shutdown handshake: nobody may close the channel while a
producer might still send (that panics), and the consumer must not stop early
and drop lines that are still sitting in the buffer.

## API to implement (in `solution.go`)

```go
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
func Aggregate(producers []Producer, bufSize int) []string
```

## Requirements

1. **Buffered channel queue** — producers send lines onto a `chan string`
   created with `make(chan string, bufSize)`.
2. **One goroutine per producer** — launch them concurrently; each calls its
   `emit` for every line, which sends on the channel.
3. **Completion via WaitGroup** — producers signal completion through a
   `sync.WaitGroup`; `Add` before each launch, `defer wg.Done()` in each.
4. **Closed exactly once, by a single coordinator** — exactly one goroutine
   closes the channel, and only after `wg.Wait()` confirms all producers are
   done. Producers must never call `close`.
5. **Consumer drains fully** — the consumer keeps receiving (e.g. `for line :=
   range ch`) until the channel is closed *and* empty, so no buffered line is
   dropped, then exits.
6. **Race- & deadlock-free** — clean under `-race`; never hangs and never panics
   on a closed/again-closed channel.

## Gotchas to avoid

- **Multiple producers calling `close(ch)`:** closing an already-closed channel
  panics, and closing while another producer is mid-send panics too. Producers
  only ever send; a single coordinator closes, once, after all producers finish.
- **Consumer exiting before draining:** if the consumer stops on a side signal
  (e.g. a "done" flag) instead of on channel close, lines still buffered in the
  channel are lost. Drain until the channel is closed *and* empty.
- **Closing before producers are done:** call `close` only after `wg.Wait()`. A
  common correct shape is a coordinator goroutine: `go func(){ wg.Wait();
  close(ch) }()`, with the consumer ranging over `ch` on the main path.
- **Deadlock from an unread channel:** if the consumer isn't running while
  producers fill a small buffer, sends block forever. Start the consumer
  concurrently with the producers.

## Running

```bash
go test -race ./06-log-aggregator/...
go vet ./06-log-aggregator/...
```
