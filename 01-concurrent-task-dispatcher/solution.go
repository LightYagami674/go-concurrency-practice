package taskdispatcher

// Task is a unit of independent work that computes and returns a result.
type Task func() int

// Dispatch runs every task in its own goroutine and returns a slice of results
// where results[i] is the value returned by tasks[i].
//
// Results are gathered via channels. Dispatch must not return until every task
// has finished. Ordering of completion is arbitrary; ordering of the returned
// slice is not — it must align with the input.
//
// TODO: implement.
func Dispatch(tasks []Task) []int {
	panic("not implemented")
}
