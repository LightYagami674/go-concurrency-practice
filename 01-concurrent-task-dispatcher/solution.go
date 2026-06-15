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

type TaskResult struct {
	TaskIndex  int
	TaskResult int
}

func Dispatch(tasks []Task) []int {
	ch := make(chan TaskResult)

	n := len(tasks)
	result := make([]int, n)

	for i, task := range tasks {
		go func() {
			taskResult := task()
			ch <- TaskResult{i, taskResult}
		}()
	}

	for range tasks {
		taskResult := <-ch
		result[taskResult.TaskIndex] = taskResult.TaskResult
	}

	return result
}
