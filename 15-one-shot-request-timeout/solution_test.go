package oneshotrequesttimeout

import (
	"testing"
	"time"
)

const fastFetch = 10 * time.Millisecond
const slowFetch = 200 * time.Millisecond
const shortTimeout = 50 * time.Millisecond
const longTimeout = 500 * time.Millisecond

// TestResultBeforeTimeout: fetch finishes before the timer fires.
func TestResultBeforeTimeout(t *testing.T) {
	fetch := func() string {
		time.Sleep(fastFetch)
		return "response"
	}

	res := FetchWithTimeout(fetch, longTimeout)

	if res.TimedOut {
		t.Fatal("TimedOut = true, want false (fetch should have finished in time)")
	}
	if res.Value != "response" {
		t.Fatalf("Value = %q, want \"response\"", res.Value)
	}
}

// TestTimeoutBeforeResult: timer fires before fetch returns.
func TestTimeoutBeforeResult(t *testing.T) {
	fetch := func() string {
		time.Sleep(slowFetch)
		return "late response"
	}

	res := FetchWithTimeout(fetch, shortTimeout)

	if !res.TimedOut {
		t.Fatalf("TimedOut = false, want true (fetch should have timed out); Value = %q", res.Value)
	}
	if res.Value != "" {
		t.Fatalf("Value = %q on timeout, want empty string", res.Value)
	}
}

// TestReturnsFastOnTimeout: FetchWithTimeout must return promptly when it times
// out — it must not block waiting for the slow fetch to finish.
func TestReturnsFastOnTimeout(t *testing.T) {
	fetch := func() string {
		time.Sleep(2 * time.Second) // very slow
		return "never"
	}

	start := time.Now()
	res := FetchWithTimeout(fetch, shortTimeout)
	elapsed := time.Since(start)

	if !res.TimedOut {
		t.Fatal("expected timeout")
	}
	// Should return close to shortTimeout, not 2 seconds.
	if elapsed > shortTimeout*4 {
		t.Fatalf("FetchWithTimeout took %v after timeout; should return immediately (blocked on fetch?)", elapsed)
	}
}

// TestInstantFetch: fetch returns immediately (zero sleep).
func TestInstantFetch(t *testing.T) {
	fetch := func() string { return "instant" }

	res := FetchWithTimeout(fetch, longTimeout)

	if res.TimedOut {
		t.Fatal("TimedOut = true for instant fetch")
	}
	if res.Value != "instant" {
		t.Fatalf("Value = %q, want \"instant\"", res.Value)
	}
}

// TestTimerStoppedOnSuccess: run many iterations where fetch wins to verify
// no timer resource accumulation (indirect — if timers pile up and the test
// finishes without OOM or excessive GC pressure, Stop was likely called).
func TestTimerStoppedOnSuccess(t *testing.T) {
	for i := 0; i < 1000; i++ {
		fetch := func() string { return "ok" }
		res := FetchWithTimeout(fetch, longTimeout)
		if res.TimedOut {
			t.Fatalf("iteration %d: unexpected timeout", i)
		}
	}
}

// TestRaceCondition exercises concurrent FetchWithTimeout calls under -race.
func TestRaceCondition(t *testing.T) {
	done := make(chan struct{}, 40)

	for i := 0; i < 20; i++ {
		go func(i int) {
			fetch := func() string {
				time.Sleep(fastFetch)
				return "r"
			}
			FetchWithTimeout(fetch, longTimeout)
			done <- struct{}{}
		}(i)
		go func(i int) {
			fetch := func() string {
				time.Sleep(slowFetch)
				return "r"
			}
			FetchWithTimeout(fetch, shortTimeout)
			done <- struct{}{}
		}(i)
	}
	for i := 0; i < 40; i++ {
		<-done
	}
}
