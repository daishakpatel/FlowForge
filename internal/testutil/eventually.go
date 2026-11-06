package testutil

import (
	"testing"
	"time"
)

// waitForTick is how often WaitFor polls.
const waitForTick = 10 * time.Millisecond

// Eventually calls cond every tick until it returns true or timeout passes, and reports whether it
// returned true. It never fails the test; use WaitFor for that.
func Eventually(cond func() bool, timeout, tick time.Duration) bool {
	if cond() {
		return true
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	for {
		select {
		case <-deadline.C:
			return false
		case <-ticker.C:
			if cond() {
				return true
			}
		}
	}
}

// WaitFor fails the test unless cond returns true within timeout, polling every 10 ms.
func WaitFor(t testing.TB, cond func() bool, timeout time.Duration) {
	t.Helper()
	if !Eventually(cond, timeout, waitForTick) {
		t.Fatalf("testutil.WaitFor: condition not met within %s", timeout)
	}
}
