package testutil

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// recordingTB captures a failure instead of stopping the test, so WaitFor's failure path can be
// asserted. Methods not overridden here panic via the nil embedded TB, which keeps the fake honest.
type recordingTB struct {
	testing.TB
	failed  bool
	skipped bool
	msg     string
}

func (r *recordingTB) Helper() {}

func (r *recordingTB) Fatalf(format string, args ...any) {
	r.failed = true
	r.msg = fmt.Sprintf(format, args...)
}

func (r *recordingTB) Skipf(format string, args ...any) {
	r.skipped = true
	r.msg = fmt.Sprintf(format, args...)
}

func TestEventually_SucceedsWhenConditionBecomesTrue(t *testing.T) {
	calls := 0
	ok := Eventually(func() bool { calls++; return calls == 3 }, time.Second, time.Millisecond)
	if !ok {
		t.Fatal("Eventually returned false, want true")
	}
	if calls != 3 {
		t.Errorf("cond called %d times, want 3", calls)
	}
}

func TestEventually_FalseOnTimeout(t *testing.T) {
	if Eventually(func() bool { return false }, 20*time.Millisecond, time.Millisecond) {
		t.Fatal("Eventually returned true for a condition that never holds")
	}
}

func TestWaitFor_PassesWhenConditionHolds(t *testing.T) {
	rec := &recordingTB{}
	WaitFor(rec, func() bool { return true }, 20*time.Millisecond)
	if rec.failed {
		t.Fatalf("WaitFor failed: %s", rec.msg)
	}
}

func TestWaitFor_FailsWithMessageOnTimeout(t *testing.T) {
	rec := &recordingTB{}
	WaitFor(rec, func() bool { return false }, 20*time.Millisecond)
	if !rec.failed {
		t.Fatal("WaitFor did not fail for a condition that never holds")
	}
	if want := "condition not met within 20ms"; !strings.Contains(rec.msg, want) {
		t.Errorf("failure message %q does not contain %q", rec.msg, want)
	}
}
