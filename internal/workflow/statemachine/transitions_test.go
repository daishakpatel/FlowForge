package statemachine

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/daishakpatel/flowforge/internal/apperrors"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
)

// Independent literal copies of 00E §3.1–3.3, written as "FROM>TO". The code's tables are
// compared against these, so a typo in either shows up as a mismatch.
var (
	want00ETask = []string{
		"PENDING>READY", "PENDING>SKIPPED", "PENDING>CANCELLED",
		"READY>QUEUED", "READY>CANCELLED", "QUEUED>CANCELLED",
		"QUEUED>RUNNING",
		"RUNNING>SUCCEEDED", "RUNNING>RETRY_WAIT", "RUNNING>FAILED", "RUNNING>TIMED_OUT", "RUNNING>CANCELLED",
		"RETRY_WAIT>QUEUED", "RETRY_WAIT>CANCELLED",
	}
	want00EAttempt = []string{
		"RUNNING>SUCCEEDED", "RUNNING>FAILED", "RUNNING>TIMED_OUT", "RUNNING>ABANDONED", "RUNNING>CANCELLED",
	}
	want00ERun = []string{
		"CREATED>RUNNING",
		"RUNNING>SUCCEEDED", "RUNNING>FAILED", "RUNNING>CANCELLED", "RUNNING>NEEDS_ATTENTION",
		"RUNNING>PAUSED", "PAUSED>RUNNING",
		"NEEDS_ATTENTION>RUNNING", "NEEDS_ATTENTION>FAILED", "NEEDS_ATTENTION>CANCELLED",
	}
)

// checkMatrix asserts can(from,to) for every pair equals membership in want, and that the
// error-returning form agrees.
func checkMatrix[S ~string](t *testing.T, kind string, all []S, want []string, can func(S, S) bool, check func(S, S) error) {
	t.Helper()
	allowed := 0
	for _, from := range all {
		for _, to := range all {
			exp := slices.Contains(want, string(from)+">"+string(to))
			if got := can(from, to); got != exp {
				t.Errorf("%s %s → %s: allowed = %v, 00E says %v", kind, from, to, got, exp)
			}
			err := check(from, to)
			switch {
			case exp && err != nil:
				t.Errorf("%s %s → %s: unexpected error %v", kind, from, to, err)
			case !exp && (!errors.Is(err, ErrInvalidTransition) || apperrors.CodeOf(err) != apperrors.CodeInvalidState):
				t.Errorf("%s %s → %s: want INVALID_STATE wrapping ErrInvalidTransition, got %v", kind, from, to, err)
			}
			if exp {
				allowed++
			}
		}
	}
	if allowed != len(want) {
		t.Errorf("%s: %d allowed pairs, 00E lists %d (a status in want is missing from the enum?)", kind, allowed, len(want))
	}
}

func TestTransitionMatrices_Match00E(t *testing.T) {
	t.Parallel()
	checkMatrix(t, "task", model.TaskStatuses(), want00ETask, CanTaskTransition, TaskTransition)
	checkMatrix(t, "attempt", model.AttemptStatuses(), want00EAttempt, CanAttemptTransition, AttemptTransition)
	checkMatrix(t, "run", model.RunStatuses(), want00ERun, CanRunTransition, RunTransition)
	if CanTaskTransition("BOGUS", model.TaskReady) || CanRunTransition(model.RunCreated, "BOGUS") {
		t.Error("unknown statuses must never transition")
	}
}

func TestTerminalSets(t *testing.T) {
	t.Parallel()
	for _, s := range model.TaskStatuses() {
		want := slices.Contains([]model.TaskStatus{model.TaskSucceeded, model.TaskFailed, model.TaskTimedOut, model.TaskCancelled, model.TaskSkipped}, s)
		if IsTerminalTask(s) != want {
			t.Errorf("IsTerminalTask(%s) = %v", s, !want)
		}
		// A terminal task has no outgoing transition (00E §3.1).
		for _, to := range model.TaskStatuses() {
			if want && CanTaskTransition(s, to) {
				t.Errorf("terminal %s may move to %s", s, to)
			}
		}
	}
	for _, s := range model.AttemptStatuses() {
		if IsTerminalAttempt(s) != (s != model.AttemptRunning) {
			t.Errorf("IsTerminalAttempt(%s)", s)
		}
	}
	if IsTerminalAttempt("BOGUS") {
		t.Error("unknown attempt status is not terminal")
	}
	for _, s := range model.RunStatuses() {
		want := s == model.RunSucceeded || s == model.RunFailed || s == model.RunCancelled
		if IsTerminalRun(s) != want {
			t.Errorf("IsTerminalRun(%s) = %v", s, !want)
		}
	}
}

func TestEventTypes_Match00E(t *testing.T) {
	t.Parallel()
	var got []string
	for _, e := range EventTypes() {
		got = append(got, string(e))
	}
	// Literal copy of the 00E §7 event-type list, expanded.
	want := "run.created run.started run.paused run.resumed run.succeeded run.failed run.cancelled run.needs_attention " +
		"task.ready task.queued task.started task.succeeded task.failed task.retry_scheduled task.timed_out task.cancelled task.skipped " +
		"task.attempt.started task.attempt.succeeded task.attempt.failed task.attempt.timed_out task.attempt.abandoned " +
		"approval.requested approval.decided approval.executed worker.registered worker.lost scheduler.leader_changed"
	if strings.Join(got, " ") != want {
		t.Errorf("EventTypes() =\n%s\nwant\n%s", strings.Join(got, " "), want)
	}
}

func TestTransitionEvents(t *testing.T) {
	t.Parallel()
	for _, from := range model.TaskStatuses() {
		for _, to := range model.TaskStatuses() {
			e := TaskTransitionEvent(from, to)
			if CanTaskTransition(from, to) != (e != "") {
				t.Errorf("task %s → %s: event %q", from, to, e)
			}
		}
	}
	if TaskTransitionEvent(model.TaskRetryWait, model.TaskQueued) != EventTaskQueued ||
		TaskTransitionEvent(model.TaskRunning, model.TaskRetryWait) != EventTaskRetryScheduled ||
		TaskTransitionEvent(model.TaskQueued, model.TaskRunning) != EventTaskStarted {
		t.Error("task event mapping")
	}
	for _, to := range model.AttemptStatuses() {
		e := AttemptTransitionEvent(model.AttemptRunning, to)
		want := to != model.AttemptRunning && to != model.AttemptCancelled // 00E §7 has no attempt-cancelled event
		if (e != "") != want {
			t.Errorf("attempt RUNNING → %s: event %q", to, e)
		}
	}
	if AttemptTransitionEvent(model.AttemptFailed, model.AttemptSucceeded) != "" {
		t.Error("disallowed attempt transition has an event")
	}
	for _, tc := range []struct {
		from, to model.RunStatus
		want     EventType
	}{
		{model.RunCreated, model.RunRunning, EventRunStarted},
		{model.RunPaused, model.RunRunning, EventRunResumed},
		{model.RunNeedsAttention, model.RunRunning, EventRunResumed},
		{model.RunRunning, model.RunPaused, EventRunPaused},
		{model.RunRunning, model.RunSucceeded, EventRunSucceeded},
		{model.RunRunning, model.RunFailed, EventRunFailed},
		{model.RunNeedsAttention, model.RunCancelled, EventRunCancelled},
		{model.RunRunning, model.RunNeedsAttention, EventRunNeedsAttention},
		{model.RunCreated, model.RunCancelled, ""},
	} {
		if got := RunTransitionEvent(tc.from, tc.to); got != tc.want {
			t.Errorf("run %s → %s: event %q, want %q", tc.from, tc.to, got, tc.want)
		}
	}
}
