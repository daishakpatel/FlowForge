// Package failpoint injects failures at named points in FlowForge for crash-consistency tests
// (task 03.04). Without the build tag `failpoint`, Inject is a no-op the compiler removes, so
// production binaries carry no failpoint logic. With it, a point is armed by
//
//	FF_FAILPOINTS=name=error|panic[,name=…]   (read on first use)
//
// or by Enable in a test. "error" makes Inject return an error wrapping ErrInjected, which rolls
// back the surrounding transaction like a crash before commit; "panic" panics, as a process
// crash would, and the transaction manager rolls back and re-raises it.
package failpoint

import "errors"

// The failpoints wired into FlowForge. Each sits inside a transaction, between two writes that
// must commit together.
const (
	// ClaimAfterTaskRunning: PollTask moved a task QUEUED → RUNNING, its attempt is not inserted.
	ClaimAfterTaskRunning = "claim/after-task-running"
	// TaskAfterUpdate: TaskRepo.Transition updated the row, its event is not recorded.
	TaskAfterUpdate = "task/after-update-before-event"
	// ProcessRunMidway: the scheduler decided readiness for a run, promotion has not run.
	ProcessRunMidway = "scheduler/process-run-midway"
	// OutcomeAfterAttemptEnded: an attempt is terminal, its task has not been moved yet.
	OutcomeAfterAttemptEnded = "outcome/after-attempt-ended"
	// OutboxAfterPublish: the relay published a batch, the rows are not marked yet (03.07). It is
	// left out of Names: the relay is not part of the run lifecycle TestCrashConsistency drives;
	// outbox's own crash test arms it.
	OutboxAfterPublish = "outbox/after-publish-before-mark"
)

// Names lists every wired failpoint, for tests that cover them all.
func Names() []string {
	return []string{ClaimAfterTaskRunning, TaskAfterUpdate, ProcessRunMidway, OutcomeAfterAttemptEnded}
}

// ErrInjected is wrapped by the error an armed "error" failpoint returns.
var ErrInjected = errors.New("failpoint: injected error")
