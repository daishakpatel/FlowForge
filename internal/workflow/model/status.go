package model

// TaskStatus is a workflow_tasks.status value (00E §2, §3.1).
type TaskStatus string

// Task statuses. SUCCEEDED, FAILED, TIMED_OUT, CANCELLED and SKIPPED are terminal.
const (
	TaskPending   TaskStatus = "PENDING"
	TaskReady     TaskStatus = "READY"
	TaskQueued    TaskStatus = "QUEUED"
	TaskRunning   TaskStatus = "RUNNING"
	TaskRetryWait TaskStatus = "RETRY_WAIT"
	TaskSucceeded TaskStatus = "SUCCEEDED"
	TaskFailed    TaskStatus = "FAILED"
	TaskTimedOut  TaskStatus = "TIMED_OUT"
	TaskCancelled TaskStatus = "CANCELLED"
	TaskSkipped   TaskStatus = "SKIPPED"
)

// TaskStatuses lists every task status in 00E §2 order.
func TaskStatuses() []TaskStatus {
	return []TaskStatus{TaskPending, TaskReady, TaskQueued, TaskRunning, TaskRetryWait,
		TaskSucceeded, TaskFailed, TaskTimedOut, TaskCancelled, TaskSkipped}
}

// AttemptStatus is a task_attempts.status value (00E §2, §3.2).
type AttemptStatus string

// Attempt statuses. Every status except RUNNING is terminal.
const (
	AttemptRunning   AttemptStatus = "RUNNING"
	AttemptSucceeded AttemptStatus = "SUCCEEDED"
	AttemptFailed    AttemptStatus = "FAILED"
	AttemptTimedOut  AttemptStatus = "TIMED_OUT"
	AttemptAbandoned AttemptStatus = "ABANDONED"
	AttemptCancelled AttemptStatus = "CANCELLED"
)

// AttemptStatuses lists every attempt status in 00E §2 order.
func AttemptStatuses() []AttemptStatus {
	return []AttemptStatus{AttemptRunning, AttemptSucceeded, AttemptFailed, AttemptTimedOut,
		AttemptAbandoned, AttemptCancelled}
}

// RunStatus is a workflow_runs.status value (00E §2, §3.3).
type RunStatus string

// Run statuses. SUCCEEDED, FAILED and CANCELLED are terminal.
const (
	RunCreated        RunStatus = "CREATED"
	RunRunning        RunStatus = "RUNNING"
	RunPaused         RunStatus = "PAUSED"
	RunSucceeded      RunStatus = "SUCCEEDED"
	RunFailed         RunStatus = "FAILED"
	RunCancelled      RunStatus = "CANCELLED"
	RunNeedsAttention RunStatus = "NEEDS_ATTENTION"
)

// RunStatuses lists every run status in 00E §2 order.
func RunStatuses() []RunStatus {
	return []RunStatus{RunCreated, RunRunning, RunPaused, RunSucceeded, RunFailed, RunCancelled, RunNeedsAttention}
}

// RunIf says when a task runs relative to its dependencies (00E §3.4, ADR-008).
type RunIf string

// The run_if values.
const (
	RunIfOnSuccess RunIf = "on_success"
	RunIfOnFailure RunIf = "on_failure"
	RunIfAlways    RunIf = "always"
	RunIfExpr      RunIf = "expr"
)

// OnFailure is the workflow-level policy for an unhandled task failure (00E §3.5).
type OnFailure string

// The on_failure values.
const (
	OnFailureFail           OnFailure = "fail"
	OnFailureNeedsAttention OnFailure = "needs_attention"
)

// RetryStrategy selects how retry delays grow (FR-WF-07).
type RetryStrategy string

// The retry strategies.
const (
	StrategyFixed       RetryStrategy = "fixed"
	StrategyExponential RetryStrategy = "exponential"
)
