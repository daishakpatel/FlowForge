package statemachine

import "github.com/daishakpatel/flowforge/internal/workflow/model"

// EventType is a workflow event type (00E §7), stored in workflow_events.event_type and the
// outbox envelope.
type EventType string

// The 00E §7 event types.
const (
	EventRunCreated        EventType = "run.created"
	EventRunStarted        EventType = "run.started"
	EventRunPaused         EventType = "run.paused"
	EventRunResumed        EventType = "run.resumed"
	EventRunSucceeded      EventType = "run.succeeded"
	EventRunFailed         EventType = "run.failed"
	EventRunCancelled      EventType = "run.cancelled"
	EventRunNeedsAttention EventType = "run.needs_attention"

	EventTaskReady          EventType = "task.ready"
	EventTaskQueued         EventType = "task.queued"
	EventTaskStarted        EventType = "task.started"
	EventTaskSucceeded      EventType = "task.succeeded"
	EventTaskFailed         EventType = "task.failed"
	EventTaskRetryScheduled EventType = "task.retry_scheduled"
	EventTaskTimedOut       EventType = "task.timed_out"
	EventTaskCancelled      EventType = "task.cancelled"
	EventTaskSkipped        EventType = "task.skipped"

	EventAttemptStarted   EventType = "task.attempt.started"
	EventAttemptSucceeded EventType = "task.attempt.succeeded"
	EventAttemptFailed    EventType = "task.attempt.failed"
	EventAttemptTimedOut  EventType = "task.attempt.timed_out"
	EventAttemptAbandoned EventType = "task.attempt.abandoned"

	EventApprovalRequested     EventType = "approval.requested"
	EventApprovalDecided       EventType = "approval.decided"
	EventApprovalExecuted      EventType = "approval.executed"
	EventWorkerRegistered      EventType = "worker.registered"
	EventWorkerLost            EventType = "worker.lost"
	EventSchedulerLeaderChange EventType = "scheduler.leader_changed"
)

// EventTypes lists every 00E §7 event type in its order.
func EventTypes() []EventType {
	return []EventType{
		EventRunCreated, EventRunStarted, EventRunPaused, EventRunResumed, EventRunSucceeded, EventRunFailed,
		EventRunCancelled, EventRunNeedsAttention,
		EventTaskReady, EventTaskQueued, EventTaskStarted, EventTaskSucceeded, EventTaskFailed,
		EventTaskRetryScheduled, EventTaskTimedOut, EventTaskCancelled, EventTaskSkipped,
		EventAttemptStarted, EventAttemptSucceeded, EventAttemptFailed, EventAttemptTimedOut, EventAttemptAbandoned,
		EventApprovalRequested, EventApprovalDecided, EventApprovalExecuted,
		EventWorkerRegistered, EventWorkerLost, EventSchedulerLeaderChange,
	}
}

// TaskTransitionEvent returns the event to record with an allowed task transition, or "" when
// the transition is not allowed.
func TaskTransitionEvent(from, to model.TaskStatus) EventType {
	if !CanTaskTransition(from, to) {
		return ""
	}
	return map[model.TaskStatus]EventType{
		model.TaskReady: EventTaskReady, model.TaskQueued: EventTaskQueued, model.TaskRunning: EventTaskStarted,
		model.TaskSucceeded: EventTaskSucceeded, model.TaskRetryWait: EventTaskRetryScheduled,
		model.TaskFailed: EventTaskFailed, model.TaskTimedOut: EventTaskTimedOut,
		model.TaskCancelled: EventTaskCancelled, model.TaskSkipped: EventTaskSkipped,
	}[to]
}

// AttemptTransitionEvent returns the event to record with an allowed attempt transition, or ""
// when it is not allowed or 00E §7 defines no event for it (RUNNING → CANCELLED; the task's
// task.cancelled covers it). task.attempt.started belongs to attempt creation, not a transition.
func AttemptTransitionEvent(from, to model.AttemptStatus) EventType {
	if !CanAttemptTransition(from, to) {
		return ""
	}
	return map[model.AttemptStatus]EventType{
		model.AttemptSucceeded: EventAttemptSucceeded, model.AttemptFailed: EventAttemptFailed,
		model.AttemptTimedOut: EventAttemptTimedOut, model.AttemptAbandoned: EventAttemptAbandoned,
	}[to]
}

// RunTransitionEvent returns the event to record with an allowed run transition, or "" when it
// is not allowed. Leaving PAUSED or NEEDS_ATTENTION for RUNNING is run.resumed; run.created
// belongs to run creation.
func RunTransitionEvent(from, to model.RunStatus) EventType {
	switch {
	case !CanRunTransition(from, to):
		return ""
	case to == model.RunRunning && from == model.RunCreated:
		return EventRunStarted
	case to == model.RunRunning:
		return EventRunResumed
	}
	return map[model.RunStatus]EventType{
		model.RunPaused: EventRunPaused, model.RunSucceeded: EventRunSucceeded, model.RunFailed: EventRunFailed,
		model.RunCancelled: EventRunCancelled, model.RunNeedsAttention: EventRunNeedsAttention,
	}[to]
}
