package events

import (
	"fmt"
	"slices"
	"strings"

	"github.com/daishakpatel/flowforge/internal/apperrors"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
	"github.com/daishakpatel/flowforge/internal/workflow/statemachine"
)

// Event is one 00E §7 event before it is recorded. Build it with a constructor (NewRunEvent and
// friends), which accept only typed payloads, so free text such as secrets or error messages
// cannot slip into a payload.
type Event struct {
	Type        statemachine.EventType
	Version     int    // event_version; constructors set 1
	RunID       string // "" only for cluster events (worker.*, scheduler.*), which skip workflow_events
	TaskID      string // "" for run-level events
	AggregateID string // the entity the event is about: run, task, attempt, approval, worker or node
	payload     any
}

// Payload returns the typed payload the event was built with.
func (e Event) Payload() any { return e.payload }

// RunPayload is the payload of run.* events.
type RunPayload struct {
	RunID          string          `json:"run_id"`
	DefinitionID   string          `json:"definition_id"`
	Status         model.RunStatus `json:"status"`
	PreviousStatus model.RunStatus `json:"previous_status,omitempty"`
	TaskCount      int             `json:"task_count,omitempty"` // run.created only
}

// TaskPayload is the payload of task.* events other than task.attempt.*.
type TaskPayload struct {
	RunID          string           `json:"run_id"`
	TaskID         string           `json:"task_id"`
	TaskKey        string           `json:"task_key"`
	Status         model.TaskStatus `json:"status"`
	PreviousStatus model.TaskStatus `json:"previous_status,omitempty"`
	AttemptCount   int              `json:"attempt_count"`
	RetryDelayMS   int64            `json:"retry_delay_ms,omitempty"` // task.retry_scheduled
	ErrorCode      string           `json:"error_code,omitempty"`     // task.failed, task.timed_out
}

// AttemptPayload is the payload of task.attempt.* events.
type AttemptPayload struct {
	RunID         string              `json:"run_id"`
	TaskID        string              `json:"task_id"`
	AttemptID     string              `json:"attempt_id"`
	AttemptNumber int                 `json:"attempt_number"`
	WorkerID      string              `json:"worker_id,omitempty"`
	FencingToken  int64               `json:"fencing_token"`
	Status        model.AttemptStatus `json:"status"`
	ErrorCode     string              `json:"error_code,omitempty"`
	Retryable     *bool               `json:"retryable,omitempty"`
}

// ApprovalPayload is the payload of approval.* events.
type ApprovalPayload struct {
	ApprovalID string `json:"approval_id"`
	RunID      string `json:"run_id"`
	ActionType string `json:"action_type"`
	Risk       string `json:"risk"`
	Status     string `json:"status"`
	DecidedBy  string `json:"decided_by,omitempty"`
}

// WorkerPayload is the payload of worker.* events.
type WorkerPayload struct {
	WorkerID string `json:"worker_id"`
	Reason   string `json:"reason,omitempty"` // worker.lost: a fixed reason code such as lease_expired
}

// LeaderChangedPayload is the payload of scheduler.leader_changed.
type LeaderChangedPayload struct {
	NodeID string `json:"node_id"`
	Epoch  int64  `json:"epoch"`
}

// NewRunEvent builds a run.* event (run.created through run.needs_attention).
func NewRunEvent(t statemachine.EventType, p RunPayload) Event {
	return Event{Type: t, Version: 1, RunID: p.RunID, AggregateID: p.RunID, payload: p}
}

// NewTaskEvent builds a task.* event (task.ready through task.skipped).
func NewTaskEvent(t statemachine.EventType, p TaskPayload) Event {
	return Event{Type: t, Version: 1, RunID: p.RunID, TaskID: p.TaskID, AggregateID: p.TaskID, payload: p}
}

// NewAttemptEvent builds a task.attempt.* event.
func NewAttemptEvent(t statemachine.EventType, p AttemptPayload) Event {
	return Event{Type: t, Version: 1, RunID: p.RunID, TaskID: p.TaskID, AggregateID: p.AttemptID, payload: p}
}

// NewApprovalEvent builds an approval.* event.
func NewApprovalEvent(t statemachine.EventType, p ApprovalPayload) Event {
	return Event{Type: t, Version: 1, RunID: p.RunID, AggregateID: p.ApprovalID, payload: p}
}

// NewWorkerEvent builds a worker.* event. It has no run, so it goes to the outbox only.
func NewWorkerEvent(t statemachine.EventType, p WorkerPayload) Event {
	return Event{Type: t, Version: 1, AggregateID: p.WorkerID, payload: p}
}

// NewLeaderChanged builds a scheduler.leader_changed event. It has no run, so it goes to the
// outbox only.
func NewLeaderChanged(p LeaderChangedPayload) Event {
	return Event{Type: statemachine.EventSchedulerLeaderChange, Version: 1, AggregateID: p.NodeID, payload: p}
}

// The 00E §7 topics the outbox relay publishes to.
const (
	TopicWorkflowEvents = "flowforge.workflow-events"
	TopicTaskEvents     = "flowforge.task-events"
)

// Topic returns the Kafka topic for an event type: task and attempt events go to
// flowforge.task-events, everything else to flowforge.workflow-events (00E §7).
func Topic(t statemachine.EventType) string {
	if strings.HasPrefix(string(t), "task.") {
		return TopicTaskEvents
	}
	return TopicWorkflowEvents
}

// validate rejects an unknown event type, a payload from the wrong family (for example a
// TaskPayload on a run.* type) and a missing run id on a run-scoped event.
func (e Event) validate() error {
	t := string(e.Type)
	var ok bool
	switch e.payload.(type) {
	case RunPayload:
		ok = strings.HasPrefix(t, "run.")
	case TaskPayload:
		ok = strings.HasPrefix(t, "task.") && !strings.HasPrefix(t, "task.attempt.")
	case AttemptPayload:
		ok = strings.HasPrefix(t, "task.attempt.")
	case ApprovalPayload:
		ok = strings.HasPrefix(t, "approval.")
	case WorkerPayload:
		ok = strings.HasPrefix(t, "worker.")
	case LeaderChangedPayload:
		ok = e.Type == statemachine.EventSchedulerLeaderChange
	}
	runScoped := !strings.HasPrefix(t, "worker.") && !strings.HasPrefix(t, "scheduler.")
	switch {
	case !ok || !slices.Contains(statemachine.EventTypes(), e.Type):
		return apperrors.Internal(fmt.Errorf("event type %q does not match payload %T", t, e.payload))
	case runScoped && e.RunID == "":
		return apperrors.Internal(fmt.Errorf("event %s has no run id", t))
	case e.AggregateID == "":
		return apperrors.Internal(fmt.Errorf("event %s has no aggregate id", t))
	}
	return nil
}
