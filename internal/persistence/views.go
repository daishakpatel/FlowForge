package persistence

import (
	"encoding/json"
	"time"

	"github.com/daishakpatel/flowforge/internal/workflow/model"
	"github.com/daishakpatel/flowforge/internal/workflow/statemachine"
)

// DefinitionView is a stored workflow definition.
type DefinitionView struct {
	ID         string
	Name       string
	Version    int
	Definition json.RawMessage
	CreatedBy  string
	CreatedAt  time.Time
}

// RunView is a stored run. Nil pointers and empty strings are NULL columns.
type RunView struct {
	ID              string
	DefinitionID    string
	Status          model.RunStatus
	Input           json.RawMessage
	IdempotencyKey  string
	TriggerID       string
	ParentRunID     string
	DeadlineAt      *time.Time
	CancelRequested bool
	CreatedBy       string
	CreatedAt       time.Time
	StartedAt       *time.Time
	CompletedAt     *time.Time
	LeaderEpoch     *int64
	Version         int64 // pass to RunTransitionParams.Version
}

// TaskView is a stored task row. Nil pointers and empty strings are NULL columns.
type TaskView struct {
	ID               string
	RunID            string
	Key              string
	Type             string
	Queue            string
	Status           model.TaskStatus
	DependsOn        []string
	RunIf            model.RunIf
	RunIfExpr        string
	Config           json.RawMessage
	Input            json.RawMessage
	Result           json.RawMessage
	RetryPolicy      model.RetryPolicy
	TimeoutSeconds   int
	AttemptCount     int
	AvailableAt      time.Time
	SupersedesTaskID string
	CreatedAt        time.Time
	UpdatedAt        time.Time
	Version          int64
}

// State returns the view the pure statemachine rules (EvaluateReadiness, DeriveRunStatus) take.
func (t TaskView) State() statemachine.TaskView {
	return statemachine.TaskView{
		Key: t.Key, Status: t.Status, RunIf: t.RunIf, RunIfExpr: t.RunIfExpr,
		DependsOn: t.DependsOn, Result: t.Result,
	}
}

// AttemptView is a stored attempt. Nil pointers and empty strings are NULL columns.
type AttemptView struct {
	ID              string
	TaskID          string
	AttemptNumber   int
	WorkerID        string
	Status          model.AttemptStatus
	FencingToken    int64
	IdempotencyKey  string // task_id:attempt_number, handed to the handler (ADR-007)
	StartedAt       time.Time
	DeadlineAt      time.Time
	LastHeartbeatAt *time.Time
	FinishedAt      *time.Time
	ErrorCode       string
	ErrorMessage    string
	Retryable       *bool
	Result          json.RawMessage
}

// EventView is a recorded workflow_events row.
type EventView struct {
	ID            int64 // ordering key, e.g. SSE Last-Event-ID
	EventID       string
	RunID         string
	TaskID        string
	Type          statemachine.EventType
	Version       int
	Payload       json.RawMessage
	CorrelationID string
	CreatedAt     time.Time
}

// APIKeyView is a stored API key. SecretHash and Salt never leave the server.
type APIKeyView struct {
	ID         string
	Name       string
	Role       string
	SecretHash []byte
	Salt       []byte
	CreatedBy  string
	CreatedAt  time.Time
	ExpiresAt  *time.Time
	RevokedAt  *time.Time
}

// Active reports whether the key is neither revoked nor expired at now.
func (k APIKeyView) Active(now time.Time) bool {
	return k.RevokedAt == nil && (k.ExpiresAt == nil || k.ExpiresAt.After(now))
}

// WorkerView is a worker_observations row.
type WorkerView struct {
	WorkerID    string
	Status      string // ACTIVE | DEREGISTERED
	KeyID       string
	Capacity    int
	ActiveTasks int
	TaskTypes   []string
	Queues      []string
	Version     string
	FirstSeenAt time.Time
	LastSeenAt  time.Time
}

// Alive reports whether the worker is ACTIVE and was seen at or after liveAfter.
func (w WorkerView) Alive(liveAfter time.Time) bool {
	return w.Status == "ACTIVE" && !w.LastSeenAt.Before(liveAfter)
}

// TaskLogView is a stored task log line.
type TaskLogView struct {
	ID        int64
	AttemptID string
	TS        time.Time
	Level     string
	Message   string
	Fields    json.RawMessage
}
