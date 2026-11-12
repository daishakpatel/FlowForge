package rest

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
	"github.com/daishakpatel/flowforge/internal/workflow/statemachine"
)

// taskResponse is the Task schema.
type taskResponse struct {
	ID               string            `json:"id"`
	RunID            string            `json:"run_id"`
	Key              string            `json:"task_key"`
	Type             string            `json:"task_type"`
	Queue            string            `json:"queue"`
	Status           model.TaskStatus  `json:"status"`
	DependsOn        []string          `json:"depends_on"`
	RunIf            model.RunIf       `json:"run_if"`
	RunIfExpr        string            `json:"run_if_expr,omitempty"`
	Config           json.RawMessage   `json:"config"`
	Input            json.RawMessage   `json:"input,omitempty"`
	Result           json.RawMessage   `json:"result,omitempty"`
	RetryPolicy      model.RetryPolicy `json:"retry_policy"`
	TimeoutSeconds   int               `json:"timeout_seconds"`
	AttemptCount     int               `json:"attempt_count"`
	AvailableAt      time.Time         `json:"available_at"`
	SupersedesTaskID string            `json:"supersedes_task_id,omitempty"`
	CreatedAt        time.Time         `json:"created_at"`
	UpdatedAt        time.Time         `json:"updated_at"`
	Version          int64             `json:"version"`
}

func toTask(v persistence.TaskView) taskResponse {
	dependsOn := v.DependsOn
	if dependsOn == nil {
		dependsOn = []string{}
	}
	return taskResponse{
		ID: v.ID, RunID: v.RunID, Key: v.Key, Type: v.Type, Queue: v.Queue, Status: v.Status,
		DependsOn: dependsOn, RunIf: v.RunIf, RunIfExpr: v.RunIfExpr, Config: v.Config,
		Input: v.Input, Result: v.Result, RetryPolicy: v.RetryPolicy, TimeoutSeconds: v.TimeoutSeconds,
		AttemptCount: v.AttemptCount, AvailableAt: v.AvailableAt.UTC(), SupersedesTaskID: v.SupersedesTaskID,
		CreatedAt: v.CreatedAt.UTC(), UpdatedAt: v.UpdatedAt.UTC(), Version: v.Version,
	}
}

// attemptResponse is the Attempt schema. The fencing token and idempotency key are internal to
// the worker protocol and not exposed.
type attemptResponse struct {
	ID              string              `json:"id"`
	TaskID          string              `json:"task_id"`
	AttemptNumber   int                 `json:"attempt_number"`
	WorkerID        string              `json:"worker_id,omitempty"`
	Status          model.AttemptStatus `json:"status"`
	StartedAt       time.Time           `json:"started_at"`
	DeadlineAt      time.Time           `json:"deadline_at"`
	LastHeartbeatAt *time.Time          `json:"last_heartbeat_at,omitempty"`
	FinishedAt      *time.Time          `json:"finished_at,omitempty"`
	ErrorCode       string              `json:"error_code,omitempty"`
	ErrorMessage    string              `json:"error_message,omitempty"`
	Retryable       *bool               `json:"retryable,omitempty"`
	Result          json.RawMessage     `json:"result,omitempty"`
}

func toAttempt(v persistence.AttemptView) attemptResponse {
	return attemptResponse{
		ID: v.ID, TaskID: v.TaskID, AttemptNumber: v.AttemptNumber, WorkerID: v.WorkerID, Status: v.Status,
		StartedAt: v.StartedAt.UTC(), DeadlineAt: v.DeadlineAt.UTC(), LastHeartbeatAt: utc(v.LastHeartbeatAt),
		FinishedAt: utc(v.FinishedAt), ErrorCode: v.ErrorCode, ErrorMessage: v.ErrorMessage,
		Retryable: v.Retryable, Result: v.Result,
	}
}

// eventResponse is the Event schema.
type eventResponse struct {
	ID            int64                  `json:"id"`
	EventID       string                 `json:"event_id"`
	RunID         string                 `json:"run_id"`
	TaskID        string                 `json:"task_id,omitempty"`
	Type          statemachine.EventType `json:"event_type"`
	Version       int                    `json:"event_version"`
	Payload       json.RawMessage        `json:"payload"`
	CorrelationID string                 `json:"correlation_id"`
	CreatedAt     time.Time              `json:"created_at"`
}

func toEvent(v persistence.EventView) eventResponse {
	return eventResponse{
		ID: v.ID, EventID: v.EventID, RunID: v.RunID, TaskID: v.TaskID, Type: v.Type, Version: v.Version,
		Payload: v.Payload, CorrelationID: v.CorrelationID, CreatedAt: v.CreatedAt.UTC(),
	}
}

func (h *handlers) listRunTasks(w http.ResponseWriter, r *http.Request) {
	views, err := h.runs.Tasks(r.Context(), chi.URLParam(r, "run_id"))
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, newList(mapList(views, toTask), ""))
}

func (h *handlers) listRunEvents(w http.ResponseWriter, r *http.Request) {
	page, err := pageParams(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	after, err := decodeEventCursor(page.Cursor)
	if err != nil {
		writeError(w, r, err)
		return
	}
	views, err := h.runs.Events(r.Context(), chi.URLParam(r, "run_id"), after, page.Limit)
	if err != nil {
		writeError(w, r, err)
		return
	}
	next := ""
	if len(views) == page.Limit {
		next = encodeEventCursor(views[len(views)-1].ID)
	}
	writeJSON(w, http.StatusOK, newList(mapList(views, toEvent), next))
}

func (h *handlers) getTask(w http.ResponseWriter, r *http.Request) {
	v, err := h.runs.Task(r.Context(), chi.URLParam(r, "task_id"))
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toTask(v))
}

func (h *handlers) listTaskAttempts(w http.ResponseWriter, r *http.Request) {
	views, err := h.runs.Attempts(r.Context(), chi.URLParam(r, "task_id"))
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, newList(mapList(views, toAttempt), ""))
}
