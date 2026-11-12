// Package workerapi is the server side of the worker protocol (proto/worker/v1, task 03.01,
// ADR-004): registration, claiming, fenced completion, heartbeats and log shipping, hosted by
// flowforge-api. Later tasks change its behaviour only through the seams in this file.
package workerapi

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5"

	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
)

// AttemptKind is how an attempt ended.
type AttemptKind string

// The attempt kinds, one per terminal attempt status (00E §3.2).
const (
	AttemptSucceeded AttemptKind = "SUCCEEDED"
	AttemptFailed    AttemptKind = "FAILED"
	AttemptTimedOut  AttemptKind = "TIMED_OUT"
	AttemptAbandoned AttemptKind = "ABANDONED"
	AttemptCancelled AttemptKind = "CANCELLED"
)

// AttemptOutcome describes a finished attempt. Attempt is already in its terminal status; Task is
// still RUNNING.
type AttemptOutcome struct {
	Attempt   persistence.AttemptView
	Task      persistence.TaskView
	Kind      AttemptKind
	ErrorCode string
	Message   string
	Retryable bool
	Result    json.RawMessage
}

// OutcomeHandler decides what a finished attempt means for its task (seam, task 03.01).
// OnAttemptFinished runs in the transaction that finished the attempt, so the task's move and the
// attempt's commit or roll back together; it must change the task only through
// TaskRepo.Transition. 03.04 replaces the default with retry handling.
type OutcomeHandler interface {
	OnAttemptFinished(ctx context.Context, tx pgx.Tx, o AttemptOutcome) error
}

// TerminalOutcomeHandler is the default OutcomeHandler: no retries. Success makes the task
// SUCCEEDED (with the result); a timeout TIMED_OUT; a cancellation CANCELLED; any other failure
// FAILED.
type TerminalOutcomeHandler struct {
	Tasks persistence.TaskRepo
}

// OnAttemptFinished implements OutcomeHandler.
func (h TerminalOutcomeHandler) OnAttemptFinished(ctx context.Context, tx pgx.Tx, o AttemptOutcome) error {
	to := map[AttemptKind]model.TaskStatus{
		AttemptSucceeded: model.TaskSucceeded, AttemptTimedOut: model.TaskTimedOut, AttemptCancelled: model.TaskCancelled,
	}[o.Kind]
	if to == "" {
		to = model.TaskFailed
	}
	return h.Tasks.Transition(ctx, tx, persistence.TaskTransitionParams{
		TaskID: o.Task.ID, From: model.TaskRunning, To: to,
		Mutate: func(m *persistence.TaskMutation) { m.Result = o.Result },
	})
}

// LeaseProvider tracks which workers are alive (seam, task 03.01). The default PGLeaseProvider
// uses worker_observations.last_seen_at; 04.x replaces it with Raft-backed leases, the liveness
// authority (ADR-003). Expired and Alive are what recovery (03.04) acts on.
type LeaseProvider interface {
	// Register starts or refreshes a worker's lease. A live lease of the same id held by another
	// API key is CONFLICT.
	Register(ctx context.Context, w persistence.WorkerRegistration) error
	// Renew extends the lease; INVALID_STATE when the worker is not registered by keyID.
	Renew(ctx context.Context, workerID, keyID string, activeTasks int) error
	// Release ends the lease (graceful shutdown).
	Release(ctx context.Context, workerID, keyID string) error
	// Expired lists workers whose lease ran out without being released.
	Expired(ctx context.Context) ([]string, error)
	// Alive lists workers holding a lease.
	Alive(ctx context.Context) ([]persistence.WorkerView, error)
}

// CancelSource names attempts the worker should stop (seam, task 03.01); 03.05 implements it from
// cancelled and paused runs. CancelledAttempts returns the subset of attemptIDs to cancel.
type CancelSource interface {
	CancelledAttempts(ctx context.Context, attemptIDs []string) ([]string, error)
}

// NoCancels is the default CancelSource: nothing is ever cancelled.
type NoCancels struct{}

// CancelledAttempts implements CancelSource.
func (NoCancels) CancelledAttempts(context.Context, []string) ([]string, error) { return nil, nil }
