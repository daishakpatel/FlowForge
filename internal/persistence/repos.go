// Package persistence is the contract of the Postgres access layer (task 02.03): the
// transaction manager, the repository interfaces, their parameter types and the read models.
// The Postgres implementations live in internal/persistence/pg.
//
// Rules every caller follows (docs/architecture/persistence.md):
//   - A status changes only through a repository Transition method, which checks the state
//     machine first and then runs a conditional UPDATE … WHERE status = $from (ADR-020).
//   - Every Transition, and every create that emits an event, records workflow_events + outbox in
//     the same transaction, so those methods take a pgx.Tx rather than a Querier (ADR-009).
//   - Scheduler transactions call ClusterRepo.AssertEpoch first, in the same transaction (ADR-003).
package persistence

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daishakpatel/flowforge/internal/events"
	"github.com/daishakpatel/flowforge/internal/persistence/sqlcgen"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
)

// ErrStaleEpoch is wrapped by the NOT_LEADER error AssertEpoch returns when the caller's epoch
// is not the stored one: another scheduler has taken over, or this one never raised the fence.
var ErrStaleEpoch = errors.New("stale scheduler epoch")

// Querier runs single statements: a *pgxpool.Pool or a pgx.Tx.
type Querier = sqlcgen.DBTX

// TxManager runs fn in a transaction. It commits when fn returns nil and rolls back when fn
// returns an error or panics (the panic is re-raised). Serialization failures and deadlocks
// rerun fn, so fn must have no side effects outside tx.
type TxManager interface {
	WithTx(ctx context.Context, opts pgx.TxOptions, fn func(tx pgx.Tx) error) error
}

// DefinitionRepo stores immutable workflow definitions (FR-WF-01).
type DefinitionRepo interface {
	// Create inserts a definition; a duplicate (name, version) is CONFLICT. The caller has
	// already decoded and validated it (model.Decode, dag.Validate).
	Create(ctx context.Context, q Querier, d NewDefinition) (DefinitionView, error)
	Get(ctx context.Context, q Querier, id string) (DefinitionView, error)
	GetByNameVersion(ctx context.Context, q Querier, name string, version int) (DefinitionView, error)
	// Latest returns the highest version of name.
	Latest(ctx context.Context, q Querier, name string) (DefinitionView, error)
	// List returns one keyset page, newest first, and the cursor of the next page ("" at the end).
	List(ctx context.Context, q Querier, page Page) ([]DefinitionView, string, error)
}

// RunRepo stores workflow runs.
type RunRepo interface {
	// CreateWithTasks inserts the run (CREATED), one PENDING task row per definition task in
	// topological order, and the run.created event, all in tx. def must be the definition that
	// run.DefinitionID names.
	CreateWithTasks(ctx context.Context, tx pgx.Tx, run NewRun, def *model.Definition) (RunView, error)
	Get(ctx context.Context, q Querier, id string) (RunView, error)
	List(ctx context.Context, q Querier, f RunFilter, page Page) ([]RunView, string, error)
	Transition(ctx context.Context, tx pgx.Tx, p RunTransitionParams) error
}

// TaskRepo stores workflow tasks.
type TaskRepo interface {
	Get(ctx context.Context, q Querier, id string) (TaskView, error)
	// ListByRun returns all of a run's task rows (including superseded ones) in insertion order.
	ListByRun(ctx context.Context, q Querier, runID string) ([]TaskView, error)
	ListByRunStatus(ctx context.Context, q Querier, runID string, status model.TaskStatus) ([]TaskView, error)
	Transition(ctx context.Context, tx pgx.Tx, p TaskTransitionParams) error
}

// AttemptRepo stores task attempts.
type AttemptRepo interface {
	// Create inserts a RUNNING attempt with a fresh fencing token and records
	// task.attempt.started. The caller moves the task QUEUED → RUNNING in the same tx.
	Create(ctx context.Context, tx pgx.Tx, a NewAttempt) (AttemptView, error)
	Get(ctx context.Context, q Querier, id string) (AttemptView, error)
	ListByTask(ctx context.Context, q Querier, taskID string) ([]AttemptView, error)
	Transition(ctx context.Context, tx pgx.Tx, p AttemptTransitionParams) error
}

// EventRepo reads a run's recorded history. Events are written by events.Recorder.
type EventRepo interface {
	// ListByRun returns up to limit events with workflow_events.id > afterID, oldest first.
	ListByRun(ctx context.Context, q Querier, runID string, afterID int64, limit int) ([]EventView, error)
}

// ClusterRepo reads and raises the scheduler epoch fence in cluster_state (ADR-003). Epoch 0
// means unfenced single-scheduler mode.
type ClusterRepo interface {
	GetEpoch(ctx context.Context, q Querier) (epoch int64, leaderID string, err error)
	// SetEpochIfGreater stores epoch and leaderID when epoch is above the stored epoch and
	// reports whether it did. It waits for transactions holding AssertEpoch's share lock.
	SetEpochIfGreater(ctx context.Context, q Querier, epoch int64, leaderID string) (bool, error)
	// AssertEpoch share-locks the fence row until tx ends and returns NOT_LEADER (wrapping
	// ErrStaleEpoch) unless the stored epoch equals epoch.
	AssertEpoch(ctx context.Context, tx pgx.Tx, epoch int64) error
}

// NewDefinition is the input of DefinitionRepo.Create.
type NewDefinition struct {
	Name       string
	Version    int
	Definition json.RawMessage // the validated definition JSON
	CreatedBy  string
}

// NewRun is the input of RunRepo.CreateWithTasks. Empty strings store NULL.
type NewRun struct {
	DefinitionID   string
	Input          json.RawMessage // nil stores {}
	IdempotencyKey string          // a duplicate is CONFLICT
	TriggerID      string
	ParentRunID    string
	CreatedBy      string
}

// RunFilter narrows RunRepo.List (00E §9). Zero values mean "no filter".
type RunFilter struct {
	Status   model.RunStatus
	Workflow string    // definition name
	Since    time.Time // created_at >= Since
	Until    time.Time // created_at < Until
}

// RunTransitionParams is the input of RunRepo.Transition. The UPDATE also requires
// version = Version (optimistic concurrency), so a stale read fails with CONFLICT.
type RunTransitionParams struct {
	RunID       string
	From, To    model.RunStatus
	Version     int64
	LeaderEpoch *int64        // stamped into leader_epoch when set
	Event       *events.Event // replaces the default event derived from (From, To)
}

// TaskMutation holds the column changes a task transition makes besides status. Nil and zero
// fields leave the column unchanged.
type TaskMutation struct {
	Input            json.RawMessage // resolved input (READY → QUEUED)
	Result           json.RawMessage
	IncrementAttempt bool           // attempt_count++ (QUEUED → RUNNING)
	RetryDelay       *time.Duration // available_at = DB now() + delay (ADR-021)
}

// TaskTransitionParams is the input of TaskRepo.Transition, the only way to change a task's
// status (design contract of task 02.03 §5).
type TaskTransitionParams struct {
	TaskID   string
	From, To model.TaskStatus
	Mutate   func(*TaskMutation) // optional
	Event    *events.Event       // replaces the default event derived from (From, To)
}

// NewAttempt is the input of AttemptRepo.Create.
type NewAttempt struct {
	TaskID         string
	RunID          string // for the event
	AttemptNumber  int
	WorkerID       string
	TimeoutSeconds int // deadline_at = DB now() + timeout (ADR-021)
}

// AttemptMutation holds what a finished attempt records. Zero fields store NULL.
type AttemptMutation struct {
	ErrorCode    string
	ErrorMessage string // stored on the attempt, never put in an event
	Retryable    *bool
	Result       json.RawMessage
}

// AttemptTransitionParams is the input of AttemptRepo.Transition. The UPDATE also requires
// fencing_token = FencingToken; a mismatch, or an attempt no longer in From, is STALE_ATTEMPT
// (ADR-003, 00E §4).
type AttemptTransitionParams struct {
	AttemptID    string
	From, To     model.AttemptStatus
	FencingToken int64
	Mutate       func(*AttemptMutation) // optional
	Event        *events.Event          // replaces the default event derived from (From, To)
}
