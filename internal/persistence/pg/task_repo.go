package pg

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/daishakpatel/flowforge/internal/apperrors"
	"github.com/daishakpatel/flowforge/internal/events"
	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/persistence/sqlcgen"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
	"github.com/daishakpatel/flowforge/internal/workflow/statemachine"
)

// TaskRepo implements persistence.TaskRepo.
type TaskRepo struct {
	events *events.Recorder
}

var _ persistence.TaskRepo = (*TaskRepo)(nil)

// NewTaskRepo returns a TaskRepo that records transition events with rec.
func NewTaskRepo(rec *events.Recorder) *TaskRepo { return &TaskRepo{events: rec} }

// Get implements persistence.TaskRepo.
func (r *TaskRepo) Get(ctx context.Context, q persistence.Querier, id string) (persistence.TaskView, error) {
	row, err := sqlcgen.New(q).GetTask(ctx, id)
	if err != nil {
		return persistence.TaskView{}, notFoundOr(err, "task")
	}
	return taskView(row)
}

// ListByRun implements persistence.TaskRepo.
func (r *TaskRepo) ListByRun(ctx context.Context, q persistence.Querier, runID string) ([]persistence.TaskView, error) {
	rows, err := sqlcgen.New(q).ListTasksByRun(ctx, sqlcgen.ListTasksByRunParams{RunID: runID, Lim: maxRows})
	if err != nil {
		return nil, fmt.Errorf("list tasks of run %s: %w", runID, err)
	}
	return taskViews(rows)
}

// ListByRunStatus implements persistence.TaskRepo.
func (r *TaskRepo) ListByRunStatus(ctx context.Context, q persistence.Querier, runID string, status model.TaskStatus) ([]persistence.TaskView, error) {
	rows, err := sqlcgen.New(q).ListTasksByRunStatus(ctx, sqlcgen.ListTasksByRunStatusParams{
		RunID: runID, Status: string(status), Lim: maxRows,
	})
	if err != nil {
		return nil, fmt.Errorf("list %s tasks of run %s: %w", status, runID, err)
	}
	return taskViews(rows)
}

// Transition implements persistence.TaskRepo: the only way to change a task's status. An
// illegal (From, To) is INVALID_STATE before any SQL. The UPDATE matches only a row still in
// From, so of concurrent callers exactly one wins; the others get INVALID_STATE (NOT_FOUND when
// the task does not exist). The event is recorded in tx.
func (r *TaskRepo) Transition(ctx context.Context, tx pgx.Tx, p persistence.TaskTransitionParams) error {
	if err := statemachine.TaskTransition(p.From, p.To); err != nil {
		return err
	}
	var m persistence.TaskMutation
	if p.Mutate != nil {
		p.Mutate(&m)
	}
	args := sqlcgen.TransitionTaskParams{
		ToStatus: string(p.To), Input: m.Input, Result: m.Result,
		ID: p.TaskID, FromStatus: string(p.From),
	}
	if m.IncrementAttempt {
		args.AttemptIncrement = 1
	}
	if m.RetryDelay != nil {
		args.RetryDelayMs = pgtype.Int8{Int64: m.RetryDelay.Milliseconds(), Valid: true}
	}
	row, err := sqlcgen.New(tx).TransitionTask(ctx, args)
	if errors.Is(err, pgx.ErrNoRows) {
		return r.explainMiss(ctx, tx, p)
	}
	if err != nil {
		return fmt.Errorf("transition task %s: %w", p.TaskID, err)
	}
	ev := events.NewTaskEvent(statemachine.TaskTransitionEvent(p.From, p.To), events.TaskPayload{
		RunID: row.RunID, TaskID: row.ID, TaskKey: row.TaskKey, Status: p.To, PreviousStatus: p.From,
		AttemptCount: int(row.AttemptCount), RetryDelayMS: args.RetryDelayMs.Int64,
	})
	if p.Event != nil {
		ev = *p.Event
	}
	return r.events.Record(ctx, tx, ev)
}

func (r *TaskRepo) explainMiss(ctx context.Context, tx pgx.Tx, p persistence.TaskTransitionParams) error {
	cur, err := r.Get(ctx, tx, p.TaskID)
	if err != nil {
		return err
	}
	return apperrors.InvalidState(fmt.Sprintf("task %s is %s, not %s", p.TaskID, cur.Status, p.From))
}
