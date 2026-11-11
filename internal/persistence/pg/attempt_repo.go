package pg

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/daishakpatel/flowforge/internal/apperrors"
	"github.com/daishakpatel/flowforge/internal/events"
	"github.com/daishakpatel/flowforge/internal/ids"
	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/persistence/sqlcgen"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
	"github.com/daishakpatel/flowforge/internal/workflow/statemachine"
)

// AttemptRepo implements persistence.AttemptRepo.
type AttemptRepo struct {
	ids    ids.Generator
	events *events.Recorder
}

var _ persistence.AttemptRepo = (*AttemptRepo)(nil)

// NewAttemptRepo returns an AttemptRepo that takes attempt IDs from g and records events with
// rec.
func NewAttemptRepo(g ids.Generator, rec *events.Recorder) *AttemptRepo {
	return &AttemptRepo{ids: g, events: rec}
}

// Create implements persistence.AttemptRepo. The idempotency key is task_id:attempt_number
// (ADR-007); a repeated attempt number violates UNIQUE (task_id, attempt_number) → CONFLICT.
func (r *AttemptRepo) Create(ctx context.Context, tx pgx.Tx, a persistence.NewAttempt) (persistence.AttemptView, error) {
	number, err := int32Of("attempt_number", a.AttemptNumber)
	if err != nil {
		return persistence.AttemptView{}, err
	}
	timeout, err := int32Of("timeout_seconds", a.TimeoutSeconds)
	if err != nil {
		return persistence.AttemptView{}, err
	}
	row, err := sqlcgen.New(tx).InsertAttempt(ctx, sqlcgen.InsertAttemptParams{
		ID: r.ids.New(ids.PrefixAttempt), TaskID: a.TaskID, AttemptNumber: number,
		WorkerID: text(a.WorkerID), IdempotencyKey: a.TaskID + ":" + strconv.Itoa(a.AttemptNumber),
		TimeoutSeconds: timeout,
	})
	if err != nil {
		msg := fmt.Sprintf("task %s already has attempt %d", a.TaskID, a.AttemptNumber)
		return persistence.AttemptView{}, conflictOr(err, msg, "insert attempt")
	}
	ev := events.NewAttemptEvent(statemachine.EventAttemptStarted, events.AttemptPayload{
		RunID: a.RunID, TaskID: row.TaskID, AttemptID: row.ID, AttemptNumber: a.AttemptNumber,
		WorkerID: a.WorkerID, FencingToken: row.FencingToken, Status: model.AttemptRunning,
	})
	if err := r.events.Record(ctx, tx, ev); err != nil {
		return persistence.AttemptView{}, err
	}
	return attemptView(row), nil
}

// Get implements persistence.AttemptRepo.
func (r *AttemptRepo) Get(ctx context.Context, q persistence.Querier, id string) (persistence.AttemptView, error) {
	row, err := sqlcgen.New(q).GetAttempt(ctx, id)
	if err != nil {
		return persistence.AttemptView{}, notFoundOr(err, "attempt")
	}
	return attemptView(row), nil
}

// ListByTask implements persistence.AttemptRepo.
func (r *AttemptRepo) ListByTask(ctx context.Context, q persistence.Querier, taskID string) ([]persistence.AttemptView, error) {
	rows, err := sqlcgen.New(q).ListAttemptsByTask(ctx, sqlcgen.ListAttemptsByTaskParams{TaskID: taskID, Lim: maxRows})
	if err != nil {
		return nil, fmt.Errorf("list attempts of task %s: %w", taskID, err)
	}
	out := make([]persistence.AttemptView, len(rows))
	for i, row := range rows {
		out[i] = attemptView(row)
	}
	return out, nil
}

// Transition implements persistence.AttemptRepo. An illegal (From, To) is INVALID_STATE before
// any SQL. The UPDATE matches only the attempt still in From with the given fencing token
// (ADR-003); otherwise the result is STALE_ATTEMPT, or NOT_FOUND when the attempt does not
// exist. RUNNING → CANCELLED records no event: 00E §7 has none, the task's task.cancelled
// covers it.
func (r *AttemptRepo) Transition(ctx context.Context, tx pgx.Tx, p persistence.AttemptTransitionParams) error {
	if err := statemachine.AttemptTransition(p.From, p.To); err != nil {
		return err
	}
	var m persistence.AttemptMutation
	if p.Mutate != nil {
		p.Mutate(&m)
	}
	var retryable pgtype.Bool
	if m.Retryable != nil {
		retryable = pgtype.Bool{Bool: *m.Retryable, Valid: true}
	}
	row, err := sqlcgen.New(tx).FinishAttempt(ctx, sqlcgen.FinishAttemptParams{
		ToStatus: string(p.To), ErrorCode: text(m.ErrorCode), ErrorMessage: text(m.ErrorMessage),
		Retryable: retryable, Result: m.Result, ID: p.AttemptID, FromStatus: string(p.From),
		FencingToken: p.FencingToken,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		if _, gerr := r.Get(ctx, tx, p.AttemptID); gerr != nil {
			return gerr
		}
		return apperrors.StaleAttempt(fmt.Sprintf("attempt %s is not %s with fencing token %d", p.AttemptID, p.From, p.FencingToken))
	}
	if err != nil {
		return fmt.Errorf("transition attempt %s: %w", p.AttemptID, err)
	}
	ev := p.Event
	if ev == nil {
		t := statemachine.AttemptTransitionEvent(p.From, p.To)
		if t == "" {
			return nil
		}
		e := events.NewAttemptEvent(t, events.AttemptPayload{
			RunID: row.RunID, TaskID: row.TaskID, AttemptID: row.ID, AttemptNumber: int(row.AttemptNumber),
			WorkerID: row.WorkerID.String, FencingToken: row.FencingToken, Status: p.To,
			ErrorCode: m.ErrorCode, Retryable: m.Retryable,
		})
		ev = &e
	}
	return r.events.Record(ctx, tx, *ev)
}
