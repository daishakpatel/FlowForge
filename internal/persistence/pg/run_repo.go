package pg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/daishakpatel/flowforge/internal/apperrors"
	"github.com/daishakpatel/flowforge/internal/dag"
	"github.com/daishakpatel/flowforge/internal/events"
	"github.com/daishakpatel/flowforge/internal/ids"
	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/persistence/sqlcgen"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
	"github.com/daishakpatel/flowforge/internal/workflow/statemachine"
)

// RunRepo implements persistence.RunRepo.
type RunRepo struct {
	ids    ids.Generator
	events *events.Recorder
}

var _ persistence.RunRepo = (*RunRepo)(nil)

// NewRunRepo returns a RunRepo that takes run and task IDs from g and records events with rec.
func NewRunRepo(g ids.Generator, rec *events.Recorder) *RunRepo {
	return &RunRepo{ids: g, events: rec}
}

// CreateWithTasks implements persistence.RunRepo.
func (r *RunRepo) CreateWithTasks(ctx context.Context, tx pgx.Tx, run persistence.NewRun, def *model.Definition) (persistence.RunView, error) {
	tasks, err := r.taskRows(def)
	if err != nil {
		return persistence.RunView{}, err
	}
	timeout, err := int32Of("timeout_seconds", def.TimeoutSeconds)
	if err != nil {
		return persistence.RunView{}, err
	}
	input := run.Input
	if input == nil {
		input = json.RawMessage(`{}`)
	}
	q := sqlcgen.New(tx)
	row, err := q.InsertRun(ctx, sqlcgen.InsertRunParams{
		ID: r.ids.New(ids.PrefixRun), DefinitionID: run.DefinitionID, Input: input,
		IdempotencyKey: text(run.IdempotencyKey), TriggerID: text(run.TriggerID),
		ParentRunID: text(run.ParentRunID), TimeoutSeconds: timeout, CreatedBy: run.CreatedBy,
	})
	if err != nil {
		return persistence.RunView{}, conflictOr(err, "a run with this idempotency key already exists", "insert run")
	}
	for i := range tasks {
		tasks[i].RunID = row.ID
	}
	if _, err := q.InsertTasks(ctx, tasks); err != nil {
		return persistence.RunView{}, fmt.Errorf("insert tasks of run %s: %w", row.ID, err)
	}
	ev := events.NewRunEvent(statemachine.EventRunCreated, events.RunPayload{
		RunID: row.ID, DefinitionID: row.DefinitionID, Status: model.RunCreated, TaskCount: len(tasks),
	})
	if err := r.events.Record(ctx, tx, ev); err != nil {
		return persistence.RunView{}, err
	}
	return runView(row), nil
}

// taskRows derives a run's PENDING task rows from def, in topological order so that task IDs
// (ULIDs) sort dependencies first. RunID is filled in by the caller.
func (r *RunRepo) taskRows(def *model.Definition) ([]sqlcgen.InsertTasksParams, error) {
	order, err := dag.TopoOrder(def)
	if err != nil {
		return nil, apperrors.Wrap(apperrors.CodeInvalidWorkflow, "workflow definition is not a DAG", err)
	}
	byID := make(map[string]model.TaskDef, len(def.Tasks))
	for _, t := range def.Tasks {
		byID[t.ID] = t
	}
	rows := make([]sqlcgen.InsertTasksParams, 0, len(order))
	for _, key := range order {
		t := byID[key]
		retry, err := json.Marshal(t.Retry)
		if err != nil {
			return nil, fmt.Errorf("encode retry policy of task %s: %w", key, err)
		}
		timeout, err := int32Of("timeout_seconds", t.TimeoutSeconds)
		if err != nil {
			return nil, err
		}
		config := []byte(t.Config)
		if config == nil {
			config = []byte(`{}`)
		}
		dependsOn := t.DependsOn
		if dependsOn == nil {
			dependsOn = []string{}
		}
		rows = append(rows, sqlcgen.InsertTasksParams{
			ID: r.ids.New(ids.PrefixTask), TaskKey: key, TaskType: t.Type, QueueName: t.Queue,
			Status: string(model.TaskPending), DependsOn: dependsOn, RunIf: string(t.RunIf),
			RunIfExpr: text(t.RunIfExpr), Config: config, RetryPolicy: retry, TimeoutSeconds: timeout,
		})
	}
	return rows, nil
}

// Get implements persistence.RunRepo.
func (r *RunRepo) Get(ctx context.Context, q persistence.Querier, id string) (persistence.RunView, error) {
	row, err := sqlcgen.New(q).GetRun(ctx, id)
	if err != nil {
		return persistence.RunView{}, notFoundOr(err, "run")
	}
	return runView(row), nil
}

// GetByIdempotencyKey implements persistence.RunRepo.
func (r *RunRepo) GetByIdempotencyKey(ctx context.Context, q persistence.Querier, key string) (persistence.RunView, error) {
	row, err := sqlcgen.New(q).GetRunByIdempotencyKey(ctx, text(key))
	if err != nil {
		return persistence.RunView{}, notFoundOr(err, "run")
	}
	return runView(row), nil
}

// List implements persistence.RunRepo.
func (r *RunRepo) List(ctx context.Context, q persistence.Querier, f persistence.RunFilter, p persistence.Page) ([]persistence.RunView, string, error) {
	at, id, err := cursorArgs(p.Cursor)
	if err != nil {
		return nil, "", err
	}
	limit := p.EffectiveLimit()
	rows, err := sqlcgen.New(q).ListRuns(ctx, sqlcgen.ListRunsParams{
		Status: text(string(f.Status)), Workflow: text(f.Workflow),
		Since: timestamptz(f.Since), Until: timestamptz(f.Until),
		CursorCreatedAt: at, CursorID: id, Lim: limitOf(limit + 1),
	})
	if err != nil {
		return nil, "", fmt.Errorf("list runs: %w", err)
	}
	out := make([]persistence.RunView, len(rows))
	for i, row := range rows {
		out[i] = runView(row)
	}
	out, next := page(out, limit, func(v persistence.RunView) (time.Time, string) { return v.CreatedAt, v.ID })
	return out, next, nil
}

// Transition implements persistence.RunRepo. An illegal (From, To) is INVALID_STATE before any
// SQL. When the conditional UPDATE matches no row, a follow-up read tells NOT_FOUND (no run),
// INVALID_STATE (status is not From) and CONFLICT (version is not Version) apart.
func (r *RunRepo) Transition(ctx context.Context, tx pgx.Tx, p persistence.RunTransitionParams) error {
	if err := statemachine.RunTransition(p.From, p.To); err != nil {
		return err
	}
	var epoch pgtype.Int8
	if p.LeaderEpoch != nil {
		epoch = pgtype.Int8{Int64: *p.LeaderEpoch, Valid: true}
	}
	row, err := sqlcgen.New(tx).TransitionRun(ctx, sqlcgen.TransitionRunParams{
		ToStatus: string(p.To), Terminal: statemachine.IsTerminalRun(p.To), LeaderEpoch: epoch,
		ID: p.RunID, FromStatus: string(p.From), Version: p.Version,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return r.explainMiss(ctx, tx, p)
	}
	if err != nil {
		return fmt.Errorf("transition run %s: %w", p.RunID, err)
	}
	ev := events.NewRunEvent(statemachine.RunTransitionEvent(p.From, p.To), events.RunPayload{
		RunID: row.ID, DefinitionID: row.DefinitionID, Status: p.To, PreviousStatus: p.From,
	})
	if p.Event != nil {
		ev = *p.Event
	}
	return r.events.Record(ctx, tx, ev)
}

func (r *RunRepo) explainMiss(ctx context.Context, tx pgx.Tx, p persistence.RunTransitionParams) error {
	cur, err := r.Get(ctx, tx, p.RunID)
	switch {
	case err != nil:
		return err
	case cur.Status != p.From:
		return apperrors.InvalidState(fmt.Sprintf("run %s is %s, not %s", p.RunID, cur.Status, p.From))
	default:
		return apperrors.Conflict(fmt.Sprintf("run %s changed (version %d, expected %d)", p.RunID, cur.Version, p.Version))
	}
}
