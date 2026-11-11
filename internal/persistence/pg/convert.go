package pg

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/daishakpatel/flowforge/internal/apperrors"
	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/persistence/sqlcgen"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
	"github.com/daishakpatel/flowforge/internal/workflow/statemachine"
)

// maxRows caps the unpaginated per-run and per-task lists (00C §9: every SELECT has a LIMIT).
// A run has at most 500 definition tasks (00E §3.5) plus rerun rows.
const maxRows = 10_000

// notFoundOr maps pgx.ErrNoRows to NOT_FOUND and wraps any other error.
func notFoundOr(err error, what string) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return apperrors.Wrap(apperrors.CodeNotFound, what+" not found", err)
	}
	return fmt.Errorf("get %s: %w", what, err)
}

// conflictOr maps a unique violation to CONFLICT with message and wraps any other error.
func conflictOr(err error, message, op string) error {
	if sqlState(err) == codeUniqueViolation {
		return apperrors.Wrap(apperrors.CodeConflict, message, err)
	}
	return fmt.Errorf("%s: %w", op, err)
}

// int32Of narrows n for an integer column, rejecting values out of range.
func int32Of(field string, n int) (int32, error) {
	if n < math.MinInt32 || n > math.MaxInt32 {
		return 0, apperrors.InvalidArgument(field + " is out of range")
	}
	return int32(n), nil
}

// limitOf converts a positive row limit for a LIMIT parameter.
func limitOf(n int) int32 { return int32(min(n, maxRows)) } //nolint:gosec // G115: bounded by maxRows

func text(s string) pgtype.Text { return pgtype.Text{String: s, Valid: s != ""} }

func timestamptz(t time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: t, Valid: !t.IsZero()}
}

func timePtr(t pgtype.Timestamptz) *time.Time {
	if !t.Valid {
		return nil
	}
	return &t.Time
}

func definitionView(r sqlcgen.WorkflowDefinition) persistence.DefinitionView {
	return persistence.DefinitionView{
		ID: r.ID, Name: r.Name, Version: int(r.Version), Definition: r.Definition,
		CreatedBy: r.CreatedBy, CreatedAt: r.CreatedAt.Time,
	}
}

func runView(r sqlcgen.WorkflowRun) persistence.RunView {
	v := persistence.RunView{
		ID: r.ID, DefinitionID: r.DefinitionID, Status: model.RunStatus(r.Status), Input: r.Input,
		IdempotencyKey: r.IdempotencyKey.String, TriggerID: r.TriggerID.String,
		ParentRunID: r.ParentRunID.String, DeadlineAt: timePtr(r.DeadlineAt),
		CancelRequested: r.CancelRequested, CreatedBy: r.CreatedBy, CreatedAt: r.CreatedAt.Time,
		StartedAt: timePtr(r.StartedAt), CompletedAt: timePtr(r.CompletedAt), Version: r.Version,
	}
	if r.LeaderEpoch.Valid {
		v.LeaderEpoch = &r.LeaderEpoch.Int64
	}
	return v
}

func taskView(r sqlcgen.WorkflowTask) (persistence.TaskView, error) {
	var policy model.RetryPolicy
	if err := json.Unmarshal(r.RetryPolicy, &policy); err != nil {
		return persistence.TaskView{}, fmt.Errorf("decode retry_policy of task %s: %w", r.ID, err)
	}
	return persistence.TaskView{
		ID: r.ID, RunID: r.RunID, Key: r.TaskKey, Type: r.TaskType, Queue: r.QueueName,
		Status: model.TaskStatus(r.Status), DependsOn: r.DependsOn, RunIf: model.RunIf(r.RunIf),
		RunIfExpr: r.RunIfExpr.String, Config: r.Config, Input: r.Input, Result: r.Result,
		RetryPolicy: policy, TimeoutSeconds: int(r.TimeoutSeconds), AttemptCount: int(r.AttemptCount),
		AvailableAt: r.AvailableAt.Time, SupersedesTaskID: r.SupersedesTaskID.String,
		CreatedAt: r.CreatedAt.Time, UpdatedAt: r.UpdatedAt.Time, Version: r.Version,
	}, nil
}

func taskViews(rows []sqlcgen.WorkflowTask) ([]persistence.TaskView, error) {
	if len(rows) >= maxRows {
		return nil, apperrors.Internal(fmt.Errorf("task list reached the %d-row cap", maxRows))
	}
	out := make([]persistence.TaskView, len(rows))
	for i, r := range rows {
		v, err := taskView(r)
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
}

func attemptView(r sqlcgen.TaskAttempt) persistence.AttemptView {
	v := persistence.AttemptView{
		ID: r.ID, TaskID: r.TaskID, AttemptNumber: int(r.AttemptNumber), WorkerID: r.WorkerID.String,
		Status: model.AttemptStatus(r.Status), FencingToken: r.FencingToken,
		IdempotencyKey: r.IdempotencyKey, StartedAt: r.StartedAt.Time, DeadlineAt: r.DeadlineAt.Time,
		LastHeartbeatAt: timePtr(r.LastHeartbeatAt), FinishedAt: timePtr(r.FinishedAt),
		ErrorCode: r.ErrorCode.String, ErrorMessage: r.ErrorMessage.String, Result: r.Result,
	}
	if r.Retryable.Valid {
		v.Retryable = &r.Retryable.Bool
	}
	return v
}

func eventView(r sqlcgen.WorkflowEvent) persistence.EventView {
	return persistence.EventView{
		ID: r.ID, EventID: r.EventID, RunID: r.RunID, TaskID: r.TaskID.String,
		Type: statemachine.EventType(r.EventType), Version: int(r.EventVersion), Payload: r.Payload,
		CorrelationID: r.CorrelationID, CreatedAt: r.CreatedAt.Time,
	}
}

// cursorArgs decodes a page cursor into keyset arguments (NULLs for the first page).
func cursorArgs(cursor string) (pgtype.Timestamptz, pgtype.Text, error) {
	if cursor == "" {
		return pgtype.Timestamptz{}, pgtype.Text{}, nil
	}
	at, id, err := persistence.DecodeCursor(cursor)
	if err != nil {
		return pgtype.Timestamptz{}, pgtype.Text{}, err
	}
	return timestamptz(at), text(id), nil
}

// page trims a limit+1 result to limit rows and returns the next cursor ("" on the last page).
func page[T any](rows []T, limit int, key func(T) (time.Time, string)) ([]T, string) {
	if len(rows) <= limit {
		return rows, ""
	}
	rows = rows[:limit]
	return rows, persistence.EncodeCursor(key(rows[limit-1]))
}
