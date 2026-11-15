package pg

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daishakpatel/flowforge/internal/apperrors"
	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/persistence/sqlcgen"
)

// RunControlRepo implements persistence.RunControlRepo (task 03.05).
type RunControlRepo struct{}

var _ persistence.RunControlRepo = RunControlRepo{}

// RequestCancel implements persistence.RunControlRepo.
func (RunControlRepo) RequestCancel(ctx context.Context, q persistence.Querier, runID string, forceAt time.Time) (persistence.RunView, error) {
	row, err := sqlcgen.New(q).RequestRunCancel(ctx, sqlcgen.RequestRunCancelParams{ID: runID, ForceAt: timestamptz(forceAt)})
	if errors.Is(err, pgx.ErrNoRows) {
		cur, gerr := sqlcgen.New(q).GetRun(ctx, runID)
		if gerr != nil {
			return persistence.RunView{}, notFoundOr(gerr, "run")
		}
		return runView(cur), apperrors.InvalidState(fmt.Sprintf("run %s is %s and cannot be cancelled", runID, cur.Status))
	}
	if err != nil {
		return persistence.RunView{}, fmt.Errorf("request cancel of run %s: %w", runID, err)
	}
	return runView(row), nil
}

// CancelRequestedAttempts implements persistence.RunControlRepo.
func (RunControlRepo) CancelRequestedAttempts(ctx context.Context, q persistence.Querier, attemptIDs []string) ([]string, error) {
	if len(attemptIDs) == 0 {
		return nil, nil
	}
	ids, err := sqlcgen.New(q).ListCancelRequestedAttempts(ctx, attemptIDs)
	if err != nil {
		return nil, fmt.Errorf("list cancel-requested attempts: %w", err)
	}
	return ids, nil
}

// OverdueRuns implements persistence.RunControlRepo.
func (RunControlRepo) OverdueRuns(ctx context.Context, q persistence.Querier, now time.Time, limit int) ([]string, error) {
	ids, err := sqlcgen.New(q).ListOverdueRuns(ctx, sqlcgen.ListOverdueRunsParams{Now: timestamptz(now), Lim: limitOf(limit)})
	if err != nil {
		return nil, fmt.Errorf("list overdue runs: %w", err)
	}
	return ids, nil
}
