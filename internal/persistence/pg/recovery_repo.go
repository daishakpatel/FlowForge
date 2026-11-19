package pg

import (
	"context"
	"fmt"
	"time"

	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/persistence/sqlcgen"
)

// RecoveryRepo implements persistence.RecoveryRepo (task 03.04).
type RecoveryRepo struct{}

var _ persistence.RecoveryRepo = RecoveryRepo{}

// OverdueAttempts implements persistence.RecoveryRepo.
func (RecoveryRepo) OverdueAttempts(ctx context.Context, q persistence.Querier, now time.Time, limit int) ([]persistence.AttemptView, error) {
	rows, err := sqlcgen.New(q).ListOverdueAttempts(ctx, sqlcgen.ListOverdueAttemptsParams{Now: timestamptz(now), Lim: limitOf(limit)})
	if err != nil {
		return nil, fmt.Errorf("list overdue attempts: %w", err)
	}
	return attemptViews(rows), nil
}

// RunningAttemptsOf implements persistence.RecoveryRepo.
func (RecoveryRepo) RunningAttemptsOf(ctx context.Context, q persistence.Querier, workerID string, limit int) ([]persistence.AttemptView, error) {
	rows, err := sqlcgen.New(q).ListRunningAttemptsOfWorker(ctx, sqlcgen.ListRunningAttemptsOfWorkerParams{WorkerID: text(workerID), Lim: limitOf(limit)})
	if err != nil {
		return nil, fmt.Errorf("list running attempts of worker %s: %w", workerID, err)
	}
	return attemptViews(rows), nil
}

// OrphanedAttempts implements persistence.RecoveryRepo.
func (RecoveryRepo) OrphanedAttempts(ctx context.Context, q persistence.Querier, before time.Time, limit int) ([]persistence.AttemptView, error) {
	rows, err := sqlcgen.New(q).ListOrphanedAttempts(ctx, sqlcgen.ListOrphanedAttemptsParams{Before: timestamptz(before), Lim: limitOf(limit)})
	if err != nil {
		return nil, fmt.Errorf("list orphaned attempts: %w", err)
	}
	return attemptViews(rows), nil
}

// MarkWorkerLost implements persistence.RecoveryRepo.
func (RecoveryRepo) MarkWorkerLost(ctx context.Context, q persistence.Querier, workerID string, before time.Time) (bool, error) {
	n, err := sqlcgen.New(q).MarkWorkerLost(ctx, sqlcgen.MarkWorkerLostParams{WorkerID: workerID, Before: timestamptz(before)})
	if err != nil {
		return false, fmt.Errorf("mark worker %s lost: %w", workerID, err)
	}
	return n == 1, nil
}

func attemptViews(rows []sqlcgen.TaskAttempt) []persistence.AttemptView {
	out := make([]persistence.AttemptView, len(rows))
	for i, r := range rows {
		out[i] = attemptView(r)
	}
	return out
}
