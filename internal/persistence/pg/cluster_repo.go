package pg

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/daishakpatel/flowforge/internal/apperrors"
	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/persistence/sqlcgen"
)

// ClusterRepo implements persistence.ClusterRepo over the cluster_state fence row.
type ClusterRepo struct{}

var _ persistence.ClusterRepo = ClusterRepo{}

// GetEpoch implements persistence.ClusterRepo.
func (ClusterRepo) GetEpoch(ctx context.Context, q persistence.Querier) (int64, string, error) {
	row, err := sqlcgen.New(q).GetClusterState(ctx)
	if err != nil {
		return 0, "", fmt.Errorf("get cluster state: %w", err)
	}
	return row.Epoch, row.LeaderID.String, nil
}

// SetEpochIfGreater implements persistence.ClusterRepo. A newly elected leader calls it once
// before scheduling (master spec §3.5).
func (ClusterRepo) SetEpochIfGreater(ctx context.Context, q persistence.Querier, epoch int64, leaderID string) (bool, error) {
	n, err := sqlcgen.New(q).SetEpochIfGreater(ctx, sqlcgen.SetEpochIfGreaterParams{Epoch: epoch, LeaderID: text(leaderID)})
	if err != nil {
		return false, fmt.Errorf("set epoch %d: %w", epoch, err)
	}
	return n == 1, nil
}

// AssertEpoch implements persistence.ClusterRepo. Call it first in every scheduler
// transaction: the share lock it takes is held until tx ends, so a new leader's
// SetEpochIfGreater waits for this transaction and a deposed leader cannot commit (ADR-003).
func (ClusterRepo) AssertEpoch(ctx context.Context, tx pgx.Tx, epoch int64) error {
	stored, err := sqlcgen.New(tx).LockEpoch(ctx)
	if err != nil {
		return fmt.Errorf("lock epoch: %w", err)
	}
	if stored != epoch {
		return &apperrors.Error{
			Code:    apperrors.CodeNotLeader,
			Message: fmt.Sprintf("scheduler epoch %d is stale (current %d)", epoch, stored),
			Cause:   persistence.ErrStaleEpoch,
		}
	}
	return nil
}
