package coordination

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daishakpatel/flowforge/internal/events"
	"github.com/daishakpatel/flowforge/internal/persistence"
)

// PGFence is the Postgres layer of the two-layer fence (task 04.07, FR-RAFT-08, ADR-003,
// 00_MASTER §3.5): it implements EpochFence over cluster_state. Activate raises the stored epoch
// to the new leader's (SetEpochIfGreater, which waits for every transaction still holding
// AssertEpoch's share lock) and records scheduler.leader_changed in the same transaction. After
// it commits, every transaction a deposed leader starts fails AssertEpoch with STALE_EPOCH, so
// the Elector announces the term only after Activate returns nil.
//
// The wait for that lock is bounded by LockTimeout: a deposed leader frozen inside a transaction
// keeps it until its session ends, so Activate fails instead (the Elector gives the term up and
// campaigns again) rather than hang past its own lease. The scheduler's sessions set
// idle_in_transaction_session_timeout, so Postgres ends such a frozen session.
type PGFence struct {
	Tx          persistence.TxManager
	Cluster     persistence.ClusterRepo
	Events      *events.Recorder
	LockTimeout time.Duration // default 2s; keep it well below the leader lease TTL
}

var _ EpochFence = PGFence{}

// defaultFenceLockTimeout is PGFence's default LockTimeout.
const defaultFenceLockTimeout = 2 * time.Second

// Activate implements EpochFence. Activating the epoch Postgres already holds for the same node
// succeeds (an activation retried after its commit); any other epoch at or below the stored one
// is an error: a newer leader exists, and this term must not start.
func (f PGFence) Activate(ctx context.Context, epoch int64, nodeID string) error {
	wait := f.LockTimeout
	if wait <= 0 {
		wait = defaultFenceLockTimeout
	}
	return f.Tx.WithTx(ctx, pgx.TxOptions{}, func(tx pgx.Tx) error {
		// SET takes no bind parameters; the value is an integer this code formats.
		if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL lock_timeout = '%dms'", wait.Milliseconds())); err != nil {
			return fmt.Errorf("activate epoch %d: set lock timeout: %w", epoch, err)
		}
		set, err := f.Cluster.SetEpochIfGreater(ctx, tx, epoch, nodeID)
		if err != nil {
			return fmt.Errorf("activate epoch %d: %w", epoch, err)
		}
		if !set {
			stored, leader, err := f.Cluster.GetEpoch(ctx, tx)
			if err != nil {
				return fmt.Errorf("activate epoch %d: read the stored epoch: %w", epoch, err)
			}
			if stored == epoch && leader == nodeID {
				return nil
			}
			return fmt.Errorf("activate epoch %d: postgres holds epoch %d of %q", epoch, stored, leader)
		}
		return f.Events.Record(ctx, tx, events.NewLeaderChanged(events.LeaderChangedPayload{NodeID: nodeID, Epoch: epoch}))
	})
}
