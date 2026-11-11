//go:build integration

package pg

import (
	"testing"

	"github.com/jackc/pgx/v5"
)

// TestTxManager_RetriesRealSerializationFailure provokes a genuine 40001: inside a REPEATABLE READ
// transaction, read the fence row, let another connection update it, then update it ourselves.
// WithTx must roll back and rerun fn, which succeeds the second time.
func TestTxManager_RetriesRealSerializationFailure(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()
	calls := 0
	err := e.tm.WithTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead}, func(tx pgx.Tx) error {
		calls++
		if _, _, err := e.cluster.GetEpoch(ctx, tx); err != nil { // takes the snapshot
			return err
		}
		if calls == 1 {
			if _, err := e.pool.Exec(ctx, `UPDATE cluster_state SET leader_id = 'other' WHERE id = 1`); err != nil {
				return err
			}
		}
		_, err := e.cluster.SetEpochIfGreater(ctx, tx, int64(calls), "me")
		return err
	})
	if err != nil {
		t.Fatalf("WithTx = %v, want success after a retry", err)
	}
	if calls != 2 {
		t.Errorf("fn ran %d times, want 2 (one serialization failure, one retry)", calls)
	}
	if epoch, leader, _ := e.cluster.GetEpoch(ctx, e.pool); epoch != 2 || leader != "me" {
		t.Errorf("fence = %d, %q; want the retried write (2, me)", epoch, leader)
	}
}
