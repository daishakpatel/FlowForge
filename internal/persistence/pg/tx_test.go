package pg

import (
	"context"
	"errors"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/daishakpatel/flowforge/internal/clock"
)

// fakeTx records Commit and Rollback; every other pgx.Tx method panics (nil embedded interface).
type fakeTx struct {
	pgx.Tx
	commitErr             error
	committed, rolledBack bool
}

func (t *fakeTx) Commit(context.Context) error   { t.committed = true; return t.commitErr }
func (t *fakeTx) Rollback(context.Context) error { t.rolledBack = true; return nil }

// fakeDB hands out a fresh fakeTx per BeginTx, with commitErrs[i] as the i-th commit's result.
type fakeDB struct {
	txs        []*fakeTx
	commitErrs []error
}

func (d *fakeDB) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	tx := &fakeTx{}
	if i := len(d.txs); i < len(d.commitErrs) {
		tx.commitErr = d.commitErrs[i]
	}
	d.txs = append(d.txs, tx)
	return tx, nil
}

// instantClock is a fake clock whose Sleep advances time at once and records the delay.
type instantClock struct {
	*clock.Fake
	slept []time.Duration
}

func (c *instantClock) Sleep(ctx context.Context, d time.Duration) error {
	c.slept = append(c.slept, d)
	c.Advance(d)
	return ctx.Err()
}

func newTestManager(db *fakeDB) (*TxManager, *instantClock) {
	c := &instantClock{Fake: clock.NewFake(time.Unix(0, 0))}
	return NewTxManager(db, c, rand.New(rand.NewPCG(1, 2))), c //nolint:gosec // G404: deterministic test jitter by design
}

func pgErr(code string) error { return &pgconn.PgError{Code: code, Message: "injected"} }

func TestTxManager_RetriesSerializationFailureThenSurfacesError(t *testing.T) {
	t.Parallel()
	db := &fakeDB{}
	m, c := newTestManager(db)
	calls := 0
	err := m.WithTx(t.Context(), pgx.TxOptions{}, func(pgx.Tx) error {
		calls++
		return pgErr(codeSerializationFailure)
	})
	if calls != MaxTxRetries+1 {
		t.Errorf("fn ran %d times, want %d (1 + %d retries)", calls, MaxTxRetries+1, MaxTxRetries)
	}
	if sqlState(err) != codeSerializationFailure {
		t.Errorf("err = %v, want the 40001 error surfaced", err)
	}
	if len(c.slept) != MaxTxRetries {
		t.Errorf("slept %d times, want %d", len(c.slept), MaxTxRetries)
	}
	for i, d := range c.slept {
		base := txRetryBase << i
		if d < base/2 || d >= base*3/2 {
			t.Errorf("backoff %d = %v, want in [%v, %v)", i, d, base/2, base*3/2)
		}
	}
	for i, tx := range db.txs {
		if !tx.rolledBack || tx.committed {
			t.Errorf("tx %d: rolledBack=%v committed=%v, want rolled back only", i, tx.rolledBack, tx.committed)
		}
	}
}

func TestTxManager_RetriesDeadlockAndCommitFailureUntilSuccess(t *testing.T) {
	t.Parallel()
	db := &fakeDB{commitErrs: []error{nil, pgErr(codeSerializationFailure), nil}}
	m, _ := newTestManager(db)
	calls := 0
	err := m.WithTx(t.Context(), pgx.TxOptions{}, func(pgx.Tx) error {
		calls++
		if calls == 1 {
			return pgErr(codeDeadlockDetected)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WithTx = %v, want success on the third attempt", err)
	}
	if calls != 3 || !db.txs[2].committed {
		t.Errorf("calls = %d, third committed = %v; want 3, true", calls, db.txs[2].committed)
	}
}

func TestTxManager_DoesNotRetryOtherErrors(t *testing.T) {
	t.Parallel()
	db := &fakeDB{}
	m, _ := newTestManager(db)
	boom := errors.New("boom")
	calls := 0
	err := m.WithTx(t.Context(), pgx.TxOptions{}, func(pgx.Tx) error { calls++; return boom })
	if !errors.Is(err, boom) || calls != 1 || !db.txs[0].rolledBack {
		t.Errorf("err=%v calls=%d rolledBack=%v; want boom, 1, true", err, calls, db.txs[0].rolledBack)
	}
	err = m.WithTx(t.Context(), pgx.TxOptions{}, func(pgx.Tx) error { return pgErr(codeUniqueViolation) })
	if sqlState(err) != codeUniqueViolation || len(db.txs) != 2 {
		t.Errorf("unique violation: err=%v txs=%d; want it returned without retry", err, len(db.txs))
	}
}

func TestTxManager_RollsBackOnPanic(t *testing.T) {
	t.Parallel()
	db := &fakeDB{}
	m, _ := newTestManager(db)
	defer func() {
		if p := recover(); p != "kaboom" {
			t.Errorf("recovered %v, want the original panic re-raised", p)
		}
		if tx := db.txs[0]; !tx.rolledBack || tx.committed {
			t.Errorf("rolledBack=%v committed=%v, want rolled back only", tx.rolledBack, tx.committed)
		}
	}()
	_ = m.WithTx(t.Context(), pgx.TxOptions{}, func(pgx.Tx) error { panic("kaboom") })
	t.Error("WithTx returned; want the panic to propagate")
}

func TestTxManager_CancelledContextStopsRetries(t *testing.T) {
	t.Parallel()
	db := &fakeDB{}
	m, _ := newTestManager(db)
	ctx, cancel := context.WithCancel(t.Context())
	calls := 0
	err := m.WithTx(ctx, pgx.TxOptions{}, func(pgx.Tx) error {
		calls++
		cancel()
		return pgErr(codeSerializationFailure)
	})
	if calls != 1 || !errors.Is(err, context.Canceled) {
		t.Errorf("calls=%d err=%v; want 1 call and context.Canceled", calls, err)
	}
}
