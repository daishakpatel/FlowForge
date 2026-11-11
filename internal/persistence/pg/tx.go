package pg

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/persistence"
)

// Postgres SQLSTATEs the layer reacts to.
const (
	codeSerializationFailure = "40001"
	codeDeadlockDetected     = "40P01"
	codeUniqueViolation      = "23505"
)

// MaxTxRetries is how many times WithTx reruns a transaction after a serialization failure or
// deadlock before returning the error (so at most MaxTxRetries+1 attempts).
const MaxTxRetries = 3

const txRetryBase = 10 * time.Millisecond

// beginner starts transactions; *pgxpool.Pool implements it, and tests use a fake.
type beginner interface {
	BeginTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error)
}

// TxManager implements persistence.TxManager over a pool.
type TxManager struct {
	db    beginner
	clock clock.Clock

	mu  sync.Mutex // guards rng, which is not safe for concurrent use
	rng *rand.Rand
}

var _ persistence.TxManager = (*TxManager)(nil)

// NewTxManager returns a TxManager over db (normally a *pgxpool.Pool). Retry waits use c and are
// jittered with rng; a nil rng means no jitter.
func NewTxManager(db beginner, c clock.Clock, rng *rand.Rand) *TxManager {
	return &TxManager{db: db, clock: c, rng: rng}
}

// WithTx runs fn in a transaction with opts (the zero value is READ COMMITTED, the layer's
// default; correctness comes from conditional UPDATEs, not isolation). It commits when fn
// returns nil and rolls back on an error or a panic, re-raising the panic. On a serialization
// failure (40001) or deadlock (40P01), from fn or from commit, it waits a jittered exponential
// backoff and reruns fn, up to MaxTxRetries times. A cancelled ctx stops the retries.
func (m *TxManager) WithTx(ctx context.Context, opts pgx.TxOptions, fn func(tx pgx.Tx) error) error {
	for retry := 0; ; retry++ {
		err := m.once(ctx, opts, fn)
		if err == nil || !retryable(err) {
			return err
		}
		if retry == MaxTxRetries {
			return fmt.Errorf("transaction gave up after %d attempts: %w", retry+1, err)
		}
		if serr := m.clock.Sleep(ctx, m.backoff(retry)); serr != nil {
			return errors.Join(err, serr)
		}
	}
}

func (m *TxManager) once(ctx context.Context, opts pgx.TxOptions, fn func(tx pgx.Tx) error) error {
	tx, err := m.db.BeginTx(ctx, opts)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	// Roll back with a context that outlives ctx's cancellation, so the connection is cleaned
	// up even when ctx is what made fn fail.
	rbCtx := context.WithoutCancel(ctx)
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback(rbCtx) // the panic is what matters; a rollback error adds nothing
			panic(p)
		}
	}()
	if err := fn(tx); err != nil {
		if rbErr := tx.Rollback(rbCtx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			return errors.Join(err, fmt.Errorf("rollback: %w", rbErr))
		}
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

// backoff returns txRetryBase·2^retry, jittered to [50%, 150%) when rng is set.
func (m *TxManager) backoff(retry int) time.Duration {
	d := txRetryBase << retry
	if m.rng == nil {
		return d
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return d/2 + time.Duration(m.rng.Int64N(int64(d)))
}

func retryable(err error) bool {
	code := sqlState(err)
	return code == codeSerializationFailure || code == codeDeadlockDetected
}

// sqlState returns the SQLSTATE of the first *pgconn.PgError in err's chain, or "".
func sqlState(err error) string {
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		return pgErr.Code
	}
	return ""
}
