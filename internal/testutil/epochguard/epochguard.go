// Package epochguard checks the ADR-003 rule that every leader-loop transaction asserts the
// scheduler epoch first (task 04.07): ClusterRepo.AssertEpoch takes the share lock that makes a
// deposed leader's commit impossible, so a transaction that writes before (or without) it is a
// fencing hole. Tests wrap the TxManager a leader loop uses in a Guard; every transaction it
// opens must start with the LockEpoch statement, and Check fails the test otherwise.
//
// Scope: transactions opened through the guarded TxManager. Writes that bypass a transaction
// are not seen; the only leader loop that has any is retention, which purges expired rows,
// makes no decision, and is exempt by design (03.07).
package epochguard

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/daishakpatel/flowforge/internal/persistence"
)

// lockEpoch starts the statement ClusterRepo.AssertEpoch runs (sqlc's LockEpoch query).
const lockEpoch = "-- name: LockEpoch "

// Guard is a persistence.TxManager that records every transaction whose first statement is not
// the epoch lock. Safe for concurrent use.
type Guard struct {
	inner persistence.TxManager

	mu         sync.Mutex
	txs        int
	violations []string
}

var _ persistence.TxManager = (*Guard)(nil)

// New guards inner.
func New(inner persistence.TxManager) *Guard { return &Guard{inner: inner} }

// WithTx implements persistence.TxManager.
func (g *Guard) WithTx(ctx context.Context, opts pgx.TxOptions, fn func(tx pgx.Tx) error) error {
	return g.inner.WithTx(ctx, opts, func(tx pgx.Tx) error {
		return fn(&guardedTx{Tx: tx, g: g})
	})
}

// Transactions is how many guarded transactions ran at least one statement.
func (g *Guard) Transactions() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.txs
}

// Violations describes each transaction that did not start with the epoch lock.
func (g *Guard) Violations() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.violations...)
}

// Check fails t for each violation.
func (g *Guard) Check(t testing.TB) {
	t.Helper()
	for _, v := range g.Violations() {
		t.Errorf("epoch guard: a leader-loop transaction did not assert the epoch first: %s", v)
	}
}

// guardedTx sees every statement of one transaction; only the first one is judged.
type guardedTx struct {
	pgx.Tx
	g       *Guard
	checked bool
}

func (t *guardedTx) first(sql string) {
	if t.checked {
		return
	}
	t.checked = true
	t.g.mu.Lock()
	defer t.g.mu.Unlock()
	t.g.txs++
	if !strings.HasPrefix(sql, lockEpoch) {
		t.g.violations = append(t.g.violations, fmt.Sprintf("first statement %q from %s", statementName(sql), caller()))
	}
}

func (t *guardedTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	t.first(sql)
	return t.Tx.Exec(ctx, sql, args...)
}

func (t *guardedTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	t.first(sql)
	return t.Tx.Query(ctx, sql, args...)
}

func (t *guardedTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	t.first(sql)
	return t.Tx.QueryRow(ctx, sql, args...)
}

func (t *guardedTx) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	sql := "BATCH"
	if len(b.QueuedQueries) > 0 {
		sql = b.QueuedQueries[0].SQL
	}
	t.first(sql)
	return t.Tx.SendBatch(ctx, b)
}

func (t *guardedTx) CopyFrom(ctx context.Context, table pgx.Identifier, cols []string, src pgx.CopyFromSource) (int64, error) {
	t.first("COPY " + table.Sanitize())
	return t.Tx.CopyFrom(ctx, table, cols, src)
}

func (t *guardedTx) Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error) {
	t.first(sql)
	return t.Tx.Prepare(ctx, name, sql)
}

func (t *guardedTx) Begin(ctx context.Context) (pgx.Tx, error) {
	t.first("SAVEPOINT")
	return t.Tx.Begin(ctx)
}

// statementName is sqlc's "-- name: X" header, or the statement's start.
func statementName(sql string) string {
	line, _, _ := strings.Cut(strings.TrimSpace(sql), "\n")
	if name, ok := strings.CutPrefix(line, "-- name: "); ok {
		return name
	}
	if len(line) > 60 {
		line = line[:60]
	}
	return line
}

// caller names the two innermost FlowForge frames outside this package and generated code.
func caller() string {
	pcs := make([]uintptr, 32)
	frames := runtime.CallersFrames(pcs[:runtime.Callers(3, pcs)])
	var out []string
	for f, more := frames.Next(); more && len(out) < 2; f, more = frames.Next() {
		if strings.Contains(f.Function, "flowforge/internal") && !strings.Contains(f.Function, "epochguard") &&
			!strings.Contains(f.Function, "sqlcgen") {
			out = append(out, f.Function)
		}
	}
	return strings.Join(out, " ← ")
}
