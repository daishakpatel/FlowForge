//go:build integration

package pg

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/config"
	"github.com/daishakpatel/flowforge/internal/events"
	"github.com/daishakpatel/flowforge/internal/ids"
	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/testutil"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
)

// env is one migrated database with the repositories wired as a service would wire them.
type env struct {
	pool     *pgxpool.Pool
	tm       *TxManager
	defs     *DefinitionRepo
	runs     *RunRepo
	tasks    *TaskRepo
	attempts *AttemptRepo
	events   EventRepo
	cluster  ClusterRepo
}

func newEnv(t *testing.T) *env {
	t.Helper()
	db := testutil.NewFlowforgeDB(t)
	pool, err := NewPool(t.Context(), config.Secret(db.DSN))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	g := &ids.Fake{}
	rec := events.NewRecorder(clock.NewFake(time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)), g, "flowforge-test")
	return &env{
		pool: pool, tm: NewTxManager(pool, clock.Real{}, nil),
		defs: NewDefinitionRepo(g), runs: NewRunRepo(g, rec), tasks: NewTaskRepo(rec),
		attempts: NewAttemptRepo(g, rec),
	}
}

// threeTasks is a → b → c, with c also depending on a; listed out of order to exercise TopoOrder.
const threeTasks = `{"name":%q,"version":%d,"timeout_seconds":600,"tasks":[
 {"id":"c","type":"t","depends_on":["a","b"],"run_if":"always"},
 {"id":"a","type":"t","queue":"q1","config":{"k":1}},
 {"id":"b","type":"t","depends_on":["a"]}]}`

// createDefinition stores a three-task definition and returns it decoded.
func (e *env) createDefinition(t *testing.T, name string, version int) (persistence.DefinitionView, *model.Definition) {
	t.Helper()
	raw := fmt.Sprintf(threeTasks, name, version)
	def, err := model.Decode([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	v, err := e.defs.Create(t.Context(), e.pool, persistence.NewDefinition{
		Name: name, Version: version, Definition: []byte(raw), CreatedBy: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	return v, def
}

// createRun creates a run of a fresh definition and returns it with its tasks by key.
func (e *env) createRun(t *testing.T) (persistence.RunView, map[string]persistence.TaskView) {
	t.Helper()
	dv, def := e.createDefinition(t, "wf-"+strings.ToLower(t.Name()), 1)
	var run persistence.RunView
	err := e.tm.WithTx(t.Context(), pgx.TxOptions{}, func(tx pgx.Tx) error {
		var err error
		run, err = e.runs.CreateWithTasks(t.Context(), tx, persistence.NewRun{DefinitionID: dv.ID, CreatedBy: "test"}, def)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return run, e.tasksByKey(t, run.ID)
}

func (e *env) tasksByKey(t *testing.T, runID string) map[string]persistence.TaskView {
	t.Helper()
	list, err := e.tasks.ListByRun(t.Context(), e.pool, runID)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]persistence.TaskView{}
	for _, v := range list {
		out[v.Key] = v
	}
	return out
}

// moveTask runs one task transition in its own transaction.
func (e *env) moveTask(t *testing.T, id string, from, to model.TaskStatus, mut func(*persistence.TaskMutation)) {
	t.Helper()
	err := e.tm.WithTx(t.Context(), pgx.TxOptions{}, func(tx pgx.Tx) error {
		return e.tasks.Transition(t.Context(), tx, persistence.TaskTransitionParams{TaskID: id, From: from, To: to, Mutate: mut})
	})
	if err != nil {
		t.Fatalf("task %s %s → %s: %v", id, from, to, err)
	}
}

// count returns a single integer from a test-only query.
func (e *env) count(t *testing.T, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := e.pool.QueryRow(t.Context(), sql, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return n
}

var errInjected = errors.New("injected failure")

// spyTx counts every statement and fails Exec of SQL containing failOn, to prove that a later
// failure rolls back everything earlier in the transaction.
type spyTx struct {
	pgx.Tx
	failOn string
	calls  int
}

func (s *spyTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	s.calls++
	if s.failOn != "" && strings.Contains(sql, s.failOn) {
		return pgconn.CommandTag{}, errInjected
	}
	return s.Tx.Exec(ctx, sql, args...)
}

func (s *spyTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	s.calls++
	return s.Tx.Query(ctx, sql, args...)
}

func (s *spyTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	s.calls++
	return s.Tx.QueryRow(ctx, sql, args...)
}

func (s *spyTx) CopyFrom(ctx context.Context, table pgx.Identifier, cols []string, src pgx.CopyFromSource) (int64, error) {
	s.calls++
	return s.Tx.CopyFrom(ctx, table, cols, src)
}
