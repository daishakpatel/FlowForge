//go:build integration

package retention

import (
	crand "crypto/rand"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/config"
	"github.com/daishakpatel/flowforge/internal/events"
	"github.com/daishakpatel/flowforge/internal/ids"
	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/persistence/pg"
	"github.com/daishakpatel/flowforge/internal/scheduler"
	"github.com/daishakpatel/flowforge/internal/testutil"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
)

type seeded struct {
	pool           *pgxpool.Pool
	oldRun, newRun string
}

// seed makes two runs with idempotency keys, created 25 h and 1 h ago, then ages rows by SQL:
// the old run's events 31 days old; outbox rows 4 days old, published or not; task log lines
// 15 days and 1 hour old.
func seed(t *testing.T) seeded {
	t.Helper()
	db := testutil.NewFlowforgeDB(t)
	pool, err := pg.NewPool(t.Context(), config.Secret(db.DSN))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	gen := ids.NewGenerator(clock.Real{}, crand.Reader)
	rec := events.NewRecorder(clock.Real{}, gen, "test")
	tm := pg.NewTxManager(pool, clock.Real{}, nil)
	raw := []byte(`{"name":"wf","version":1,"tasks":[{"id":"a","type":"t"}]}`)
	def, err := model.Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	dv, err := pg.NewDefinitionRepo(gen).Create(t.Context(), pool, persistence.NewDefinition{Name: "wf", Version: 1, Definition: raw, CreatedBy: "t"})
	if err != nil {
		t.Fatal(err)
	}
	s := seeded{pool: pool}
	for _, key := range []string{"old-key", "new-key"} {
		var run persistence.RunView
		if err := tm.WithTx(t.Context(), pgx.TxOptions{}, func(tx pgx.Tx) error {
			run, err = pg.NewRunRepo(gen, rec).CreateWithTasks(t.Context(), tx, persistence.NewRun{DefinitionID: dv.ID, IdempotencyKey: key, CreatedBy: "t"}, def)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		if key == "old-key" {
			s.oldRun = run.ID
		} else {
			s.newRun = run.ID
		}
	}
	var task string
	exec(t, pool, `SELECT id FROM workflow_tasks WHERE run_id = $1`, &task, s.oldRun)
	var attempt persistence.AttemptView
	if err := tm.WithTx(t.Context(), pgx.TxOptions{}, func(tx pgx.Tx) error {
		attempt, err = pg.NewAttemptRepo(gen, rec).Create(t.Context(), tx, persistence.NewAttempt{TaskID: task, RunID: s.oldRun, AttemptNumber: 1, WorkerID: "w", TimeoutSeconds: 60})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{
		`UPDATE workflow_runs SET created_at = now() - interval '25 hours' WHERE id = '` + s.oldRun + `'`,
		`UPDATE workflow_runs SET created_at = now() - interval '1 hour' WHERE id = '` + s.newRun + `'`,
		`UPDATE workflow_events SET created_at = now() - interval '31 days' WHERE run_id = '` + s.oldRun + `'`,
		// Old run: outbox rows 4 days old, published. New run: 4 days old but unpublished.
		`UPDATE outbox SET created_at = now() - interval '4 days', published_at = now() WHERE key = '` + s.oldRun + `'`,
		`UPDATE outbox SET created_at = now() - interval '4 days' WHERE key = '` + s.newRun + `'`,
		`INSERT INTO task_logs (attempt_id, task_id, run_id, ts, level, message)
		 SELECT '` + attempt.ID + `', '` + task + `', '` + s.oldRun + `', now() - interval '15 days', 'INFO', 'old ' || g FROM generate_series(1, 7) g`,
		`INSERT INTO task_logs (attempt_id, task_id, run_id, ts, level, message)
		 VALUES ('` + attempt.ID + `', '` + task + `', '` + s.oldRun + `', now() - interval '1 hour', 'INFO', 'recent')`,
	} {
		if _, err := pool.Exec(t.Context(), sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	return s
}

func exec(t *testing.T, pool *pgxpool.Pool, sql string, dest any, args ...any) {
	t.Helper()
	if err := pool.QueryRow(t.Context(), sql, args...).Scan(dest); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

func count(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) int {
	t.Helper()
	var n int
	exec(t, pool, sql, &n, args...)
	return n
}

// TestLoopPG_DeletesOldRowsInBatchesKeepsRecent is task 03.07 §8's retention test: seeded old
// rows go, in batches of 2; recent rows and unpublished outbox rows stay; the idempotency key of
// the 25-hour-old run is cleared and the 1-hour-old run's is kept.
func TestLoopPG_DeletesOldRowsInBatchesKeepsRecent(t *testing.T) {
	t.Parallel()
	s := seed(t)
	oldEvents := count(t, s.pool, `SELECT count(*) FROM workflow_events WHERE run_id = $1`, s.oldRun)
	newEvents := count(t, s.pool, `SELECT count(*) FROM workflow_events WHERE run_id = $1`, s.newRun)
	if oldEvents < 2 || newEvents < 1 { // 2 is one full batch; the 7 log lines need four
		t.Fatalf("seeded %d old and %d new events; want at least 2 and 1", oldEvents, newEvents)
	}
	l := &Loop{DB: s.pool, Repo: pg.RetentionRepo{}, Clock: clock.Real{}, Policy: DefaultPolicy(), Batch: 2, Pause: time.Millisecond}
	if err := l.Once(t.Context(), scheduler.AlwaysLeader{}); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		what, sql string
		want      int
	}{
		{"old run's events", `SELECT count(*) FROM workflow_events WHERE run_id = '` + s.oldRun + `'`, 0},
		{"new run's events", `SELECT count(*) FROM workflow_events WHERE run_id = '` + s.newRun + `'`, newEvents},
		{"old published outbox rows", `SELECT count(*) FROM outbox WHERE key = '` + s.oldRun + `'`, 0},
		{"old unpublished outbox rows (never deleted)", `SELECT count(*) FROM outbox WHERE key = '` + s.newRun + `' AND published_at IS NULL`, newEvents},
		{"15-day-old log lines", `SELECT count(*) FROM task_logs WHERE message LIKE 'old %'`, 0},
		{"recent log line", `SELECT count(*) FROM task_logs WHERE message = 'recent'`, 1},
		{"25 h old run's key", `SELECT count(*) FROM workflow_runs WHERE idempotency_key = 'old-key'`, 0},
		{"1 h old run's key", `SELECT count(*) FROM workflow_runs WHERE idempotency_key = 'new-key'`, 1},
	} {
		if got := count(t, s.pool, c.sql); got != c.want {
			t.Errorf("%s: %d; want %d", c.what, got, c.want)
		}
	}
	if err := l.Once(t.Context(), scheduler.AlwaysLeader{}); err != nil { // idempotent: nothing left to do
		t.Fatal(err)
	}
}
