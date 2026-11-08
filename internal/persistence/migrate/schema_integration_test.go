//go:build integration

package migrate_test

import (
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pressly/goose/v3"

	"github.com/daishakpatel/flowforge/internal/persistence/migrate"
	"github.com/daishakpatel/flowforge/internal/persistence/sqlcgen"
	"github.com/daishakpatel/flowforge/internal/testutil"
	"github.com/daishakpatel/flowforge/migrations"
)

func TestMain(m *testing.M) { testutil.LeakCheck(m) }

// SQLSTATE codes asserted below.
const (
	checkViolation  = "23514"
	uniqueViolation = "23505"
	fkViolation     = "23503"
)

func connect(t *testing.T, dsn string) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(t.Context(), dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(t.Context()) }) // close errors don't affect the test
	return conn
}

func sqlState(err error) string {
	if e, ok := errors.AsType[*pgconn.PgError](err); ok {
		return e.Code
	}
	return ""
}

func TestUp_FromEmptyIsIdempotent(t *testing.T) {
	db := testutil.NewPostgres(t)
	ctx := t.Context()
	for run := 1; run <= 2; run++ {
		if v, err := migrate.Up(ctx, db.DSN, migrate.Flowforge); err != nil || v != 4 {
			t.Fatalf("Up #%d = %d, %v; want 4", run, v, err)
		}
	}
	if v, err := migrate.Version(ctx, db.DSN, migrate.Flowforge); err != nil || v != 4 {
		t.Fatalf("Version = %d, %v; want 4", v, err)
	}
	st, err := migrate.Status(ctx, db.DSN, migrate.Flowforge)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"0001_core.sql", "0002_events_outbox_logs.sql", "0003_workers_security.sql", "0004_triggers_approvals.sql"}
	if len(st) != len(want) {
		t.Fatalf("Status has %d entries, want %d", len(st), len(want))
	}
	for i, m := range st {
		if m.Name != want[i] || m.Version != int64(i+1) || !m.Applied || m.AppliedAt.IsZero() {
			t.Errorf("status[%d] = %+v", i, m)
		}
	}
}

// TestUp_FromEachPreviousVersion: every migration also applies on top of its predecessor (00C §9).
func TestUp_FromEachPreviousVersion(t *testing.T) {
	fsys, err := fs.Sub(migrations.FS, "flowforge")
	if err != nil {
		t.Fatal(err)
	}
	for prev := int64(1); prev <= 3; prev++ {
		t.Run(fmt.Sprintf("from_%d", prev), func(t *testing.T) {
			db := testutil.NewPostgres(t)
			sqlDB, err := sql.Open("pgx", db.DSN)
			if err != nil {
				t.Fatal(err)
			}
			p, err := goose.NewProvider(goose.DialectPostgres, sqlDB, fsys)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := p.UpTo(t.Context(), prev); err != nil {
				t.Fatalf("UpTo(%d): %v", prev, err)
			}
			_ = p.Close() // also closes sqlDB
			st, err := migrate.Status(t.Context(), db.DSN, migrate.Flowforge)
			if err != nil || st[prev].Applied {
				t.Fatalf("migration %d should be pending: %v %+v", prev+1, err, st)
			}
			if v, err := migrate.Up(t.Context(), db.DSN, migrate.Flowforge); err != nil || v != 4 {
				t.Fatalf("Up from %d = %d, %v; want 4", prev, v, err)
			}
		})
	}
}

// TestUp_ConcurrentRunnersSerialised: the advisory lock lets several migrators start at once.
func TestUp_ConcurrentRunnersSerialised(t *testing.T) {
	db := testutil.NewPostgres(t)
	errs := make([]error, 5)
	var wg sync.WaitGroup
	for i := range errs {
		wg.Go(func() { _, errs[i] = migrate.Up(t.Context(), db.DSN, migrate.Flowforge) })
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("runner %d: %v", i, err)
		}
	}
	if v, err := migrate.Version(t.Context(), db.DSN, migrate.Flowforge); err != nil || v != 4 {
		t.Errorf("Version = %d, %v; want 4", v, err)
	}
}

func TestUp_UnreachableDatabase(t *testing.T) {
	const dsn = "postgres://nobody:hunter2@127.0.0.1:1/x?sslmode=disable&connect_timeout=2"
	for _, set := range []migrate.Set{migrate.Flowforge, migrate.Marketdata} {
		_, err := migrate.Up(t.Context(), dsn, set)
		if err == nil {
			t.Fatalf("%s: want a connection error", set)
		}
		if strings.Contains(err.Error(), "hunter2") {
			t.Errorf("%s: error leaks the password: %v", set, err)
		}
	}
}

func TestUp_MarketdataEmptySet(t *testing.T) {
	db := testutil.NewTimescale(t)
	ctx := t.Context()
	if v, err := migrate.Up(ctx, db.DSN, migrate.Marketdata); err != nil || v != 0 {
		t.Errorf("Up = %d, %v; want 0, nil", v, err)
	}
	if st, err := migrate.Status(ctx, db.DSN, migrate.Marketdata); err != nil || len(st) != 0 {
		t.Errorf("Status = %v, %v; want empty", st, err)
	}
	if v, err := migrate.Version(ctx, db.DSN, migrate.Marketdata); err != nil || v != 0 {
		t.Errorf("Version = %d, %v; want 0", v, err)
	}
}

func TestSchema_ObjectsExist(t *testing.T) {
	db := testutil.NewFlowforgeDB(t)
	conn := connect(t, db.DSN)
	ctx := t.Context()

	rows, _ := conn.Query(ctx, `SELECT table_name FROM information_schema.tables
		WHERE table_schema = 'public' AND table_type = 'BASE TABLE' AND table_name <> 'goose_db_version'`)
	tables, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(tables)
	want := []string{"api_keys", "approval_requests", "audit_log", "cluster_state", "cron_triggers", "investigations",
		"outbox", "task_attempts", "task_logs", "worker_observations", "workflow_definitions", "workflow_events",
		"workflow_runs", "workflow_tasks"}
	if !slices.Equal(tables, want) {
		t.Errorf("tables = %v\nwant     %v", tables, want)
	}

	indexes := map[string]string{}
	rows, _ = conn.Query(ctx, `SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = 'public'`)
	var name, def string
	if _, err := pgx.ForEachRow(rows, []any{&name, &def}, func() error { indexes[name] = def; return nil }); err != nil {
		t.Fatal(err)
	}
	for idx, partial := range map[string]bool{
		"uq_runs_idem": true, "ix_runs_status_created": false, "ix_tasks_dispatch": true, "ix_tasks_run": false,
		"uq_tasks_original_key": true, "ix_attempts_running": true, "ix_attempts_worker": true,
		"ix_events_run": false, "ix_outbox_unpublished": true, "ix_logs_attempt": false,
	} {
		def, ok := indexes[idx]
		switch {
		case !ok:
			t.Errorf("index %s missing", idx)
		case strings.Contains(def, " WHERE ") != partial:
			t.Errorf("index %s partial = %v, want %v: %s", idx, !partial, partial, def)
		case strings.HasPrefix(idx, "uq_") && !strings.Contains(def, "UNIQUE"):
			t.Errorf("index %s is not unique: %s", idx, def)
		}
	}

	var seqs int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM pg_sequences WHERE sequencename = 'attempt_fencing_seq'`).Scan(&seqs); err != nil || seqs != 1 {
		t.Errorf("attempt_fencing_seq: count %d, %v", seqs, err)
	}

	rows, _ = conn.Query(ctx, `SELECT column_name FROM information_schema.columns
		WHERE table_name = 'task_attempts' ORDER BY ordinal_position`)
	cols, err := pgx.CollectRows(rows, pgx.RowTo[string])
	wantCols := []string{"id", "task_id", "attempt_number", "worker_id", "status", "fencing_token", "idempotency_key",
		"started_at", "deadline_at", "last_heartbeat_at", "finished_at", "error_code", "error_message", "retryable", "result"}
	if err != nil || !slices.Equal(cols, wantCols) {
		t.Errorf("task_attempts columns = %v (%v)", cols, err)
	}

	// The seeded fence row, read through the generated sqlc code.
	cs, err := sqlcgen.New(conn).GetClusterState(ctx)
	if err != nil || cs.ID != 1 || cs.Epoch != 0 || cs.LeaderID.Valid || !cs.UpdatedAt.Valid {
		t.Errorf("cluster_state = %+v, %v; want id 1, epoch 0, no leader", cs, err)
	}
}

func TestSchema_Constraints(t *testing.T) {
	db := testutil.NewFlowforgeDB(t)
	conn := connect(t, db.DSN)
	for _, stmt := range []string{
		`INSERT INTO workflow_definitions(id, name, version, definition, created_by) VALUES ('wf_1', 'wf', 1, '{}', 't')`,
		`INSERT INTO workflow_runs(id, definition_id, status, created_by, idempotency_key) VALUES ('run_1', 'wf_1', 'CREATED', 't', 'idem-1')`,
		`INSERT INTO workflow_tasks(id, run_id, task_key, task_type, status, retry_policy, timeout_seconds) VALUES ('task_1', 'run_1', 'fetch', 'noop', 'PENDING', '{}', 60)`,
		`INSERT INTO task_attempts(id, task_id, attempt_number, status, idempotency_key, deadline_at) VALUES ('att_1', 'task_1', 1, 'RUNNING', 'task_1:1', now())`,
	} {
		if _, err := conn.Exec(t.Context(), stmt); err != nil {
			t.Fatalf("seed %q: %v", stmt, err)
		}
	}

	// Run in order: the rerun rows depend on earlier ones. want "" means the statement succeeds.
	tests := []struct {
		name, sql, want string
	}{
		{"run invalid status", `INSERT INTO workflow_runs(id, definition_id, status, created_by) VALUES ('run_x', 'wf_1', 'DONE', 't')`, checkViolation},
		{"task invalid status", `INSERT INTO workflow_tasks(id, run_id, task_key, task_type, status, retry_policy, timeout_seconds) VALUES ('task_x', 'run_1', 'x', 'noop', 'WAITING', '{}', 60)`, checkViolation},
		{"attempt invalid status", `INSERT INTO task_attempts(id, task_id, attempt_number, status, idempotency_key, deadline_at) VALUES ('att_x', 'task_1', 9, 'LOST', 'task_1:9', now())`, checkViolation},
		{"duplicate definition name and version", `INSERT INTO workflow_definitions(id, name, version, definition, created_by) VALUES ('wf_2', 'wf', 1, '{}', 't')`, uniqueViolation},
		{"definition version 0", `INSERT INTO workflow_definitions(id, name, version, definition, created_by) VALUES ('wf_3', 'wf', 0, '{}', 't')`, checkViolation},
		{"second cluster_state row", `INSERT INTO cluster_state(id) VALUES (2)`, checkViolation},
		{"cluster_state id 1 again", `INSERT INTO cluster_state(id) VALUES (1)`, uniqueViolation},
		{"duplicate run idempotency key", `INSERT INTO workflow_runs(id, definition_id, status, created_by, idempotency_key) VALUES ('run_2', 'wf_1', 'CREATED', 't', 'idem-1')`, uniqueViolation},
		{"run of unknown definition", `INSERT INTO workflow_runs(id, definition_id, status, created_by) VALUES ('run_3', 'wf_nope', 'CREATED', 't')`, fkViolation},
		{"duplicate attempt number", `INSERT INTO task_attempts(id, task_id, attempt_number, status, idempotency_key, deadline_at) VALUES ('att_2', 'task_1', 1, 'RUNNING', 'task_1:1', now())`, uniqueViolation},
		{"api key invalid role", `INSERT INTO api_keys(id, name, role, secret_hash, salt, created_by) VALUES ('k1', 'k', 'root', '\x00', '\x00', 't')`, checkViolation},
		{"approval invalid action", `INSERT INTO approval_requests(id, run_id, action_type, proposal, risk, expires_at) VALUES ('apr_1', 'run_1', 'delete_run', '{}', 'low', now())`, checkViolation},
		{"cron invalid missed policy", `INSERT INTO cron_triggers(id, name, workflow_name, schedule, missed_policy, created_by) VALUES ('trg_1', 'c', 'wf', '* * * * *', 'catch_up_all', 't')`, checkViolation},
		{"task log invalid level", `INSERT INTO task_logs(attempt_id, task_id, run_id, ts, level, message) VALUES ('att_1', 'task_1', 'run_1', now(), 'TRACE', 'm')`, checkViolation},
		// Partial unique index (task 02.01 §5): two original tasks can't share a key in one run.
		{"duplicate original task key", `INSERT INTO workflow_tasks(id, run_id, task_key, task_type, status, retry_policy, timeout_seconds) VALUES ('task_2', 'run_1', 'fetch', 'noop', 'PENDING', '{}', 60)`, uniqueViolation},
		// A rerun row (ADR-008) may reuse the key, once per superseded task.
		{"rerun reuses the key", `INSERT INTO workflow_tasks(id, run_id, task_key, task_type, status, retry_policy, timeout_seconds, supersedes_task_id) VALUES ('task_3', 'run_1', 'fetch', 'noop', 'PENDING', '{}', 60, 'task_1')`, ""},
		{"second rerun of the same task", `INSERT INTO workflow_tasks(id, run_id, task_key, task_type, status, retry_policy, timeout_seconds, supersedes_task_id) VALUES ('task_4', 'run_1', 'fetch', 'noop', 'PENDING', '{}', 60, 'task_1')`, uniqueViolation},
	}
	for _, tt := range tests {
		_, err := conn.Exec(t.Context(), tt.sql)
		if got := sqlState(err); got != tt.want || (tt.want == "" && err != nil) {
			t.Errorf("%s: SQLSTATE %q (%v), want %q", tt.name, got, err, tt.want)
		}
	}
}

// TestSchema_FencingSequenceMonotonic: 100 concurrent attempt inserts get distinct fencing tokens,
// and later inserts always get larger ones (ADR-003).
func TestSchema_FencingSequenceMonotonic(t *testing.T) {
	db := testutil.NewFlowforgeDB(t)
	ctx := t.Context()
	pool, err := pgxpool.New(ctx, db.DSN)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	for _, stmt := range []string{
		`INSERT INTO workflow_definitions(id, name, version, definition, created_by) VALUES ('wf_1', 'wf', 1, '{}', 't')`,
		`INSERT INTO workflow_runs(id, definition_id, status, created_by) VALUES ('run_1', 'wf_1', 'CREATED', 't')`,
		`INSERT INTO workflow_tasks(id, run_id, task_key, task_type, status, retry_policy, timeout_seconds) VALUES ('task_1', 'run_1', 'fetch', 'noop', 'RUNNING', '{}', 60)`,
	} {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}
	insert := func(n int) (int64, error) {
		var token int64
		err := pool.QueryRow(ctx, `INSERT INTO task_attempts(id, task_id, attempt_number, status, idempotency_key, deadline_at)
			VALUES ($1, 'task_1', $2, 'RUNNING', $3, now()) RETURNING fencing_token`,
			fmt.Sprintf("att_%d", n), n, fmt.Sprintf("task_1:%d", n)).Scan(&token)
		return token, err
	}

	const n = 100
	tokens, errs := make([]int64, n), make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() { tokens[i], errs[i] = insert(i + 1) })
	}
	wg.Wait()
	seen := map[int64]bool{}
	for i := range n {
		if errs[i] != nil {
			t.Fatalf("insert %d: %v", i+1, errs[i])
		}
		if seen[tokens[i]] || tokens[i] <= 0 {
			t.Fatalf("token %d duplicated or not positive", tokens[i])
		}
		seen[tokens[i]] = true
	}
	prev := slices.Max(tokens)
	for k := n + 1; k <= n+3; k++ {
		tok, err := insert(k)
		if err != nil || tok <= prev {
			t.Fatalf("sequential insert %d: token %d (%v), want > %d", k, tok, err, prev)
		}
		prev = tok
	}
}
