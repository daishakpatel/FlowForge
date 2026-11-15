//go:build integration

package main

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/config"
	"github.com/daishakpatel/flowforge/internal/events"
	"github.com/daishakpatel/flowforge/internal/ids"
	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/persistence/pg"
	"github.com/daishakpatel/flowforge/internal/testutil"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
)

func TestMain(m *testing.M) { testutil.LeakCheck(m) }

// syncBuffer collects the binary's log output from its goroutines.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}
func (s *syncBuffer) String() string { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

func envOpts(env map[string]string) config.Options {
	return config.Options{LookupEnv: func(k string) (string, bool) { v, ok := env[k]; return v, ok }}
}

func TestRun_RefusesToStartWhenSchemaIsBehind(t *testing.T) {
	db := testutil.NewPostgres(t)
	err := run(t.Context(), envOpts(map[string]string{"FF_PG_DSN": db.DSN}), io.Discard,
		func(context.Context, string, string) (net.Listener, error) {
			t.Error("listened before checking the schema")
			return nil, nil
		})
	if err == nil || !strings.Contains(err.Error(), "flowforge-migrate up") {
		t.Fatalf("run = %v, want a schema error", err)
	}
}

// TestRun_CompletesDAGRuns starts the real binary's wiring with AlwaysLeader and drives a
// diamond run to SUCCEEDED with a simulated executor that claims and completes QUEUED tasks.
func TestRun_CompletesDAGRuns(t *testing.T) {
	db := testutil.NewFlowforgeDB(t)
	pool, err := pg.NewPool(t.Context(), config.Secret(db.DSN))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	gen := ids.NewGenerator(clock.Real{}, crand.Reader)
	rec := events.NewRecorder(clock.Real{}, gen, "test")
	tm, runs, tasks, attempts := pg.NewTxManager(pool, clock.Real{}, nil), pg.NewRunRepo(gen, rec), pg.NewTaskRepo(rec), pg.NewAttemptRepo(gen, rec)
	runID := startRun(t, pool, tm, runs, gen,
		`{"name":"diamond","version":1,"tasks":[{"id":"a","type":"t"},{"id":"b","type":"t","depends_on":["a"]},
		  {"id":"c","type":"t","depends_on":["a"]},{"id":"d","type":"t","depends_on":["b","c"]}]}`)

	addr := make(chan string, 1)
	listen := func(ctx context.Context, network, _ string) (net.Listener, error) {
		ln, err := (&net.ListenConfig{}).Listen(ctx, network, "127.0.0.1:0")
		if err == nil {
			addr <- ln.Addr().String()
		}
		return ln, err
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var logs syncBuffer
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, envOpts(map[string]string{"FF_PG_DSN": db.DSN, "FF_SCHED_TICK": "50ms", "FF_LOG_LEVEL": "debug"}), &logs, listen)
	}()
	base := "http://" + <-addr

	finished := testutil.Eventually(func() bool {
		executeQueued(t, tm, tasks, attempts)
		r, err := runs.Get(t.Context(), pool, runID)
		return err == nil && r.Status == model.RunSucceeded
	}, 30*time.Second, 50*time.Millisecond)
	if !finished {
		t.Fatalf("run did not reach SUCCEEDED; logs:\n%s", logs.String())
	}
	if code, _ := get(t, base+"/readyz"); code != http.StatusOK {
		t.Errorf("/readyz = %d", code)
	}
	if _, body := get(t, base+"/metrics"); !strings.Contains(body, `flowforge_workflows_completed_total{status="SUCCEEDED"} 1`) ||
		!strings.Contains(body, "flowforge_workflows_started_total 1") {
		t.Errorf("/metrics lacks the run's counters:\n%s", body)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("run = %v, want nil on shutdown; logs:\n%s", err, logs.String())
	}
}

func startRun(t *testing.T, pool persistence.Querier, tm *pg.TxManager, runs *pg.RunRepo, gen ids.Generator, raw string) string {
	t.Helper()
	def, err := model.Decode([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	dv, err := pg.NewDefinitionRepo(gen).Create(t.Context(), pool, persistence.NewDefinition{Name: def.Name, Version: 1, Definition: []byte(raw), CreatedBy: "test"})
	if err != nil {
		t.Fatal(err)
	}
	var run persistence.RunView
	if err := tm.WithTx(t.Context(), pgx.TxOptions{}, func(tx pgx.Tx) error {
		run, err = runs.CreateWithTasks(t.Context(), tx, persistence.NewRun{DefinitionID: dv.ID, CreatedBy: "test"}, def)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return run.ID
}

// executeQueued claims every QUEUED task and completes it successfully, as a worker would
// through PollTask and CompleteTask.
func executeQueued(t *testing.T, tm *pg.TxManager, tasks *pg.TaskRepo, attempts *pg.AttemptRepo) {
	t.Helper()
	ctx := t.Context()
	err := tm.WithTx(ctx, pgx.TxOptions{}, func(tx pgx.Tx) error {
		list, err := tasks.ClaimQueued(ctx, tx, persistence.ClaimFilter{
			Types: []string{"t"}, Queues: []string{model.DefaultQueue}, Now: time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC), Limit: 100,
		})
		if err != nil {
			return err
		}
		for _, task := range list {
			a, err := attempts.Create(ctx, tx, persistence.NewAttempt{TaskID: task.ID, RunID: task.RunID, AttemptNumber: 1, WorkerID: "sim", TimeoutSeconds: 60})
			if err != nil {
				return err
			}
			if err := tasks.Transition(ctx, tx, persistence.TaskTransitionParams{
				TaskID: task.ID, From: model.TaskQueued, To: model.TaskRunning,
				Mutate: func(m *persistence.TaskMutation) { m.IncrementAttempt = true },
			}); err != nil {
				return err
			}
			if err := attempts.Transition(ctx, tx, persistence.AttemptTransitionParams{
				AttemptID: a.ID, From: model.AttemptRunning, To: model.AttemptSucceeded, FencingToken: a.FencingToken,
			}); err != nil {
				return err
			}
			if err := tasks.Transition(ctx, tx, persistence.TaskTransitionParams{TaskID: task.ID, From: model.TaskRunning, To: model.TaskSucceeded}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(body)
}
