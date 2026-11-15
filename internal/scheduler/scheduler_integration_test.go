//go:build integration

package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/daishakpatel/flowforge/internal/obs"
	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/persistence/pg"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
)

const (
	pending   = model.TaskPending
	queued    = model.TaskQueued
	succeeded = model.TaskSucceeded
	failed    = model.TaskFailed
	skipped   = model.TaskSkipped
)

// wantStatuses fails unless every listed key has the listed status.
func wantStatuses(t *testing.T, e *env, runID string, want map[string]model.TaskStatus) {
	t.Helper()
	got := e.statuses(t, runID)
	for k, s := range want {
		if got[k] != s {
			t.Fatalf("task %s is %s, want %s (all: %v)", k, got[k], s, got)
		}
	}
}

func TestScheduler_DependenciesGateReadiness(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	t.Run("linear", func(t *testing.T) {
		run, id := e.createRun(t, "", tasksJSON("a", "b:a", "c:b"))
		e.tick(t)
		if e.runStatus(t, run) != model.RunRunning {
			t.Fatal("run not RUNNING after the first tick")
		}
		wantStatuses(t, e, run, map[string]model.TaskStatus{"a": queued, "b": pending, "c": pending})
		e.run(t, id["a"], true)
		e.tick(t)
		wantStatuses(t, e, run, map[string]model.TaskStatus{"b": queued, "c": pending})
		e.run(t, id["b"], true)
		e.tick(t)
		e.run(t, id["c"], true)
		e.tick(t)
		if s := e.runStatus(t, run); s != model.RunSucceeded {
			t.Fatalf("run %s, want SUCCEEDED", s)
		}
		if e.events(t, run, "run.started") != 1 || e.events(t, run, "run.succeeded") != 1 {
			t.Error("want exactly one run.started and one run.succeeded")
		}
	})
	t.Run("diamond", func(t *testing.T) {
		run, id := e.createRun(t, "", tasksJSON("a", "b:a", "c:a", "d:b+c"))
		e.tick(t)
		e.run(t, id["a"], true)
		e.tick(t)
		wantStatuses(t, e, run, map[string]model.TaskStatus{"b": queued, "c": queued, "d": pending})
		e.run(t, id["b"], true)
		e.tick(t)
		wantStatuses(t, e, run, map[string]model.TaskStatus{"d": pending}) // c not done yet
		e.run(t, id["c"], true)
		e.tick(t)
		wantStatuses(t, e, run, map[string]model.TaskStatus{"d": queued})
		e.run(t, id["d"], true)
		e.tick(t)
		if s := e.runStatus(t, run); s != model.RunSucceeded {
			t.Fatalf("run %s, want SUCCEEDED", s)
		}
	})
}

func TestScheduler_OnFailureBranchAndRunStatus(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	for name, tc := range map[string]struct {
		extra   string
		specs   []string
		aOK     bool
		want    map[string]model.TaskStatus
		wantRun model.RunStatus
	}{
		"parent succeeds: handler skipped": {
			specs: []string{"a", "fix:a:on_failure", "next:a"}, aOK: true,
			want: map[string]model.TaskStatus{"fix": skipped, "next": succeeded}, wantRun: model.RunSucceeded,
		},
		"parent fails: handler runs and handles it": {
			specs: []string{"a", "fix:a:on_failure", "next:a"}, aOK: false,
			want: map[string]model.TaskStatus{"fix": succeeded, "next": skipped}, wantRun: model.RunSucceeded,
		},
		"unhandled failure fails the run": {
			specs: []string{"a", "next:a"}, aOK: false,
			want: map[string]model.TaskStatus{"a": failed, "next": skipped}, wantRun: model.RunFailed,
		},
		"needs_attention policy": {
			extra: `"on_failure":"needs_attention",`, specs: []string{"a", "next:a"}, aOK: false,
			want: map[string]model.TaskStatus{"next": skipped}, wantRun: model.RunNeedsAttention,
		},
	} {
		t.Run(name, func(t *testing.T) {
			run, id := e.createRun(t, tc.extra, tasksJSON(tc.specs...))
			e.tick(t)
			e.run(t, id["a"], tc.aOK)
			for range 4 { // drive whatever got queued to success until the run settles
				e.tick(t)
				for k, s := range e.statuses(t, run) {
					if s == queued {
						e.run(t, id[k], true)
					}
				}
			}
			wantStatuses(t, e, run, tc.want)
			if s := e.runStatus(t, run); s != tc.wantRun {
				t.Fatalf("run %s, want %s", s, tc.wantRun)
			}
		})
	}
}

func TestScheduler_ConcurrencyLimits(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	active := func(runID string) int {
		return e.count(t, `SELECT count(*) FROM workflow_tasks WHERE run_id = $1 AND status IN ('QUEUED','RUNNING')`, runID)
	}
	t.Run("max_parallel_tasks", func(t *testing.T) {
		run, _ := e.createRun(t, `"concurrency":{"max_parallel_tasks":2},`, tasksJSON("a", "b", "c", "d", "e"))
		for range 10 {
			e.tick(t)
			if n := active(run); n > 2 {
				t.Fatalf("%d tasks QUEUED+RUNNING, limit 2", n)
			}
			for _, v := range e.taskRows(t, run) {
				if v.Status == queued {
					e.run(t, v.ID, true)
					break // finish one per round, so the limit is what bounds the queueing
				}
			}
		}
		if s := e.runStatus(t, run); s != model.RunSucceeded {
			t.Fatalf("run %s, want SUCCEEDED", s)
		}
	})
	t.Run("FF_QUEUE_LIMITS across runs", func(t *testing.T) {
		e.loop.QueueLimits = map[string]int{"slow": 1}
		t.Cleanup(func() { e.loop.QueueLimits = nil })
		spec := `[{"id":"s1","type":"t","queue":"slow"},{"id":"s2","type":"t","queue":"slow"},{"id":"f","type":"t"}]`
		r1, _ := e.createRun(t, "", spec)
		r2, _ := e.createRun(t, "", spec)
		e.tick(t)
		slow := e.count(t, `SELECT count(*) FROM workflow_tasks WHERE run_id IN ($1,$2) AND queue_name='slow' AND status='QUEUED'`, r1, r2)
		fast := e.count(t, `SELECT count(*) FROM workflow_tasks WHERE run_id IN ($1,$2) AND queue_name='default' AND status='QUEUED'`, r1, r2)
		if slow != 1 || fast != 2 {
			t.Fatalf("queued slow=%d (limit 1) default=%d (unlimited, want 2)", slow, fast)
		}
	})
}

// TestScheduler_HeadInstance inserts a superseding row by hand (03.06 will create them): the
// superseded FAILED row no longer counts, and dependents wait for the new head.
func TestScheduler_HeadInstance(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	run, id := e.createRun(t, "", tasksJSON("a", "b:a"))
	e.tick(t)
	e.run(t, id["a"], false)
	if _, err := e.pool.Exec(t.Context(), `
		INSERT INTO workflow_tasks (id, run_id, task_key, task_type, queue_name, status, depends_on, run_if,
		                            config, retry_policy, timeout_seconds, supersedes_task_id)
		SELECT 'task_rerun_a', run_id, task_key, task_type, queue_name, 'PENDING', depends_on, run_if,
		       config, retry_policy, timeout_seconds, id
		FROM workflow_tasks WHERE id = $1`, id["a"]); err != nil {
		t.Fatal(err)
	}
	e.tick(t)
	wantStatuses(t, e, run, map[string]model.TaskStatus{"a": failed, "b": pending}) // original rows
	if s := e.taskStatus(t, "task_rerun_a"); s != queued {
		t.Fatalf("rerun of a is %s, want QUEUED", s)
	}
	if s := e.runStatus(t, run); s != model.RunRunning {
		t.Fatalf("run %s: the superseded failure must not finish it", s)
	}
	e.run(t, "task_rerun_a", true)
	e.tick(t)
	wantStatuses(t, e, run, map[string]model.TaskStatus{"b": queued})
	e.run(t, id["b"], true)
	e.tick(t)
	if s := e.runStatus(t, run); s != model.RunSucceeded {
		t.Fatalf("run %s, want SUCCEEDED (the superseded FAILED row is not a head)", s)
	}
}

func (e *env) taskStatus(t *testing.T, id string) model.TaskStatus {
	t.Helper()
	v, err := e.tasks.Get(t.Context(), e.pool, id)
	if err != nil {
		t.Fatal(err)
	}
	return v.Status
}

// TestScheduler_ConcurrentProcessRun runs processRun twice at once on the same run, round after
// round: the run lock and conditional transitions leave no duplicate event and no error.
func TestScheduler_ConcurrentProcessRun(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	run, _ := e.createRun(t, "", tasksJSON("a", "b:a", "c:a", "d:b+c", "e", "f:e+d"))
	for round := 0; e.runStatus(t, run) == model.RunCreated || e.runStatus(t, run) == model.RunRunning; round++ {
		if round > 20 {
			t.Fatal("run did not finish")
		}
		var wg sync.WaitGroup
		errs := make(chan error, 2)
		for range 2 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := e.loop.runOnce(t.Context(), run, 0); err != nil {
					errs <- err
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatalf("concurrent processRun: %v", err)
		}
		for _, v := range e.taskRows(t, run) {
			if v.Status == queued {
				e.run(t, v.ID, true)
			}
		}
	}
	dups := e.count(t, `SELECT count(*) FROM (SELECT task_id, event_type FROM workflow_events WHERE run_id = $1
		GROUP BY task_id, event_type HAVING count(*) > 1) d`, run)
	if dups != 0 || e.events(t, run, "run.started") != 1 || e.events(t, run, "run.succeeded") != 1 {
		t.Fatalf("duplicate events: %d task duplicates, %d run.started, %d run.succeeded",
			dups, e.events(t, run, "run.started"), e.events(t, run, "run.succeeded"))
	}
}

// staleGate is leader at a fixed epoch.
type staleGate struct{ AlwaysLeader }

func (staleGate) Epoch() int64 { return 3 }

func TestScheduler_StaleEpochCommitsNothing(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	run, _ := e.createRun(t, "", tasksJSON("a", "b:a"))
	if ok, err := (pg.ClusterRepo{}).SetEpochIfGreater(t.Context(), e.pool, 7, "other"); err != nil || !ok {
		t.Fatalf("raise epoch: %v %v", ok, err)
	}
	err := e.loop.Run(t.Context(), staleGate{})
	if !errors.Is(err, persistence.ErrStaleEpoch) {
		t.Fatalf("Run = %v, want ErrStaleEpoch", err)
	}
	if s := e.runStatus(t, run); s != model.RunCreated {
		t.Fatalf("run %s, want CREATED: no decision may commit at a stale epoch", s)
	}
	if n := e.count(t, `SELECT count(*) FROM workflow_events WHERE run_id = $1 AND event_type <> 'run.created'`, run); n != 0 {
		t.Fatalf("%d events recorded at a stale epoch", n)
	}
}

// TestScheduler_EpochShareLock covers the 02.03 follow-up: a new leader raising the fence waits
// for the old leader's open decision transaction, so the old decision commits first or not at all.
func TestScheduler_EpochShareLock(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	tx, err := e.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(t.Context())) }() // after Commit it is a no-op
	if err := (pg.ClusterRepo{}).AssertEpoch(t.Context(), tx, 0); err != nil {
		t.Fatal(err)
	}
	raised := make(chan error, 1)
	go func() {
		_, err := pg.ClusterRepo{}.SetEpochIfGreater(t.Context(), e.pool, 1, "new")
		raised <- err
	}()
	select {
	case err := <-raised:
		t.Fatalf("SetEpochIfGreater returned (%v) while a decision transaction held the fence", err)
	case <-time.After(200 * time.Millisecond): //nolint:forbidigo // proving a wait needs real time
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := <-raised; err != nil {
		t.Fatal(err)
	}
}

func TestScheduler_PausedRunIsLeftAlone(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	run, id := e.createRun(t, "", tasksJSON("a", "b:a"))
	e.tick(t)
	e.run(t, id["a"], true)
	r, err := e.runs.Get(t.Context(), e.pool, run)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.tm.WithTx(t.Context(), pgx.TxOptions{}, func(tx pgx.Tx) error {
		return e.runs.Transition(t.Context(), tx, persistence.RunTransitionParams{RunID: run, From: model.RunRunning, To: model.RunPaused, Version: r.Version})
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.loop.runOnce(t.Context(), run, 0); err != nil {
		t.Fatal(err)
	}
	wantStatuses(t, e, run, map[string]model.TaskStatus{"b": pending}) // 03.05 fills in resume
}

type failingResolver struct{}

func (failingResolver) Resolve(context.Context, persistence.RunView, persistence.TaskView, []persistence.TaskView) (json.RawMessage, error) {
	return nil, errors.New("template references a missing field")
}

func TestScheduler_InputResolutionFailure(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.loop.Inputs = failingResolver{}
	run, _ := e.createRun(t, "", tasksJSON("a", "b:a"))
	e.tick(t)
	e.tick(t)
	wantStatuses(t, e, run, map[string]model.TaskStatus{"a": failed, "b": skipped})
	if s := e.runStatus(t, run); s != model.RunFailed {
		t.Fatalf("run %s, want FAILED", s)
	}
	n := e.count(t, `SELECT count(*) FROM workflow_events WHERE run_id = $1 AND event_type = 'task.failed'
		AND payload->>'error_code' = 'INVALID_INPUT' AND payload->>'previous_status' = 'READY'`, run)
	if n != 1 || e.count(t, `SELECT count(*) FROM task_attempts`) != 0 {
		t.Fatalf("want one task.failed INVALID_INPUT event and no attempt, got %d events", n)
	}
}

func TestScheduler_NotifiesAndCountsMetrics(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	reg := obs.NewRegistry()
	m, err := NewMetrics(reg)
	if err != nil {
		t.Fatal(err)
	}
	e.loop.Metrics = m
	conn, err := pgx.Connect(t.Context(), e.pool.Config().ConnString())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.WithoutCancel(t.Context())) })
	if _, err := conn.Exec(t.Context(), "LISTEN "+persistence.TaskQueuedChannel); err != nil {
		t.Fatal(err)
	}

	run, id := e.createRun(t, "", tasksJSON("a", "b:a:on_failure"))
	e.tick(t)
	wctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if _, err := conn.WaitForNotification(wctx); err != nil {
		t.Fatalf("no NOTIFY %s after queueing: %v", persistence.TaskQueuedChannel, err)
	}
	e.run(t, id["a"], true)
	e.tick(t)
	if s := e.runStatus(t, run); s != model.RunSucceeded {
		t.Fatalf("run %s", s)
	}
	checks := map[string]float64{
		"started":           promtest.ToFloat64(m.WorkflowsStarted),
		"completed":         promtest.ToFloat64(m.WorkflowsCompleted.WithLabelValues("SUCCEEDED")),
		"tasks READY":       promtest.ToFloat64(m.Tasks.WithLabelValues("READY")),
		"tasks QUEUED":      promtest.ToFloat64(m.Tasks.WithLabelValues("QUEUED")),
		"tasks SKIPPED":     promtest.ToFloat64(m.Tasks.WithLabelValues("SKIPPED")),
		"queue depth after": promtest.ToFloat64(m.QueueDepth.WithLabelValues("default")),
	}
	want := map[string]float64{"started": 1, "completed": 1, "tasks READY": 1, "tasks QUEUED": 1, "tasks SKIPPED": 1, "queue depth after": 0}
	if !maps.Equal(checks, want) {
		t.Errorf("metrics %v, want %v", checks, want)
	}
}

// TestScheduler_RoundRobinServesEveryRun shrinks the per-tick cap to 3 with 7 active runs: each
// tick takes the next 3 (wrapping), so all 7 start within 3 ticks and none is starved.
func TestScheduler_RoundRobinServesEveryRun(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.loop.pageSize = 3
	var runs []string
	for range 7 {
		r, _ := e.createRun(t, "", tasksJSON("a"))
		runs = append(runs, r)
	}
	started := func() int {
		return e.count(t, `SELECT count(*) FROM workflow_runs WHERE status = 'RUNNING'`)
	}
	for tick, want := range []int{3, 6, 7} {
		e.tick(t)
		if got := started(); got != want {
			t.Fatalf("after tick %d: %d runs started, want %d", tick+1, got, want)
		}
	}
	for _, r := range runs {
		if s := e.runStatus(t, r); s != model.RunRunning {
			t.Errorf("run %s is %s", r, s)
		}
	}
}
