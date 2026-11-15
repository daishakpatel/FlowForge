//go:build integration

package scheduler

import (
	"context"
	crand "crypto/rand"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
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
	"github.com/daishakpatel/flowforge/internal/testutil"
	"github.com/daishakpatel/flowforge/internal/workflow/expr"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
)

// env is one migrated database, the repositories, and a DecisionLoop wired as main wires it.
type env struct {
	pool     *pgxpool.Pool
	tm       *pg.TxManager
	defs     *pg.DefinitionRepo
	runs     *pg.RunRepo
	tasks    *pg.TaskRepo
	attempts *pg.AttemptRepo
	loop     *DecisionLoop
	clock    *clock.Fake
	seq      atomic.Int64
}

func newEnv(t *testing.T) *env {
	t.Helper()
	db := testutil.NewFlowforgeDB(t)
	pool, err := pg.NewPool(t.Context(), config.Secret(db.DSN))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	fc := clock.NewFake(time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC))
	gen := ids.NewGenerator(clock.Real{}, crand.Reader)
	rec := events.NewRecorder(fc, gen, "flowforge-scheduler")
	e := &env{
		pool: pool, tm: pg.NewTxManager(pool, clock.Real{}, nil), clock: fc,
		defs: pg.NewDefinitionRepo(gen), runs: pg.NewRunRepo(gen, rec), tasks: pg.NewTaskRepo(rec),
		attempts: pg.NewAttemptRepo(gen, rec),
	}
	e.loop = &DecisionLoop{
		DB: pool, Tx: e.tm, Cluster: pg.ClusterRepo{}, Sched: pg.SchedulerRepo{}, Notify: pg.SchedulerRepo{},
		Runs: e.runs, Tasks: e.tasks, Defs: e.defs, Inputs: TemplateResolver{}, Eval: expr.Evaluator{}, Clock: fc, Tick: time.Second,
	}
	return e
}

// createRun stores a definition (tasks is the JSON array of task objects; extra holds more
// top-level fields, e.g. `"on_failure":"needs_attention",`) and starts one run of it. It returns
// the run id and task ids by key.
func (e *env) createRun(t *testing.T, extra, tasks string) (string, map[string]string) {
	t.Helper()
	return e.createRunInput(t, extra, tasks, nil)
}

// createRunInput is createRun with a run input (03.06).
func (e *env) createRunInput(t *testing.T, extra, tasks string, input json.RawMessage) (string, map[string]string) {
	t.Helper()
	raw := fmt.Sprintf(`{"name":"wf-%d","version":1,%s"tasks":%s}`, e.seq.Add(1), extra, tasks)
	def, err := model.Decode([]byte(raw))
	if err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	dv, err := e.defs.Create(t.Context(), e.pool, persistence.NewDefinition{
		Name: def.Name, Version: 1, Definition: []byte(raw), CreatedBy: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	var run persistence.RunView
	if err := e.tm.WithTx(t.Context(), pgx.TxOptions{}, func(tx pgx.Tx) error {
		run, err = e.runs.CreateWithTasks(t.Context(), tx, persistence.NewRun{DefinitionID: dv.ID, Input: input, CreatedBy: "test"}, def)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	byKey := map[string]string{}
	for _, v := range e.taskRows(t, run.ID) {
		byKey[v.Key] = v.ID
	}
	return run.ID, byKey
}

func (e *env) taskRows(t *testing.T, runID string) []persistence.TaskView {
	t.Helper()
	list, err := e.tasks.ListByRun(t.Context(), e.pool, runID)
	if err != nil {
		t.Fatal(err)
	}
	return list
}

// statuses returns the status of each task key's original row.
func (e *env) statuses(t *testing.T, runID string) map[string]model.TaskStatus {
	t.Helper()
	out := map[string]model.TaskStatus{}
	for _, v := range e.taskRows(t, runID) {
		if v.SupersedesTaskID == "" {
			out[v.Key] = v.Status
		}
	}
	return out
}

func (e *env) runStatus(t *testing.T, runID string) model.RunStatus {
	t.Helper()
	r, err := e.runs.Get(t.Context(), e.pool, runID)
	if err != nil {
		t.Fatal(err)
	}
	return r.Status
}

// tick runs one scheduler tick at epoch 0 and fails the test on a stale epoch.
func (e *env) tick(t *testing.T) {
	t.Helper()
	if err := e.loop.Once(t.Context(), AlwaysLeader{}); err != nil {
		t.Fatal(err)
	}
}

// run is the simulated executor's two steps for one task: claim (attempt + QUEUED → RUNNING),
// then finish (attempt and task → SUCCEEDED, or FAILED when ok is false), as flowforge-api
// would through PollTask and CompleteTask/FailTask with the terminal outcome handler.
func (e *env) run(t *testing.T, taskID string, ok bool) {
	t.Helper()
	if err := e.claim(t.Context(), taskID); err != nil {
		t.Fatal(err)
	}
	if err := e.finish(t.Context(), taskID, ok); err != nil {
		t.Fatal(err)
	}
}

func (e *env) claim(ctx context.Context, taskID string) error {
	return e.tm.WithTx(ctx, pgx.TxOptions{}, func(tx pgx.Tx) error {
		task, err := e.tasks.Get(ctx, tx, taskID)
		if err != nil {
			return err
		}
		if _, err := e.attempts.Create(ctx, tx, persistence.NewAttempt{
			TaskID: taskID, RunID: task.RunID, AttemptNumber: task.AttemptCount + 1, WorkerID: "sim", TimeoutSeconds: 60,
		}); err != nil {
			return err
		}
		return e.tasks.Transition(ctx, tx, persistence.TaskTransitionParams{
			TaskID: taskID, From: model.TaskQueued, To: model.TaskRunning,
			Mutate: func(m *persistence.TaskMutation) { m.IncrementAttempt = true },
		})
	})
}

func (e *env) finish(ctx context.Context, taskID string, ok bool) error {
	return e.finishResult(ctx, taskID, ok, nil)
}

// finishResult is finish recording result on the task, as CompleteTask does (03.06).
func (e *env) finishResult(ctx context.Context, taskID string, ok bool, result json.RawMessage) error {
	attempt, task := model.AttemptSucceeded, model.TaskSucceeded
	if !ok {
		attempt, task = model.AttemptFailed, model.TaskFailed
	}
	return e.tm.WithTx(ctx, pgx.TxOptions{}, func(tx pgx.Tx) error {
		list, err := e.attempts.ListByTask(ctx, tx, taskID)
		if err != nil {
			return err
		}
		a := list[len(list)-1]
		if err := e.attempts.Transition(ctx, tx, persistence.AttemptTransitionParams{
			AttemptID: a.ID, From: model.AttemptRunning, To: attempt, FencingToken: a.FencingToken,
		}); err != nil {
			return err
		}
		return e.tasks.Transition(ctx, tx, persistence.TaskTransitionParams{
			TaskID: taskID, From: model.TaskRunning, To: task, Mutate: func(m *persistence.TaskMutation) { m.Result = result },
		})
	})
}

// count returns one integer from a test-only query.
func (e *env) count(t *testing.T, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := e.pool.QueryRow(t.Context(), sql, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return n
}

// events counts a run's events of one type (task_id "" counts every task).
func (e *env) events(t *testing.T, runID, eventType string) int {
	t.Helper()
	return e.count(t, `SELECT count(*) FROM workflow_events WHERE run_id = $1 AND event_type = $2`, runID, eventType)
}

// tasksJSON builds a task array from "key:dep1+dep2:run_if" specs (deps and run_if optional).
func tasksJSON(specs ...string) string {
	parts := make([]string, len(specs))
	for i, s := range specs {
		f := append(strings.Split(s, ":"), "", "")
		deps := "[]"
		if f[1] != "" {
			deps = `["` + strings.ReplaceAll(f[1], "+", `","`) + `"]`
		}
		runIf := ""
		if f[2] != "" {
			runIf = fmt.Sprintf(`,"run_if":%q`, f[2])
		}
		parts[i] = fmt.Sprintf(`{"id":%q,"type":"t","depends_on":%s%s}`, f[0], deps, runIf)
	}
	return "[" + strings.Join(parts, ",") + "]"
}
