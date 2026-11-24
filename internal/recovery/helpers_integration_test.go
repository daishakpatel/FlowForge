//go:build integration

package recovery

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/daishakpatel/flowforge/internal/api/service"
	"github.com/daishakpatel/flowforge/internal/clock"
	workerv1 "github.com/daishakpatel/flowforge/internal/gen/proto/worker/v1"
	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/persistence/pg"
	"github.com/daishakpatel/flowforge/internal/scheduler"
	"github.com/daishakpatel/flowforge/internal/testutil/epochguard"
	"github.com/daishakpatel/flowforge/internal/worker/runtime"
	"github.com/daishakpatel/flowforge/internal/workerapi"
	"github.com/daishakpatel/flowforge/internal/workerapi/workerapitest"
	"github.com/daishakpatel/flowforge/internal/workflow/expr"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
)

const (
	leaseTTL     = 10 * time.Second
	heartbeat    = time.Second
	recoveryTick = 2 * time.Second
	cancelGrace  = 30 * time.Second
)

// env is the real WorkerService (03.01) with the 03.04 RetryOutcomeHandler, plus the three
// scheduler loops, all on one fake clock synced to the database's now(), so deadlines (set by
// the database at claim time) and every clock comparison line up. Tests call each loop's Once.
type env struct {
	srv      *workerapitest.Server
	clock    *clock.Fake
	decide   *scheduler.DecisionLoop
	timeouts *scheduler.TimeoutLoop
	recov    *Loop
	guard    *epochguard.Guard   // the loops' TxManager: every loop transaction asserts the epoch first (04.07)
	runs     *service.RunService // run control (03.05)
	seq      atomic.Int64
}

func newEnv(t *testing.T) *env {
	t.Helper()
	fc := clock.NewFake(time.Date(2026, 11, 15, 0, 0, 0, 0, time.UTC))
	srv := workerapitest.Start(t, workerapitest.Options{
		Clock: fc, LeaseTTL: leaseTTL, HeartbeatInterval: heartbeat,
		Outcomes: func(s *workerapitest.Server) workerapi.OutcomeHandler {
			return workerapi.NewRetryOutcomeHandler(s.Tasks, s.Runs, fc, nil) // jitter 0: exact delays
		},
	})
	srv.Service.Cancels = workerapi.RunCancelSource{DB: srv.Pool, Runs: pg.RunControlRepo{}} // set before any worker calls
	e := &env{srv: srv, clock: fc, runs: &service.RunService{
		DB: srv.Pool, Tx: srv.TM, DefRepo: srv.Defs, RunRepo: srv.Runs, Reruns: srv.Runs, TaskRepo: srv.Tasks, AttemptRepo: srv.Attempts,
		RunControl: pg.RunControlRepo{}, Notify: pg.SchedulerRepo{}, Clock: fc, CancelGrace: cancelGrace,
	}}
	e.guard = epochguard.New(srv.TM)
	t.Cleanup(func() { e.guard.Check(t) })
	e.syncClock(t)
	e.build()
	return e
}

// build makes fresh scheduler loops, as a restarted scheduler process would have.
func (e *env) build() {
	s := e.srv
	ender := workerapi.AttemptEnder{Attempts: s.Attempts, Tasks: s.Tasks, Outcomes: s.Service.Outcomes}
	e.decide = &scheduler.DecisionLoop{
		DB: s.Pool, Tx: e.guard, Cluster: pg.ClusterRepo{}, Sched: pg.SchedulerRepo{}, Notify: pg.SchedulerRepo{},
		Runs: s.Runs, Tasks: s.Tasks, Defs: s.Defs, Inputs: scheduler.TemplateResolver{}, Eval: expr.Evaluator{}, Clock: e.clock, Tick: time.Second,
	}
	e.timeouts = &scheduler.TimeoutLoop{
		DB: s.Pool, Tx: e.guard, Cluster: pg.ClusterRepo{}, Recovery: pg.RecoveryRepo{}, Ender: ender,
		Notify: pg.SchedulerRepo{}, Clock: e.clock, Tick: time.Second,
		Runs: s.Runs, Sched: pg.SchedulerRepo{}, RunControl: pg.RunControlRepo{},
	}
	e.recov = &Loop{
		DB: s.Pool, Tx: e.guard, Cluster: pg.ClusterRepo{}, Recovery: pg.RecoveryRepo{},
		Leases: &workerapi.PGLeaseProvider{DB: s.Pool, Workers: pg.WorkerRepo{}, Clock: e.clock, TTL: leaseTTL},
		Ender:  ender, Notify: pg.SchedulerRepo{}, Events: s.Recorder, Clock: e.clock, Tick: recoveryTick,
		LeaseTTL: leaseTTL, Heartbeat: heartbeat,
	}
}

// syncClock sets the fake clock to the database's now().
func (e *env) syncClock(t *testing.T) {
	t.Helper()
	var now time.Time
	if err := e.srv.Pool.QueryRow(t.Context(), `SELECT now()`).Scan(&now); err != nil {
		t.Fatal(err)
	}
	e.clock.Set(now)
}

// once runs one iteration of a loop as leader at epoch 0.
func once(t *testing.T, l interface {
	Once(context.Context, scheduler.Gate) error
}) {
	t.Helper()
	if err := l.Once(t.Context(), scheduler.AlwaysLeader{}); err != nil {
		t.Fatal(err)
	}
}

// startRun stores a definition with the given tasks and starts a run of it, then syncs the
// clock so its tasks are claimable once queued. It returns the run id and task ids by key.
func (e *env) startRun(t *testing.T, tasks ...string) (string, map[string]string) {
	t.Helper()
	return e.startRunWith(t, "", tasks...)
}

// startRunWith is startRun with extra top-level definition fields, e.g. `"timeout_seconds":5,`.
func (e *env) startRunWith(t *testing.T, extra string, tasks ...string) (string, map[string]string) {
	t.Helper()
	raw := fmt.Sprintf(`{"name":"wf-%d","version":1,%s"tasks":[%s]}`, e.seq.Add(1), extra, strings.Join(tasks, ","))
	def, err := model.Decode([]byte(raw))
	if err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	dv, err := e.srv.Defs.Create(t.Context(), e.srv.Pool, persistence.NewDefinition{Name: def.Name, Version: 1, Definition: []byte(raw), CreatedBy: "t"})
	if err != nil {
		t.Fatal(err)
	}
	var run persistence.RunView
	if err := e.srv.TM.WithTx(t.Context(), pgx.TxOptions{}, func(tx pgx.Tx) error {
		run, err = e.srv.Runs.CreateWithTasks(t.Context(), tx, persistence.NewRun{DefinitionID: dv.ID, CreatedBy: "t"}, def)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	ids := map[string]string{}
	list, err := e.srv.Tasks.ListByRun(t.Context(), e.srv.Pool, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range list {
		ids[v.Key] = v.ID
	}
	e.syncClock(t)
	return run.ID, ids
}

func (e *env) task(t *testing.T, id string) persistence.TaskView {
	t.Helper()
	v, err := e.srv.Tasks.Get(t.Context(), e.srv.Pool, id)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func (e *env) runStatus(t *testing.T, id string) model.RunStatus {
	t.Helper()
	r, err := e.srv.Runs.Get(t.Context(), e.srv.Pool, id)
	if err != nil {
		t.Fatal(err)
	}
	return r.Status
}

// sim is a simulated worker speaking the protocol directly, so a test controls exactly when it
// heartbeats, reports or goes silent (a killed or paused process).
type sim struct {
	id string
	c  workerv1.WorkerServiceClient
}

func (e *env) worker(t *testing.T, id string) *sim {
	t.Helper()
	conn, err := runtime.Dial(e.srv.Addr, e.srv.Token)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() }) // releases resources only
	w := &sim{id: id, c: workerv1.NewWorkerServiceClient(conn)}
	if _, err := w.c.Register(t.Context(), &workerv1.RegisterRequest{
		WorkerId: id, TaskTypes: []string{"t"}, Queues: []string{model.DefaultQueue}, Capacity: 10,
	}); err != nil {
		t.Fatalf("register %s: %v", id, err)
	}
	return w
}

// poll claims what is claimable now. The API's long-poll wait runs on the shared fake clock,
// which nobody advances while a poll waits, so a short real deadline ends an empty poll; the
// API honors the cancellation (03.01) and that counts as nothing to claim.
func (w *sim) poll(t *testing.T) ([]*workerv1.Task, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	resp, err := w.c.PollTask(ctx, &workerv1.PollTaskRequest{WorkerId: w.id, FreeCapacity: 10, WaitMs: 1})
	if status.Code(err) == codes.DeadlineExceeded {
		return nil, nil
	}
	return resp.GetTasks(), err
}

// mustPoll claims exactly want tasks. One poll may come back empty because its short real
// deadline passed while the machine was loaded, so it polls again until it has them or 10 s
// pass; a poll that must find nothing uses poll directly.
func (w *sim) mustPoll(t *testing.T, want int) []*workerv1.Task {
	t.Helper()
	var got []*workerv1.Task
	wall := clock.Real{}
	deadline := wall.Now().Add(10 * time.Second)
	for len(got) < want && wall.Now().Before(deadline) {
		tasks, err := w.poll(t)
		if err != nil {
			t.Fatalf("%s poll: %v", w.id, err)
		}
		got = append(got, tasks...)
	}
	if len(got) != want {
		t.Fatalf("%s polled %d tasks, want %d", w.id, len(got), want)
	}
	return got
}

func (w *sim) beat(t *testing.T, tasks ...*workerv1.Task) *workerv1.HeartbeatResponse {
	t.Helper()
	running := make([]*workerv1.RunningAttempt, len(tasks))
	for i, task := range tasks {
		running[i] = &workerv1.RunningAttempt{AttemptId: task.GetAttemptId(), FencingToken: task.GetFencingToken()}
	}
	resp, err := w.c.Heartbeat(t.Context(), &workerv1.HeartbeatRequest{WorkerId: w.id, Running: running})
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	return resp
}

func (w *sim) complete(t *testing.T, task *workerv1.Task, result string) error {
	t.Helper()
	_, err := w.c.CompleteTask(t.Context(), &workerv1.CompleteTaskRequest{
		AttemptId: task.GetAttemptId(), FencingToken: task.GetFencingToken(), ResultJson: []byte(result),
	})
	return err
}

func (w *sim) fail(t *testing.T, task *workerv1.Task, code string, retryable bool) error {
	t.Helper()
	_, err := w.c.FailTask(t.Context(), &workerv1.FailTaskRequest{
		AttemptId: task.GetAttemptId(), FencingToken: task.GetFencingToken(), ErrorCode: code, Retryable: retryable,
	})
	return err
}

// taskJSON is one task of type t with a retry policy (jitter 0) and a timeout.
func taskJSON(key string, maxAttempts, initialMS, maxMS, timeoutS int) string {
	return fmt.Sprintf(`{"id":%q,"type":"t","timeout_seconds":%d,"retry":{"max_attempts":%d,"strategy":"exponential",`+
		`"initial_delay_ms":%d,"max_delay_ms":%d,"multiplier":2,"jitter":0}}`, key, timeoutS, maxAttempts, initialMS, maxMS)
}

func deregister(id string) *workerv1.DeregisterRequest {
	return &workerv1.DeregisterRequest{WorkerId: id}
}
