//go:build integration

package recovery

import (
	"context"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/config"
	"github.com/daishakpatel/flowforge/internal/obs"
	"github.com/daishakpatel/flowforge/internal/persistence/pg"
	"github.com/daishakpatel/flowforge/internal/scheduler"
	"github.com/daishakpatel/flowforge/internal/testutil"
	"github.com/daishakpatel/flowforge/internal/workerapi"
	"github.com/daishakpatel/flowforge/internal/workerapi/workerapitest"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
)

// TestRetry_BackoffOnTheFakeClock (polls happen only when a task is claimable: the API's
// long-poll wait runs on the shared fake clock): each failure parks the task in RETRY_WAIT until exactly
// now + delay (jitter 0), then the scheduler requeues it and a worker reclaims it; delays run
// 1s, 2s, 4s, then stay at the 4s cap; the last allowed failure fails the task and the run.
func TestRetry_BackoffOnTheFakeClock(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	w := e.worker(t, "w-retry")
	run, ids := e.startRun(t, taskJSON("a", 5, 1000, 4000, 60))
	once(t, e.decide)
	for attempt, delay := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 4 * time.Second} {
		task := w.mustPoll(t, 1)[0]
		if int(task.GetAttemptNumber()) != attempt+1 {
			t.Fatalf("claimed attempt %d, want %d", task.GetAttemptNumber(), attempt+1)
		}
		failedAt := e.clock.Now()
		if err := w.fail(t, task, "UPSTREAM", true); err != nil {
			t.Fatal(err)
		}
		v := e.task(t, ids["a"])
		if v.Status != model.TaskRetryWait || !v.AvailableAt.Equal(failedAt.Add(delay)) {
			t.Fatalf("after attempt %d: %s available_at %v, want RETRY_WAIT at now+%s", attempt+1, v.Status, v.AvailableAt, delay)
		}
		e.clock.Advance(delay - time.Millisecond)
		once(t, e.decide)
		if e.task(t, ids["a"]).Status != model.TaskRetryWait {
			t.Fatalf("attempt %d: requeued before its delay passed", attempt+1)
		}
		e.clock.Advance(time.Millisecond)
		once(t, e.decide)
		if s := e.task(t, ids["a"]).Status; s != model.TaskQueued {
			t.Fatalf("attempt %d: %s after the delay, want QUEUED", attempt+1, s)
		}
	}
	if err := w.fail(t, w.mustPoll(t, 1)[0], "UPSTREAM", true); err != nil {
		t.Fatal(err)
	}
	once(t, e.decide)
	if s, r := e.task(t, ids["a"]).Status, e.runStatus(t, run); s != model.TaskFailed || r != model.RunFailed {
		t.Fatalf("after max_attempts: task %s run %s, want FAILED/FAILED", s, r)
	}
	testutil.AssertInvariants(t, e.srv.Pool)
}

// TestTimeout_ServerDecidesAndLateReportIsStale: an attempt still RUNNING at its deadline is
// TIMED_OUT on the first TimeoutLoop pass after it (not before), the task retries, the worker's
// late completion is rejected, and the last attempt timing out ends the task TIMED_OUT.
func TestTimeout_ServerDecidesAndLateReportIsStale(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	w := e.worker(t, "w-slow")
	run, ids := e.startRun(t, taskJSON("a", 2, 1000, 1000, 1))
	once(t, e.decide)
	first := w.mustPoll(t, 1)[0]
	e.syncClock(t) // the deadline is the database's claim time + 1 s
	e.clock.Advance(900 * time.Millisecond)
	once(t, e.timeouts)
	if a, _ := e.srv.Attempt(t, ids["a"]); a.Status != model.AttemptRunning {
		t.Fatalf("timed out before the deadline: %s", a.Status)
	}
	e.clock.Advance(200 * time.Millisecond) // past the deadline, one tick later
	once(t, e.timeouts)
	a, _ := e.srv.Attempt(t, ids["a"])
	if a.Status != model.AttemptTimedOut || a.ErrorCode != scheduler.CodeTimeout || e.task(t, ids["a"]).Status != model.TaskRetryWait {
		t.Fatalf("after the deadline: attempt %s %s, task %s", a.Status, a.ErrorCode, e.task(t, ids["a"]).Status)
	}
	if err := w.complete(t, first, `{"late":true}`); reasonOf(err) != "STALE_ATTEMPT" {
		t.Fatalf("late completion = %v, want STALE_ATTEMPT", err)
	}
	e.clock.Advance(time.Second)
	once(t, e.decide)
	w.mustPoll(t, 1)
	e.syncClock(t)
	e.clock.Advance(1100 * time.Millisecond)
	once(t, e.timeouts)
	once(t, e.decide)
	if s, r := e.task(t, ids["a"]).Status, e.runStatus(t, run); s != model.TaskTimedOut || r != model.RunFailed {
		t.Fatalf("last attempt timed out: task %s run %s, want TIMED_OUT/FAILED", s, r)
	}
	testutil.AssertInvariants(t, e.srv.Pool)
}

func reasonOf(err error) string {
	if err == nil {
		return ""
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.FailedPrecondition {
		return st.Code().String()
	}
	if strings.Contains(st.Message(), "stale") || len(st.Details()) > 0 {
		return "STALE_ATTEMPT"
	}
	return st.Message()
}

// TestWorkerLoss_RecoveredAndZombieRejected: a worker stops heartbeating mid-task (killed or
// paused). Within lease TTL + 2 recovery ticks its attempt is ABANDONED and the worker LOST
// (worker.lost); the task is retried and a second worker completes it; when the first wakes up
// and reports with its old token it gets STALE_ATTEMPT, and the result is the second's.
func TestWorkerLoss_RecoveredAndZombieRejected(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	zombie := e.worker(t, "w-zombie")
	run, ids := e.startRun(t, taskJSON("a", 3, 1000, 1000, 600))
	once(t, e.decide)
	old := zombie.mustPoll(t, 1)[0]
	zombie.beat(t, old)
	lastSeen := e.clock.Now()
	for steps := 0; ; steps++ { // the zombie is silent from here on
		if a, _ := e.srv.Attempt(t, ids["a"]); a.Status == model.AttemptAbandoned {
			break
		}
		if steps > 20 {
			t.Fatal("the lost worker's attempt was never abandoned")
		}
		e.clock.Advance(recoveryTick)
		once(t, e.recov)
	}
	took := e.clock.Now().Sub(lastSeen)
	t.Logf("worker loss recovered %s after the last heartbeat (lease TTL %s, recovery tick %s)", took, leaseTTL, recoveryTick)
	if took > leaseTTL+2*recoveryTick {
		t.Fatalf("recovered after %s, want ≤ lease TTL + 2 ticks (%s)", took, leaseTTL+2*recoveryTick)
	}
	a, _ := e.srv.Attempt(t, ids["a"])
	if a.ErrorCode != workerapi.CodeWorkerLost || e.task(t, ids["a"]).Status != model.TaskRetryWait {
		t.Fatalf("abandoned attempt code %q, task %s", a.ErrorCode, e.task(t, ids["a"]).Status)
	}
	if s := e.srv.Scalar(t, `SELECT status FROM worker_observations WHERE worker_id = 'w-zombie'`); s != "LOST" {
		t.Errorf("worker status %s, want LOST", s)
	}
	if n := e.srv.Scalar(t, `SELECT count(*) FROM outbox WHERE envelope->>'event_type' = 'worker.lost' AND envelope->'payload'->>'worker_id' = 'w-zombie'`); n != "1" {
		t.Errorf("worker.lost events = %s, want 1", n)
	}

	e.clock.Advance(time.Second) // the retry delay
	once(t, e.decide)
	rescuer := e.worker(t, "w-rescuer")
	retry := rescuer.mustPoll(t, 1)[0]
	if retry.GetFencingToken() <= old.GetFencingToken() || retry.GetAttemptNumber() != 2 {
		t.Fatalf("retry token %d attempt %d; want a newer token and attempt 2", retry.GetFencingToken(), retry.GetAttemptNumber())
	}
	if err := rescuer.complete(t, retry, `{"by":"rescuer"}`); err != nil {
		t.Fatal(err)
	}
	if err := zombie.complete(t, old, `{"by":"zombie"}`); reasonOf(err) != "STALE_ATTEMPT" {
		t.Fatalf("zombie completion = %v, want STALE_ATTEMPT", err)
	}
	once(t, e.decide)
	if r := string(e.task(t, ids["a"]).Result); r != `{"by": "rescuer"}` || e.runStatus(t, run) != model.RunSucceeded {
		t.Fatalf("task result %s, run %s; want the rescuer's result and SUCCEEDED", r, e.runStatus(t, run))
	}
	if n := e.srv.Scalar(t, `SELECT count(*) FROM task_attempts WHERE task_id = $1 AND status = 'SUCCEEDED'`, ids["a"]); n != "1" {
		t.Errorf("succeeded attempts = %s, want exactly 1", n)
	}
	testutil.AssertInvariants(t, e.srv.Pool)
}

// TestWorkerLoss_FallbackForWorkersWithoutLease: a worker that deregistered (or was never known)
// holds no lease, so the lease source never reports it expired; its attempt is still abandoned
// once silent for 3×heartbeat + lease TTL.
func TestWorkerLoss_FallbackForWorkersWithoutLease(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	w := e.worker(t, "w-gone")
	_, ids := e.startRun(t, taskJSON("a", 2, 1000, 1000, 600))
	once(t, e.decide)
	w.mustPoll(t, 1)
	if _, err := w.c.Deregister(t.Context(), deregister("w-gone")); err != nil {
		t.Fatal(err)
	}
	e.clock.Advance(3*heartbeat + leaseTTL - time.Second)
	once(t, e.recov)
	if a, _ := e.srv.Attempt(t, ids["a"]); a.Status != model.AttemptRunning {
		t.Fatalf("abandoned before the staleness bound: %s", a.Status)
	}
	e.clock.Advance(recoveryTick)
	once(t, e.recov)
	if a, _ := e.srv.Attempt(t, ids["a"]); a.Status != model.AttemptAbandoned || e.task(t, ids["a"]).Status != model.TaskRetryWait {
		t.Fatalf("orphaned attempt %s, task %s; want ABANDONED and RETRY_WAIT", a.Status, e.task(t, ids["a"]).Status)
	}
	if s := e.srv.Scalar(t, `SELECT status FROM worker_observations WHERE worker_id = 'w-gone'`); s != "DEREGISTERED" {
		t.Errorf("worker status %s; the fallback must not mark workers", s)
	}
	testutil.AssertInvariants(t, e.srv.Pool)
}

// TestLoops_SurviveDatabaseOutage: with the scheduler's database connection cut for about 5 s,
// every loop backs off and counts its failures without stopping or panicking; once the database
// is back they resume and the run completes.
func TestLoops_SurviveDatabaseOutage(t *testing.T) {
	t.Parallel()
	srv := workerapitest.Start(t, workerapitest.Options{
		Outcomes: func(s *workerapitest.Server) workerapi.OutcomeHandler {
			return workerapi.NewRetryOutcomeHandler(s.Tasks, s.Runs, clock.Real{}, rand.New(rand.NewPCG(1, 1))) //nolint:gosec // G404: test jitter
		},
	})
	proxy, dsn := testutil.ProxyDSN(t, srv.Pool.Config().ConnString())
	pool, err := pg.NewPool(t.Context(), config.Secret(dsn))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	errs, err := scheduler.NewLoopErrors(obs.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	tm := pg.NewTxManager(pool, clock.Real{}, nil)
	ender := workerapi.AttemptEnder{Attempts: srv.Attempts, Tasks: srv.Tasks, Outcomes: srv.Service.Outcomes}
	runner := scheduler.NewRunner(scheduler.AlwaysLeader{})
	runner.RegisterLoop(&scheduler.DecisionLoop{
		DB: pool, Tx: tm, Cluster: pg.ClusterRepo{}, Sched: pg.SchedulerRepo{}, Notify: pg.SchedulerRepo{},
		Runs: srv.Runs, Tasks: srv.Tasks, Defs: srv.Defs, Inputs: scheduler.IdentityResolver{}, Clock: clock.Real{},
		Tick: 50 * time.Millisecond, Errors: errs,
	})
	runner.RegisterLoop(&scheduler.TimeoutLoop{DB: pool, Tx: tm, Cluster: pg.ClusterRepo{}, Recovery: pg.RecoveryRepo{},
		Ender: ender, Notify: pg.SchedulerRepo{}, Clock: clock.Real{}, Tick: 50 * time.Millisecond, Errors: errs})
	runner.RegisterLoop(&Loop{DB: pool, Tx: tm, Cluster: pg.ClusterRepo{}, Recovery: pg.RecoveryRepo{},
		Leases: &workerapi.PGLeaseProvider{DB: pool, Workers: pg.WorkerRepo{}, Clock: clock.Real{}, TTL: leaseTTL},
		Ender:  ender, Notify: pg.SchedulerRepo{}, Events: srv.Recorder, Clock: clock.Real{}, Tick: 50 * time.Millisecond,
		LeaseTTL: leaseTTL, Heartbeat: heartbeat, Errors: errs})

	e := &env{srv: srv, clock: clock.NewFake(time.Time{})} // only for startRun and the worker helpers
	run, _ := e.startRun(t, taskJSON("a", 1, 1000, 1000, 600), taskJSON("b", 1, 1000, 1000, 600))
	proxy.Cut()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	outage := time.Now() //nolint:forbidigo // measuring a real outage in a test
	testutil.WaitFor(t, func() bool {
		return promtest.ToFloat64(errs.WithLabelValues("scheduler")) >= 3 &&
			promtest.ToFloat64(errs.WithLabelValues("timeout")) >= 1 && promtest.ToFloat64(errs.WithLabelValues("recovery")) >= 1 &&
			time.Since(outage) >= 5*time.Second //nolint:forbidigo // the outage lasts about 5 s
	}, 30*time.Second)
	proxy.Restore()

	w := e.worker(t, "w-after-outage")
	testutil.WaitFor(t, func() bool {
		tasks, _ := w.poll(t)
		for _, task := range tasks {
			_ = w.complete(t, task, `{}`) // the run's status shows whether every task completed
		}
		return e.runStatus(t, run) == model.RunSucceeded
	}, 60*time.Second)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Runner.Run = %v; the loops must survive the outage", err)
	}
	t.Logf("loop errors during the outage: scheduler %v, timeout %v, recovery %v",
		promtest.ToFloat64(errs.WithLabelValues("scheduler")), promtest.ToFloat64(errs.WithLabelValues("timeout")), promtest.ToFloat64(errs.WithLabelValues("recovery")))
	testutil.AssertInvariants(t, srv.Pool)
}
