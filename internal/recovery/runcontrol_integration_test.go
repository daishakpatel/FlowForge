//go:build integration

package recovery

import (
	"context"
	"encoding/json"
	"log/slog"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/daishakpatel/flowforge/internal/apperrors"
	"github.com/daishakpatel/flowforge/internal/clock"
	workerv1 "github.com/daishakpatel/flowforge/internal/gen/proto/worker/v1"
	"github.com/daishakpatel/flowforge/internal/scheduler"
	"github.com/daishakpatel/flowforge/internal/testutil"
	"github.com/daishakpatel/flowforge/internal/worker/handlers"
	"github.com/daishakpatel/flowforge/internal/worker/runtime"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
)

// TestCancel_TasksNotStarted: cancelling a run whose tasks are PENDING, READY or QUEUED cancels
// them all on the next scheduler pass and ends the run CANCELLED; nothing is claimed afterwards.
func TestCancel_TasksNotStarted(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	w := e.worker(t, "w-idle")
	run, ids := e.startRun(t, taskJSON("a", 1, 1000, 1000, 60), `{"id":"b","type":"t","depends_on":["a"]}`, taskJSON("c", 1, 1000, 1000, 60))
	once(t, e.decide) // a and c QUEUED, b PENDING
	if _, err := e.runs.Cancel(t.Context(), run, false); err != nil {
		t.Fatal(err)
	}
	once(t, e.decide)
	for key, id := range ids {
		if s := e.task(t, id).Status; s != model.TaskCancelled {
			t.Errorf("task %s is %s, want CANCELLED", key, s)
		}
	}
	if s := e.runStatus(t, run); s != model.RunCancelled {
		t.Fatalf("run %s, want CANCELLED", s)
	}
	if tasks, _ := w.poll(t); len(tasks) != 0 {
		t.Fatalf("claimed %d tasks of a cancelled run", len(tasks))
	}
	if n := e.srv.Scalar(t, `SELECT count(*) FROM task_attempts`); n != "0" {
		t.Errorf("%s attempts created for a cancelled run", n)
	}
	if v, err := e.runs.Cancel(t.Context(), run, false); err != nil || v.Status != model.RunCancelled {
		t.Errorf("repeated cancel = %s, %v; want the CANCELLED run", v.Status, err)
	}
	testutil.AssertInvariants(t, e.srv.Pool)
}

// TestCancel_RunningTaskCooperative: a real worker running a task hears the cancel in its next
// heartbeat answer, cancels the handler's context within 2 heartbeat intervals and reports
// CANCELLED; the attempt and task are CANCELLED and then the run.
func TestCancel_RunningTaskCooperative(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	run, ids := e.startRun(t, taskJSON("a", 3, 1000, 1000, 600))
	once(t, e.decide)
	cancelled := make(chan time.Time, 1)
	stop := e.realWorker(t, "w-real", handlerFunc(func(ctx context.Context, _ handlers.TaskContext) (json.RawMessage, error) {
		<-ctx.Done()
		cancelled <- time.Now() //nolint:forbidigo // measuring real elapsed time in a test
		return nil, ctx.Err()
	}))
	defer stop()
	e.srv.WaitRunning(t, ids["a"])
	requested := time.Now() //nolint:forbidigo // measuring real elapsed time in a test
	if _, err := e.runs.Cancel(t.Context(), run, false); err != nil {
		t.Fatal(err)
	}
	took := (<-cancelled).Sub(requested)
	t.Logf("handler cancelled %s after the cancel request (heartbeat %s)", took, heartbeat)
	if took > 2*heartbeat+300*time.Millisecond {
		t.Fatalf("cancel reached the handler after %s, want within 2 heartbeats (%s)", took, 2*heartbeat)
	}
	a := e.srv.WaitFinished(t, ids["a"])
	if a.Status != model.AttemptCancelled || e.task(t, ids["a"]).Status != model.TaskCancelled {
		t.Fatalf("attempt %s task %s, want CANCELLED/CANCELLED", a.Status, e.task(t, ids["a"]).Status)
	}
	once(t, e.decide)
	if s := e.runStatus(t, run); s != model.RunCancelled {
		t.Fatalf("run %s, want CANCELLED", s)
	}
	testutil.AssertInvariants(t, e.srv.Pool)
}

// TestCancel_UnresponsiveWorkerForcedAfterGrace: with the worker silent, the cancelled run waits
// for the grace period, then its RUNNING attempt is force-cancelled; the late report is stale.
// With force=true there is no grace.
func TestCancel_UnresponsiveWorkerForcedAfterGrace(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	w := e.worker(t, "w-silent")
	run, ids := e.startRun(t, taskJSON("a", 3, 1000, 1000, 600))
	once(t, e.decide)
	claimed := w.mustPoll(t, 1)[0]
	if _, err := e.runs.Cancel(t.Context(), run, false); err != nil {
		t.Fatal(err)
	}
	once(t, e.decide)
	e.clock.Advance(cancelGrace - time.Second)
	once(t, e.timeouts)
	if s := e.task(t, ids["a"]).Status; s != model.TaskRunning || e.runStatus(t, run) == model.RunCancelled {
		t.Fatalf("forced before the grace ran out: task %s run %s", s, e.runStatus(t, run))
	}
	e.clock.Advance(time.Second)
	once(t, e.timeouts)
	a, _ := e.srv.Attempt(t, ids["a"])
	if a.Status != model.AttemptCancelled || e.task(t, ids["a"]).Status != model.TaskCancelled || e.runStatus(t, run) != model.RunCancelled {
		t.Fatalf("after the grace: attempt %s task %s run %s", a.Status, e.task(t, ids["a"]).Status, e.runStatus(t, run))
	}
	if err := w.complete(t, claimed, `{"late":true}`); reasonOf(err) != "STALE_ATTEMPT" {
		t.Fatalf("late completion = %v, want STALE_ATTEMPT", err)
	}
	testutil.AssertInvariants(t, e.srv.Pool)

	run2, ids2 := e.startRun(t, taskJSON("a", 3, 1000, 1000, 600))
	once(t, e.decide)
	w.mustPoll(t, 1)
	if _, err := e.runs.Cancel(t.Context(), run2, true); err != nil { // force: no grace
		t.Fatal(err)
	}
	once(t, e.timeouts)
	if s := e.task(t, ids2["a"]).Status; s != model.TaskCancelled || e.runStatus(t, run2) != model.RunCancelled {
		t.Fatalf("forced cancel: task %s run %s", s, e.runStatus(t, run2))
	}
}

// TestPause_RunningFinishesNothingNewUntilResume: while PAUSED, a running attempt completes
// normally but nothing new is queued or claimed; after resume the run continues to success.
func TestPause_RunningFinishesNothingNewUntilResume(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	w := e.worker(t, "w-pause")
	run, ids := e.startRun(t, taskJSON("a", 1, 1000, 1000, 600), `{"id":"b","type":"t","depends_on":["a"]}`)
	once(t, e.decide)
	running := w.mustPoll(t, 1)[0]
	for range 2 { // pausing a PAUSED run is a no-op
		if v, err := e.runs.Pause(t.Context(), run); err != nil || v.Status != model.RunPaused {
			t.Fatalf("pause = %s, %v", v.Status, err)
		}
	}
	if err := w.complete(t, running, `{}`); err != nil {
		t.Fatalf("a running attempt must complete normally while paused: %v", err)
	}
	once(t, e.decide)
	if s := e.task(t, ids["b"]).Status; s != model.TaskPending {
		t.Fatalf("b is %s while paused, want PENDING", s)
	}
	if tasks, _ := w.poll(t); len(tasks) != 0 {
		t.Fatal("a task was claimed while the run was paused")
	}
	if _, err := e.runs.Resume(t.Context(), run); err != nil {
		t.Fatal(err)
	}
	if _, err := e.runs.Resume(t.Context(), run); apperrors.CodeOf(err) != apperrors.CodeInvalidState {
		t.Fatalf("resume of a RUNNING run = %v, want INVALID_STATE", err)
	}
	once(t, e.decide)
	if err := w.complete(t, w.mustPoll(t, 1)[0], `{}`); err != nil {
		t.Fatal(err)
	}
	once(t, e.decide)
	if s := e.runStatus(t, run); s != model.RunSucceeded {
		t.Fatalf("run %s after resume, want SUCCEEDED", s)
	}
	if _, err := e.runs.Cancel(t.Context(), run, false); apperrors.CodeOf(err) != apperrors.CodeInvalidState {
		t.Fatalf("cancel of a SUCCEEDED run = %v, want INVALID_STATE", err)
	}
	testutil.AssertInvariants(t, e.srv.Pool)
}

// TestWorkflowTimeout_FailsRunAndCancelsTasks: a run with timeout_seconds=5 and a 60 s task fails
// with reason WORKFLOW_TIMEOUT when its deadline (set at start) passes; its tasks are CANCELLED
// and the worker's late report is stale. A paused run fails the same way: pause does not stop it.
func TestWorkflowTimeout_FailsRunAndCancelsTasks(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	w := e.worker(t, "w-timeout")
	run, ids := e.startRunWith(t, `"timeout_seconds":5,`, taskJSON("a", 3, 1000, 1000, 60), `{"id":"b","type":"t","depends_on":["a"]}`)
	once(t, e.decide)
	claimed := w.mustPoll(t, 1)[0]
	e.clock.Advance(4 * time.Second)
	once(t, e.timeouts)
	if s := e.runStatus(t, run); s != model.RunRunning {
		t.Fatalf("run %s before its deadline", s)
	}
	e.clock.Advance(time.Second)
	once(t, e.timeouts)
	if s := e.runStatus(t, run); s != model.RunFailed {
		t.Fatalf("run %s after its deadline, want FAILED", s)
	}
	for key, id := range ids {
		if s := e.task(t, id).Status; s != model.TaskCancelled {
			t.Errorf("task %s is %s, want CANCELLED", key, s)
		}
	}
	reason := e.srv.Scalar(t, `SELECT payload->>'reason' FROM workflow_events WHERE run_id = $1 AND event_type = 'run.failed'`, run)
	if reason != scheduler.ReasonWorkflowTimeout {
		t.Errorf("run.failed reason %q, want %s", reason, scheduler.ReasonWorkflowTimeout)
	}
	if err := w.complete(t, claimed, `{}`); reasonOf(err) != "STALE_ATTEMPT" {
		t.Fatalf("late completion = %v, want STALE_ATTEMPT", err)
	}

	paused, _ := e.startRunWith(t, `"timeout_seconds":5,`, taskJSON("a", 1, 1000, 1000, 60))
	once(t, e.decide)
	if _, err := e.runs.Pause(t.Context(), paused); err != nil {
		t.Fatal(err)
	}
	e.clock.Advance(5 * time.Second)
	once(t, e.timeouts)
	if s := e.runStatus(t, paused); s != model.RunFailed {
		t.Fatalf("paused run past its deadline is %s, want FAILED", s)
	}
	testutil.AssertInvariants(t, e.srv.Pool)
}

// handlerFunc adapts a function to handlers.Handler for task type "t".
type handlerFunc func(context.Context, handlers.TaskContext) (json.RawMessage, error)

func (handlerFunc) Type() string { return "t" }
func (f handlerFunc) Execute(ctx context.Context, tc handlers.TaskContext) (json.RawMessage, error) {
	return f(ctx, tc)
}

// realWorker runs a flowforge-worker runtime (real clock) against the env's API with one
// handler for type "t"; the returned function stops it (also run at cleanup).
func (e *env) realWorker(t *testing.T, id string, h handlers.Handler) func() {
	t.Helper()
	reg := handlers.NewRegistry()
	if err := reg.Register(h); err != nil {
		t.Fatal(err)
	}
	conn, err := runtime.Dial(e.srv.Addr, e.srv.Token)
	if err != nil {
		t.Fatal(err)
	}
	w := runtime.New(runtime.Config{
		ID: id, Queues: []string{model.DefaultQueue}, Capacity: 2,
		PollWait: 200 * time.Millisecond, RPCTimeout: 2 * time.Second, ShutdownGrace: time.Second,
	}, runtime.Deps{
		Client: workerv1.NewWorkerServiceClient(conn), Registry: reg, Clock: clock.Real{},
		Logger: slog.New(slog.DiscardHandler), Rand: rand.New(rand.NewPCG(3, 3)), //nolint:gosec // G404: test jitter
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		if err := <-done; err != nil {
			t.Errorf("worker %s Run = %v, want a clean shutdown", id, err)
		}
		_ = conn.Close() // releases resources only
	}
	t.Cleanup(stop)
	return stop
}
