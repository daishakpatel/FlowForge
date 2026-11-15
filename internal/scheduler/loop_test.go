package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	promtest "github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/obs"
	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/testutil"
	"github.com/daishakpatel/flowforge/internal/workerapi"
	"github.com/daishakpatel/flowforge/internal/workflow/expr"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
)

const diamond = `{"name":"diamond","version":1,"tasks":[{"id":"a","type":"t"},{"id":"b","type":"t","depends_on":["a"]},
  {"id":"c","type":"t","depends_on":["a"]},{"id":"d","type":"t","depends_on":["b","c"]}]}`

func newFakeLoop(t *testing.T) (*memStore, *DecisionLoop) {
	t.Helper()
	s := newMemStore()
	return s, s.loop(clock.NewFake(time.Date(2026, 11, 14, 1, 0, 0, 0, time.UTC)))
}

func tickOK(t *testing.T, l *DecisionLoop) {
	t.Helper()
	if err := l.Once(t.Context(), AlwaysLeader{}); err != nil {
		t.Fatal(err)
	}
}

func TestDecisionLoop_DrivesRunsToTheirStatus(t *testing.T) {
	t.Parallel()
	s, l := newFakeLoop(t)
	reg := obs.NewRegistry()
	m, err := NewMetrics(reg)
	if err != nil {
		t.Fatal(err)
	}
	l.Metrics = m
	ok := s.addRun(diamond)
	bad := s.addRun(`{"name":"bad","version":1,"tasks":[{"id":"a","type":"t"},{"id":"b","type":"t","depends_on":["a"]}]}`)

	tickOK(t, l)
	if s.status(ok, "a") != model.TaskQueued || s.status(ok, "b") != model.TaskPending || s.runStatus(ok) != model.RunRunning {
		t.Fatal("first tick must start the run and queue only the root")
	}
	if got := promtest.ToFloat64(m.QueueDepth.WithLabelValues("default")); got != 2 {
		t.Errorf("queue depth %v, want 2", got)
	}
	for range 4 {
		s.execute("a") // root a fails in both runs
		tickOK(t, l)
	}
	if s.runStatus(ok) != model.RunFailed || s.status(ok, "d") != model.TaskSkipped {
		t.Fatalf("diamond with failed root: run %s, d %s; want FAILED, SKIPPED (skip propagates)", s.runStatus(ok), s.status(ok, "d"))
	}
	if s.runStatus(bad) != model.RunFailed || s.status(bad, "b") != model.TaskSkipped {
		t.Fatalf("unhandled failure: run %s, b %s; want FAILED, SKIPPED", s.runStatus(bad), s.status(bad, "b"))
	}
	if !slices.Contains(s.notified, persistence.TaskQueuedChannel) {
		t.Error("no NOTIFY ff_task_queued after queueing")
	}
	if got := promtest.ToFloat64(m.WorkflowsStarted); got != 2 {
		t.Errorf("workflows started %v, want 2", got)
	}
	if got := promtest.ToFloat64(m.WorkflowsCompleted.WithLabelValues("FAILED")); got != 2 {
		t.Errorf("workflows completed FAILED %v", got)
	}
	if got := promtest.ToFloat64(m.Tasks.WithLabelValues("SKIPPED")); got != 4 { // b, c, d and bad's b
		t.Errorf("tasks SKIPPED %v", got)
	}
}

func TestDecisionLoop_DiamondSucceedsInOrder(t *testing.T) {
	t.Parallel()
	s, l := newFakeLoop(t)
	run := s.addRun(diamond)
	steps := []map[string]model.TaskStatus{
		{"a": model.TaskQueued, "b": model.TaskPending},
		{"b": model.TaskQueued, "c": model.TaskQueued, "d": model.TaskPending},
		{"d": model.TaskQueued},
	}
	for i, want := range steps {
		tickOK(t, l)
		for k, st := range want {
			if got := s.status(run, k); got != st {
				t.Fatalf("step %d: %s is %s, want %s", i, k, got, st)
			}
		}
		s.execute()
	}
	tickOK(t, l)
	if s.runStatus(run) != model.RunSucceeded || s.count("run.succeeded") != 1 || s.count("task.queued") != 4 {
		t.Fatalf("run %s, %d run.succeeded, %d task.queued", s.runStatus(run), s.count("run.succeeded"), s.count("task.queued"))
	}
}

func TestDecisionLoop_StaleEpochStops(t *testing.T) {
	t.Parallel()
	s, l := newFakeLoop(t)
	run := s.addRun(diamond)
	s.epoch = 7
	if err := l.Run(t.Context(), AlwaysLeader{}); !errors.Is(err, persistence.ErrStaleEpoch) {
		t.Fatalf("Run = %v, want ErrStaleEpoch", err)
	}
	if s.runStatus(run) != model.RunCreated || len(s.events) != 0 {
		t.Fatalf("decisions made at a stale epoch: %v", s.events)
	}
}

// fakeWaker is a Waker the test fires by hand.
type fakeWaker struct {
	mu sync.Mutex
	ch chan struct{}
}

func (w *fakeWaker) Wait() <-chan struct{} {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.ch
}

func (w *fakeWaker) fire() {
	w.mu.Lock()
	defer w.mu.Unlock()
	close(w.ch)
	w.ch = make(chan struct{})
}

func TestDecisionLoop_RunTicksAndWakes(t *testing.T) {
	t.Parallel()
	s := newMemStore()
	fc := clock.NewFake(time.Date(2026, 11, 14, 1, 0, 0, 0, time.UTC))
	l := s.loop(fc)
	w := &fakeWaker{ch: make(chan struct{})}
	l.Wake = w
	run := s.addRun(diamond)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- l.Run(ctx, AlwaysLeader{}) }()

	testutil.WaitFor(t, func() bool { return s.status(run, "a") == model.TaskQueued }, 5*time.Second) // first tick at once
	s.execute()
	w.fire() // a notification, no clock movement
	testutil.WaitFor(t, func() bool { return s.status(run, "b") == model.TaskQueued }, 5*time.Second)
	s.execute()
	fc.BlockUntilWaiters(1)
	fc.Advance(time.Second) // the tick
	testutil.WaitFor(t, func() bool { return s.status(run, "d") == model.TaskQueued }, 5*time.Second)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run = %v, want nil on cancel", err)
	}
}

// closedGate is leader by every measure except that its term has already ended.
type closedGate struct{ AlwaysLeader }

func (closedGate) Done() <-chan struct{} { c := make(chan struct{}); close(c); return c }

func TestDecisionLoop_NoDecisionAfterTermEnds(t *testing.T) {
	t.Parallel()
	s, l := newFakeLoop(t)
	run := s.addRun(diamond)
	if err := l.Once(t.Context(), closedGate{}); err != nil {
		t.Fatal(err)
	}
	if s.runStatus(run) != model.RunCreated {
		t.Fatal("a decision was made after Done closed")
	}
}

func TestProcessRun_BenignRacePausedAndUndecidable(t *testing.T) {
	t.Parallel()
	s, l := newFakeLoop(t)
	run := s.addRun(diamond)
	s.rejectTask = run + "_a" // someone else moves a first: not an error, nothing counted
	res, err := l.runOnce(t.Context(), run, 0)
	if err != nil || res.tasks[model.TaskReady] != 0 || res.queued != 0 || !res.started {
		t.Fatalf("benign race: res %+v err %v", res, err)
	}
	tickOK(t, l)
	if s.status(run, "a") != model.TaskQueued {
		t.Fatal("a not queued on the next tick")
	}

	s.runs[run].Status = model.RunPaused
	before := len(s.events)
	if _, err := l.runOnce(t.Context(), run, 0); err != nil || len(s.events) != before {
		t.Fatalf("paused run changed: err %v, events %v", err, s.events[before:])
	}

	expr := s.addRun(`{"name":"expr","version":1,"tasks":[{"id":"a","type":"t"},
	  {"id":"b","type":"t","depends_on":["a"],"run_if":"expr","run_if_expr":"tasks.a.status == 'SUCCEEDED'"}]}`)
	tickOK(t, l)
	s.execute()
	tickOK(t, l)
	if s.status(expr, "b") != model.TaskPending {
		t.Fatalf("run_if=expr without an evaluator must stay PENDING, is %s", s.status(expr, "b"))
	}
}

type errResolver struct{}

func (errResolver) Resolve(context.Context, persistence.RunView, persistence.TaskView, []persistence.TaskView) (json.RawMessage, error) {
	return nil, errors.New("bad template")
}

func TestProcessRun_InputFailureAndLimits(t *testing.T) {
	t.Parallel()
	s, l := newFakeLoop(t)
	l.Inputs = errResolver{}
	run := s.addRun(diamond)
	tickOK(t, l)
	tickOK(t, l)
	if s.status(run, "a") != model.TaskFailed || s.count("task.failed a") != 1 || s.runStatus(run) != model.RunFailed {
		t.Fatalf("resolver failure: a %s, run %s, events %v", s.status(run, "a"), s.runStatus(run), s.events)
	}

	s2, l2 := newFakeLoop(t)
	l2.QueueLimits = map[string]int{"default": 1}
	wide := `{"name":"w","version":1,"concurrency":{"max_parallel_tasks":2},"tasks":[{"id":"a","type":"t"},{"id":"b","type":"t"},{"id":"c","type":"t"}]}`
	r1, r2 := s2.addRun(wide), s2.addRun(wide)
	tickOK(t, l2)
	queued := 0
	for _, r := range []string{r1, r2} {
		for _, k := range []string{"a", "b", "c"} {
			if s2.status(r, k) == model.TaskQueued {
				queued++
			}
		}
	}
	if queued != 1 {
		t.Fatalf("queue limit 1: %d queued", queued)
	}
	l2.QueueLimits = nil
	tickOK(t, l2)
	if got := s2.count("task.queued"); got != 4 { // max_parallel_tasks 2 per run
		t.Fatalf("per-run limit 2 over two runs: %d queued", got)
	}
}

func TestDecisionLoop_PageWrapsRoundRobin(t *testing.T) {
	t.Parallel()
	s, l := newFakeLoop(t)
	l.pageSize = 3
	for range 7 {
		s.addRun(diamond)
	}
	for i, want := range []int{3, 6, 7} {
		tickOK(t, l)
		if got := s.count("run.started"); got != want {
			t.Fatalf("tick %d: %d runs started, want %d", i+1, got, want)
		}
	}
}

// TestProcessRun_PromotesDueRetries: a RETRY_WAIT task is queued once its available_at has come
// on the injected clock (03.04), keeping the input resolved before its first attempt.
func TestProcessRun_PromotesDueRetries(t *testing.T) {
	t.Parallel()
	s := newMemStore()
	fc := clock.NewFake(time.Date(2026, 11, 14, 1, 0, 0, 0, time.UTC))
	l := s.loop(fc)
	run := s.addRun(`{"name":"r","version":1,"tasks":[{"id":"a","type":"t"}]}`)
	tickOK(t, l)
	a := s.task(run + "_a")
	a.Status, a.Input, a.AvailableAt = model.TaskRetryWait, []byte(`{"kept":true}`), fc.Now().Add(2*time.Second)
	tickOK(t, l)
	if s.status(run, "a") != model.TaskRetryWait {
		t.Fatal("a retry was queued before its available_at")
	}
	fc.Advance(2 * time.Second)
	tickOK(t, l)
	if s.status(run, "a") != model.TaskQueued || string(a.Input) != `{"kept":true}` || s.count("task.queued a") != 2 {
		t.Fatalf("due retry: %s input %s, %d task.queued", s.status(run, "a"), a.Input, s.count("task.queued a"))
	}
}

// TestRunTicking_BacksOffAndRecovers: a failing iteration is counted and followed by a growing,
// capped backoff instead of the tick; a stale epoch stops the loop.
func TestRunTicking_BacksOffAndRecovers(t *testing.T) {
	t.Parallel()
	if ErrorBackoff(1, time.Second) != 2*time.Second || ErrorBackoff(3, time.Second) != 8*time.Second || ErrorBackoff(20, time.Second) != maxLoopBackoff {
		t.Fatal("ErrorBackoff must double per failure and cap at 30s")
	}
	fc := clock.NewFake(time.Date(2026, 11, 14, 1, 0, 0, 0, time.UTC))
	errs, err := NewLoopErrors(obs.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int64
	results := make(chan error, 8)
	for _, e := range []error{errors.New("db down"), errors.New("db down"), nil, persistence.ErrStaleEpoch} {
		results <- e
	}
	done := make(chan error, 1)
	go func() {
		done <- RunTicking(t.Context(), "test", fc, time.Second, nil, errs, func(context.Context) error {
			calls.Add(1)
			return <-results
		})
	}()
	for _, wait := range []time.Duration{2 * time.Second, 4 * time.Second, time.Second} { // two backoffs, then the tick
		fc.BlockUntilWaiters(1)
		fc.Advance(wait - time.Millisecond)
		if fc.Waiters() != 1 {
			t.Fatalf("woke before %s", wait)
		}
		fc.Advance(time.Millisecond)
	}
	if err := <-done; !errors.Is(err, persistence.ErrStaleEpoch) {
		t.Fatalf("RunTicking = %v, want the stale epoch", err)
	}
	if calls.Load() != 4 || promtest.ToFloat64(errs.WithLabelValues("test")) != 2 {
		t.Fatalf("calls %d, errors %v", calls.Load(), promtest.ToFloat64(errs.WithLabelValues("test")))
	}
}

// TestProcessRun_CancelRequested: tasks that have not started are cancelled at once; a RUNNING
// task holds the run until it ends; then the run is CANCELLED, from RUNNING or PAUSED (03.05).
func TestProcessRun_CancelRequested(t *testing.T) {
	t.Parallel()
	s, l := newFakeLoop(t)
	run := s.addRun(diamond)
	tickOK(t, l)
	s.task(run + "_a").Status = model.TaskRunning
	s.runs[run].CancelRequested = true
	tickOK(t, l)
	for _, k := range []string{"b", "c", "d"} {
		if st := s.status(run, k); st != model.TaskCancelled {
			t.Errorf("%s is %s, want CANCELLED", k, st)
		}
	}
	if s.runStatus(run) != model.RunRunning {
		t.Fatalf("run %s while a task still runs, want RUNNING", s.runStatus(run))
	}
	s.task(run + "_a").Status = model.TaskCancelled // the worker stopped it
	tickOK(t, l)
	if s.runStatus(run) != model.RunCancelled || s.count("run.cancelled") != 1 {
		t.Fatalf("run %s, %d run.cancelled", s.runStatus(run), s.count("run.cancelled"))
	}

	paused := s.addRun(diamond)
	s.runs[paused].Status, s.runs[paused].CancelRequested = model.RunPaused, true
	created := s.addRun(diamond)
	s.runs[created].CancelRequested = true
	for _, id := range []string{paused, created} {
		if _, err := l.runOnce(t.Context(), id, 0); err != nil {
			t.Fatal(err)
		}
		if s.runStatus(id) != model.RunCancelled {
			t.Errorf("cancel-requested run %s, want CANCELLED", s.runStatus(id))
		}
	}
}

// TestTimeoutLoop_EndsOverdueRuns: the start sets the workflow deadline; once it passes the run's
// unfinished tasks are CANCELLED and the run FAILED with reason WORKFLOW_TIMEOUT, or CANCELLED
// when a cancel was requested (its grace ran out).
func TestTimeoutLoop_EndsOverdueRuns(t *testing.T) {
	t.Parallel()
	s, l := newFakeLoop(t)
	fc := l.Clock.(*clock.Fake)
	run := s.addRun(`{"name":"to","version":1,"timeout_seconds":5,"tasks":[{"id":"a","type":"t"},{"id":"b","type":"t","depends_on":["a"]}]}`)
	cancelled := s.addRun(diamond)
	tickOK(t, l)
	if d := s.runs[run].DeadlineAt; d == nil || !d.Equal(fc.Now().Add(5*time.Second)) {
		t.Fatalf("deadline %v, want start + 5s", d)
	}
	past := fc.Now().Add(-time.Second)
	s.runs[cancelled].CancelRequested, s.runs[cancelled].DeadlineAt = true, &past
	tl := &TimeoutLoop{
		Tx: memTx{}, Cluster: memCluster{s}, Recovery: memRecovery{}, Notify: memSched{s},
		Ender: workerapi.AttemptEnder{Tasks: memTasks{s: s}}, Runs: memRuns{s: s}, Sched: memSched{s},
		RunControl: memRunControl{s: s}, Clock: fc, Tick: time.Second,
	}
	if err := tl.Once(t.Context(), AlwaysLeader{}); err != nil {
		t.Fatal(err)
	}
	if s.runStatus(run) != model.RunRunning || s.runStatus(cancelled) != model.RunCancelled {
		t.Fatalf("before the deadline: run %s; cancel-requested run past its grace %s", s.runStatus(run), s.runStatus(cancelled))
	}
	fc.Advance(5 * time.Second)
	if err := tl.Once(t.Context(), AlwaysLeader{}); err != nil {
		t.Fatal(err)
	}
	if s.runStatus(run) != model.RunFailed || s.status(run, "a") != model.TaskCancelled || s.status(run, "b") != model.TaskCancelled {
		t.Fatalf("after the deadline: run %s, a %s, b %s", s.runStatus(run), s.status(run, "a"), s.status(run, "b"))
	}
	if s.count("run.failed "+run) != 1 {
		t.Errorf("want one run.failed event for %s", run)
	}
}

// TestProcessRun_ExprAndTemplatesSeeAncestors: run_if expressions and config placeholders read
// any ancestor (here a grandparent) and the run input, with the production evaluator and resolver.
func TestProcessRun_ExprAndTemplatesSeeAncestors(t *testing.T) {
	t.Parallel()
	s, l := newFakeLoop(t)
	l.Inputs, l.Eval = TemplateResolver{}, expr.Evaluator{}
	run := s.addRun(`{"name":"anc","version":1,"tasks":[{"id":"a","type":"t"},{"id":"b","type":"t","depends_on":["a"]},
	  {"id":"c","type":"t","depends_on":["b"],"run_if":"expr","run_if_expr":"tasks.a.result.n > run.input.min",
	   "config":{"v":"${tasks.a.result.n}","note":"min ${run.input.min}"}},
	  {"id":"d","type":"t","depends_on":["b"],"run_if":"expr","run_if_expr":"tasks.a.result.n > 100"}]}`)
	s.runs[run].Input = json.RawMessage(`{"min":1}`)
	tickOK(t, l)
	s.mu.Lock()
	a := s.task(run + "_a")
	a.Status, a.Result = model.TaskSucceeded, json.RawMessage(`{"n":5}`)
	s.mu.Unlock()
	tickOK(t, l)
	s.execute() // b
	tickOK(t, l)
	if s.status(run, "c") != model.TaskQueued || s.status(run, "d") != model.TaskSkipped {
		t.Fatalf("c %s (want QUEUED), d %s (want SKIPPED); events %v", s.status(run, "c"), s.status(run, "d"), s.events)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if got := string(s.task(run + "_c").Input); got != `{"note":"min 1","v":5}` {
		t.Errorf("c input = %s", got)
	}
}
