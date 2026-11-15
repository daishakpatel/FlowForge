//go:build integration

package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/daishakpatel/flowforge/internal/clock"
	workerv1 "github.com/daishakpatel/flowforge/internal/gen/proto/worker/v1"
	"github.com/daishakpatel/flowforge/internal/obs"
	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/persistence/pg"
	"github.com/daishakpatel/flowforge/internal/testutil"
	"github.com/daishakpatel/flowforge/internal/worker/handlers"
	"github.com/daishakpatel/flowforge/internal/workerapi/workerapitest"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
)

// fn adapts a function to handlers.Handler for test-only task types.
type fn struct {
	typ string
	f   func(ctx context.Context, tc handlers.TaskContext) (json.RawMessage, error)
}

func (h fn) Type() string { return h.typ }
func (h fn) Execute(ctx context.Context, tc handlers.TaskContext) (json.RawMessage, error) {
	return h.f(ctx, tc)
}

var workerSeq atomic.Int64

// syncBuffer collects a worker's log, printed only when its test fails.
type syncBuffer struct {
	mu sync.Mutex
	b  []byte
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.b = append(s.b, p...)
	return len(p), nil
}

func (s *syncBuffer) String() string { s.mu.Lock(); defer s.mu.Unlock(); return string(s.b) }

// running is a started Worker; stop shuts it down (as SIGTERM would) and returns Run's error.
type running struct {
	w       *Worker
	metrics *Metrics
	stop    func() error
}

// startWorker runs a Worker against srv with the given handlers. change adjusts its Config.
func startWorker(t *testing.T, srv *workerapitest.Server, hs []handlers.Handler, change ...func(*Config)) *running {
	t.Helper()
	reg := handlers.NewRegistry()
	for _, h := range hs {
		if err := reg.Register(h); err != nil {
			t.Fatal(err)
		}
	}
	conn, err := Dial(srv.Addr, srv.Token)
	if err != nil {
		t.Fatal(err)
	}
	m, err := NewMetrics(obs.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{
		ID: fmt.Sprintf("w-test-%d", workerSeq.Add(1)), Queues: []string{model.DefaultQueue}, Capacity: 4,
		PollWait: 500 * time.Millisecond, RPCTimeout: 2 * time.Second, ShutdownGrace: 5 * time.Second,
	}
	for _, c := range change {
		c(&cfg)
	}
	logs := &syncBuffer{}
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("worker %s log:\n%s", cfg.ID, logs.String())
		}
	})
	w := New(cfg, Deps{
		Client: workerv1.NewWorkerServiceClient(conn), Registry: reg, Clock: clock.Real{},
		Logger: slog.New(slog.NewTextHandler(logs, nil)), Metrics: m, Rand: rand.New(rand.NewPCG(1, 2)), //nolint:gosec // G404: test jitter
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	stop := sync.OnceValue(func() error {
		cancel()
		err := <-done
		_ = conn.Close() // releases resources only
		return err
	})
	t.Cleanup(func() { _ = stop() })
	return &running{w: w, metrics: m, stop: stop}
}

func builtins(exit func(int)) []handlers.Handler { return handlers.Builtins(clock.Real{}, exit) }

func wantAttempt(t *testing.T, a persistence.AttemptView, status model.AttemptStatus, code string, retryable *bool) {
	t.Helper()
	if a.Status != status || a.ErrorCode != code {
		t.Fatalf("attempt %s: status %s code %q, want %s %q", a.ID, a.Status, a.ErrorCode, status, code)
	}
	if retryable != nil && (a.Retryable == nil || *a.Retryable != *retryable) {
		t.Fatalf("attempt %s: retryable %v, want %v", a.ID, a.Retryable, *retryable)
	}
}

var yes, no = ptr(true), ptr(false)

func ptr(b bool) *bool { return &b }

func TestWorker_BuiltinsEndToEnd(t *testing.T) {
	t.Parallel()
	srv := workerapitest.Start(t, workerapitest.Options{})
	exited := make(chan int, 1)
	startWorker(t, srv, builtins(func(code int) { exited <- code }))

	noop := srv.Queue(t, "noop", 60, `{"a":1}`)[0]
	echo := srv.Queue(t, "echo", 60, `{"x":true}`)[0]
	sleep := srv.Queue(t, "sleep", 60, `{"duration_ms":50}`)[0]
	fail := srv.Queue(t, "fail", 60, `{"code":"NOPE","retryable":false}`)[0]
	flaky := srv.Queue(t, "flaky", 60, `{"fail_attempts":0}`, `{"fail_attempts":1}`)
	crash := srv.Queue(t, "crash", 60, `{}`)[0]

	a := srv.WaitFinished(t, noop)
	wantAttempt(t, a, model.AttemptSucceeded, "", nil)
	task, err := srv.Tasks.Get(t.Context(), srv.Pool, noop)
	if err != nil || string(a.Result) != `{"a": 1}` || string(task.Result) != `{"a": 1}` || task.Status != model.TaskSucceeded {
		t.Fatalf("noop: attempt result %s, task %s result %s, %v", a.Result, task.Status, task.Result, err)
	}
	if n := srv.Scalar(t, `SELECT count(*) FROM workflow_events WHERE task_id = $1 AND event_type IN ('task.started','task.attempt.started','task.attempt.succeeded','task.succeeded')`, noop); n != "4" {
		t.Errorf("noop events = %s, want 4", n)
	}
	var echoed map[string]any
	a = srv.WaitFinished(t, echo)
	if a.Status != model.AttemptSucceeded || json.Unmarshal(a.Result, &echoed) != nil || echoed["attempt_number"] != float64(1) || echoed["task_id"] != echo {
		t.Fatalf("echo: %s %s", a.Status, a.Result)
	}
	wantAttempt(t, srv.WaitFinished(t, sleep), model.AttemptSucceeded, "", nil)
	wantAttempt(t, srv.WaitFinished(t, fail), model.AttemptFailed, "NOPE", no)
	wantAttempt(t, srv.WaitFinished(t, flaky[0]), model.AttemptSucceeded, "", nil)
	wantAttempt(t, srv.WaitFinished(t, flaky[1]), model.AttemptFailed, "FLAKY", yes)
	wantAttempt(t, srv.WaitFinished(t, crash), model.AttemptFailed, "CRASH_RETURNED", no)
	if code := <-exited; code != 1 {
		t.Errorf("crash exit code %d, want 1", code)
	}
}

func TestWorker_FailureClassification(t *testing.T) {
	t.Parallel()
	srv := workerapitest.Start(t, workerapitest.Options{})
	startWorker(t, srv, []handlers.Handler{
		fn{"panics", func(context.Context, handlers.TaskContext) (json.RawMessage, error) { panic("boom") }},
		fn{"errs", func(context.Context, handlers.TaskContext) (json.RawMessage, error) { return nil, errors.New("plain") }},
		fn{"badjson", func(context.Context, handlers.TaskContext) (json.RawMessage, error) { return json.RawMessage(`{`), nil }},
	})
	wantAttempt(t, srv.WaitFinished(t, srv.Queue(t, "panics", 60, `{}`)[0]), model.AttemptFailed, handlers.CodePanic, yes)
	wantAttempt(t, srv.WaitFinished(t, srv.Queue(t, "errs", 60, `{}`)[0]), model.AttemptFailed, handlers.CodeHandlerError, yes)
	wantAttempt(t, srv.WaitFinished(t, srv.Queue(t, "badjson", 60, `{}`)[0]), model.AttemptFailed, "INVALID_RESULT", no)
}

func TestWorker_CapacityHonored(t *testing.T) {
	t.Parallel()
	srv := workerapitest.Start(t, workerapitest.Options{})
	r := startWorker(t, srv, builtins(nil), func(c *Config) { c.Capacity = 2 })
	inputs := make([]string, 10)
	for i := range inputs {
		inputs[i] = `{"duration_ms":150}`
	}
	ids := srv.Queue(t, "sleep", 60, inputs...)
	peak := 0
	finished := testutil.Eventually(func() bool {
		n, _ := strconv.Atoi(srv.Scalar(t, `SELECT count(*) FROM task_attempts WHERE worker_id = $1 AND status = 'RUNNING'`, r.w.cfg.ID))
		peak = max(peak, n)
		done, _ := strconv.Atoi(srv.Scalar(t, `SELECT count(*) FROM task_attempts WHERE status = 'SUCCEEDED'`))
		return done == len(ids)
	}, 30*time.Second, 10*time.Millisecond)
	if !finished || peak > 2 || peak == 0 {
		t.Fatalf("finished %v, peak RUNNING %d (capacity 2)", finished, peak)
	}
}

// blocker runs until its context ends and reports when that happened.
func blocker(typ string, cancelled chan<- time.Time) handlers.Handler {
	return fn{typ, func(ctx context.Context, _ handlers.TaskContext) (json.RawMessage, error) {
		<-ctx.Done()
		cancelled <- time.Now() //nolint:forbidigo // measuring real elapsed time in a test
		return nil, ctx.Err()
	}}
}

// cancelOne is a CancelSource that cancels one attempt once armed.
type cancelOne struct{ id atomic.Value }

func (c *cancelOne) CancelledAttempts(_ context.Context, ids []string) ([]string, error) {
	want, _ := c.id.Load().(string)
	for _, id := range ids {
		if id == want {
			return []string{id}, nil
		}
	}
	return nil, nil
}

func TestWorker_ServerCancellation(t *testing.T) {
	t.Parallel()
	const beat = 200 * time.Millisecond
	cs := &cancelOne{}
	srv := workerapitest.Start(t, workerapitest.Options{HeartbeatInterval: beat, Cancels: cs})
	cancelled := make(chan time.Time, 1)
	startWorker(t, srv, []handlers.Handler{blocker("block", cancelled)})
	id := srv.Queue(t, "block", 60, `{}`)[0]
	a := srv.WaitRunning(t, id)
	armed := time.Now() //nolint:forbidigo // measuring real elapsed time in a test
	cs.id.Store(a.ID)
	if took := (<-cancelled).Sub(armed); took > 2*beat+150*time.Millisecond {
		t.Errorf("handler cancelled %s after the cancel was armed; want within 2 heartbeats (%s)", took, 2*beat)
	}
	wantAttempt(t, srv.WaitFinished(t, id), model.AttemptCancelled, handlers.CodeCancelled, no) // CANCELLED since 03.05 (was FAILED)
}

// abandon ends an attempt server-side, as recovery (03.04) will for a lost worker.
func abandon(t *testing.T, srv *workerapitest.Server, a persistence.AttemptView) {
	t.Helper()
	if err := srv.TM.WithTx(t.Context(), pgx.TxOptions{}, func(tx pgx.Tx) error {
		return srv.Attempts.Transition(t.Context(), tx, persistence.AttemptTransitionParams{
			AttemptID: a.ID, From: model.AttemptRunning, To: model.AttemptAbandoned, FencingToken: a.FencingToken,
		})
	}); err != nil {
		t.Fatal(err)
	}
}

func TestWorker_StaleAttemptFromHeartbeat(t *testing.T) {
	t.Parallel()
	const beat = 200 * time.Millisecond
	srv := workerapitest.Start(t, workerapitest.Options{HeartbeatInterval: beat})
	cancelled := make(chan time.Time, 1)
	r := startWorker(t, srv, []handlers.Handler{blocker("block", cancelled)})
	id := srv.Queue(t, "block", 60, `{}`)[0]
	a := srv.WaitRunning(t, id)
	abandoned := time.Now() //nolint:forbidigo // measuring real elapsed time in a test
	abandon(t, srv, a)
	if took := (<-cancelled).Sub(abandoned); took > 2*beat+150*time.Millisecond {
		t.Errorf("stale handler cancelled after %s; want within 2 heartbeats", took)
	}
	testutil.WaitFor(t, func() bool { return promtest.ToFloat64(r.metrics.outcomes.WithLabelValues("stale")) == 1 }, 5*time.Second)
	if n := srv.Calls("CompleteTask") + srv.Calls("FailTask"); n != 0 {
		t.Errorf("a stale attempt was reported %d times; its result must be dropped", n)
	}
	wantAttempt(t, srv.WaitFinished(t, id), model.AttemptAbandoned, "", nil)
}

func TestWorker_StaleReportIsNotRetried(t *testing.T) {
	t.Parallel()
	// No heartbeat during the test, so the report itself meets the stale token.
	srv := workerapitest.Start(t, workerapitest.Options{HeartbeatInterval: time.Minute})
	release := make(chan struct{})
	r := startWorker(t, srv, []handlers.Handler{fn{"gate", func(ctx context.Context, _ handlers.TaskContext) (json.RawMessage, error) {
		select {
		case <-release:
			return json.RawMessage(`{"late":true}`), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}}})
	id := srv.Queue(t, "gate", 60, `{}`)[0]
	abandon(t, srv, srv.WaitRunning(t, id))
	close(release)
	testutil.WaitFor(t, func() bool { return promtest.ToFloat64(r.metrics.outcomes.WithLabelValues("stale")) == 1 }, 10*time.Second)
	if n := srv.Calls("CompleteTask"); n != 1 {
		t.Errorf("CompleteTask called %d times; a STALE_ATTEMPT answer must not be retried", n)
	}
	wantAttempt(t, srv.WaitFinished(t, id), model.AttemptAbandoned, "", nil)
}

func TestWorker_ReconnectAfterAPIRestart(t *testing.T) {
	t.Parallel()
	srv := workerapitest.Start(t, workerapitest.Options{HeartbeatInterval: 100 * time.Millisecond})
	r := startWorker(t, srv, builtins(nil))
	id := srv.Queue(t, "sleep", 60, `{"duration_ms":1500}`)[0]
	srv.WaitRunning(t, id)
	registered := srv.Calls("Register")
	srv.Stop()
	testutil.WaitFor(t, func() bool { return promtest.ToFloat64(r.metrics.leaseFailures) > 0 }, 10*time.Second)
	srv.Restart(t)
	wantAttempt(t, srv.WaitFinished(t, id), model.AttemptSucceeded, "", nil)
	testutil.WaitFor(t, func() bool { return srv.Calls("Register") > registered }, 10*time.Second)
	// The API commits before the worker reads the answer, so the counter can lag the row.
	testutil.WaitFor(t, func() bool { return promtest.ToFloat64(r.metrics.outcomes.WithLabelValues("succeeded")) == 1 }, 10*time.Second)
}

func TestWorker_GracefulShutdown(t *testing.T) {
	t.Parallel()
	deregistered := func(srv *workerapitest.Server, id string) bool {
		return srv.Scalar(t, `SELECT status FROM worker_observations WHERE worker_id = $1`, id) == "DEREGISTERED"
	}
	t.Run("in-flight attempt finishes within the grace", func(t *testing.T) {
		t.Parallel()
		srv := workerapitest.Start(t, workerapitest.Options{})
		r := startWorker(t, srv, builtins(nil), func(c *Config) { c.ShutdownGrace = 5 * time.Second })
		id := srv.Queue(t, "sleep", 60, `{"duration_ms":2000}`)[0]
		srv.WaitRunning(t, id)
		if err := r.stop(); err != nil {
			t.Fatalf("Run = %v, want nil (exit 0)", err)
		}
		wantAttempt(t, srv.WaitFinished(t, id), model.AttemptSucceeded, "", nil)
		if !deregistered(srv, r.w.cfg.ID) || srv.Calls("Deregister") != 1 {
			t.Error("worker did not deregister")
		}
	})
	t.Run("grace too short: cancelled and reported", func(t *testing.T) {
		t.Parallel()
		srv := workerapitest.Start(t, workerapitest.Options{})
		r := startWorker(t, srv, builtins(nil), func(c *Config) { c.ShutdownGrace = 100 * time.Millisecond })
		id := srv.Queue(t, "sleep", 60, `{"duration_ms":60000}`)[0]
		srv.WaitRunning(t, id)
		if err := r.stop(); err != nil {
			t.Fatalf("Run = %v", err)
		}
		wantAttempt(t, srv.WaitFinished(t, id), model.AttemptFailed, handlers.CodeWorkerShutdown, yes)
		if !deregistered(srv, r.w.cfg.ID) {
			t.Error("worker did not deregister")
		}
	})
}

func TestWorker_LogsShippedAndOverflowDropsOldest(t *testing.T) {
	t.Parallel()
	srv := workerapitest.Start(t, workerapitest.Options{})
	r := startWorker(t, srv, []handlers.Handler{fn{"chatty", func(ctx context.Context, tc handlers.TaskContext) (json.RawMessage, error) {
		for i := range 10 {
			tc.Logger.InfoContext(ctx, fmt.Sprintf("line %d", i), "i", i)
		}
		tc.Logger.WarnContext(ctx, "done")
		return nil, nil
	}}}, func(c *Config) { c.LogLines = 3 })
	id := srv.Queue(t, "chatty", 60, `{}`)[0]
	a := srv.WaitFinished(t, id)
	lines, err := (pg.TaskLogRepo{}).List(t.Context(), srv.Pool, a.ID, 0, 100) // what GET /attempts/{id}/logs serves
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, l := range lines {
		got = append(got, l.Level+" "+l.Message)
	}
	if want := []string{"INFO line 8", "INFO line 9", "WARN done"}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("shipped lines %v, want %v (the newest 3)", got, want)
	}
	if string(lines[0].Fields) != `{"i": 8}` {
		t.Errorf("fields %s, want {\"i\": 8}", lines[0].Fields)
	}
	if n := promtest.ToFloat64(r.metrics.logDrops); n != 8 {
		t.Errorf("dropped lines counted %v, want 8", n)
	}
}
