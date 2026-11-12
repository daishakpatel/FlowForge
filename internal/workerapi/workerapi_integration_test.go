//go:build integration

package workerapi

import (
	"context"
	crand "crypto/rand"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/daishakpatel/flowforge/internal/api/auth"
	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/config"
	"github.com/daishakpatel/flowforge/internal/events"
	workerv1 "github.com/daishakpatel/flowforge/internal/gen/proto/worker/v1"
	"github.com/daishakpatel/flowforge/internal/ids"
	"github.com/daishakpatel/flowforge/internal/obs"
	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/persistence/pg"
	"github.com/daishakpatel/flowforge/internal/testutil"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
)

type env struct {
	pool   *pgxpool.Pool
	svc    *Service
	client workerv1.WorkerServiceClient
	tm     *pg.TxManager
	runs   *pg.RunRepo
	tasks  *pg.TaskRepo
	defs   *pg.DefinitionRepo
	worker string // token of a worker-role key
	other  string // token of a second worker-role key
	viewer string // token of a viewer key
	seq    atomic.Int64
}

// newEnv serves a Service over an in-process gRPC connection with the production interceptors,
// against its own database. change may adjust the service before it starts.
func newEnv(t *testing.T, change ...func(*Service)) *env {
	t.Helper()
	db := testutil.NewFlowforgeDB(t)
	pool, err := pg.NewPool(t.Context(), config.Secret(db.DSN))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	gen := ids.NewGenerator(clock.Real{}, crand.Reader)
	rec := events.NewRecorder(clock.Real{}, gen, "test")
	e := &env{pool: pool, tm: pg.NewTxManager(pool, clock.Real{}, nil), runs: pg.NewRunRepo(gen, rec), tasks: pg.NewTaskRepo(rec), defs: pg.NewDefinitionRepo(gen)}
	e.worker, e.other, e.viewer = e.key(t, auth.RoleWorker), e.key(t, auth.RoleWorker), e.key(t, auth.RoleViewer)

	waker := NewWaker(func(ctx context.Context) (*pgx.Conn, error) { return pgx.Connect(ctx, db.DSN) }, clock.Real{})
	reg := obs.NewRegistry()
	stale, err := reg.Counter(prometheus.CounterOpts{Name: "flowforge_stale_attempt_rejections_total", Help: "test"})
	if err != nil {
		t.Fatal(err)
	}
	e.svc = &Service{
		DB: pool, Tx: e.tm, Tasks: e.tasks, Attempts: pg.NewAttemptRepo(gen, rec), Workers: pg.WorkerRepo{}, Logs: pg.TaskLogRepo{},
		Outcomes: TerminalOutcomeHandler{Tasks: e.tasks}, Cancels: NoCancels{},
		Leases: &PGLeaseProvider{DB: pool, Workers: pg.WorkerRepo{}, Clock: clock.Real{}, TTL: 15 * time.Second},
		Waker:  waker, Clock: clock.Real{}, LeaseTTL: 15 * time.Second, HeartbeatInterval: 5 * time.Second,
		StaleRejections: stale.WithLabelValues(),
	}
	for _, c := range change {
		c(e.svc)
	}
	ic, err := NewInterceptors(reg, auth.NewKeyAuthenticator(pool, pg.APIKeyRepo{}, clock.Real{}), gen, slog.New(slog.DiscardHandler), clock.Real{})
	if err != nil {
		t.Fatal(err)
	}
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(ic.Unary), grpc.ChainStreamInterceptor(ic.Stream))
	workerv1.RegisterWorkerServiceServer(srv, e.svc)
	ctx, cancel := context.WithCancel(context.Background())
	served, listened := make(chan struct{}), make(chan struct{})
	go func() { defer close(served); _ = srv.Serve(lis) }()
	go func() { defer close(listened); _ = waker.Run(ctx) }()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		srv.Stop()
		cancel()
		<-served
		<-listened
	})
	e.client = workerv1.NewWorkerServiceClient(conn)
	return e
}

func (e *env) key(t *testing.T, role string) string {
	t.Helper()
	k, err := auth.GenerateKey(crand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	_, err = pg.APIKeyRepo{}.Create(t.Context(), e.pool, persistence.NewAPIKey{ID: k.ID, Name: role, Role: role, SecretHash: k.Hash, Salt: k.Salt, CreatedBy: "t"})
	if err != nil {
		t.Fatal(err)
	}
	return k.Token
}

// as returns a context carrying token as the gRPC bearer credential.
func as(ctx context.Context, token string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
}

func (e *env) register(t *testing.T, id string, types, queues []string, capacity int32) {
	t.Helper()
	_, err := e.client.Register(as(t.Context(), e.worker), &workerv1.RegisterRequest{WorkerId: id, TaskTypes: types, Queues: queues, Capacity: capacity})
	if err != nil {
		t.Fatalf("register %s: %v", id, err)
	}
}

// queue creates a run with n independent tasks of typ in queue and moves them all to QUEUED.
// It returns the run id and the task ids.
func (e *env) queue(t *testing.T, n int, typ, queue string) (string, []string) {
	t.Helper()
	var defTasks []string
	for i := range n {
		defTasks = append(defTasks, fmt.Sprintf(`{"id":"t%03d","type":%q,"queue":%q}`, i, typ, queue))
	}
	raw := fmt.Sprintf(`{"name":"wf-%d","version":1,"tasks":[%s]}`, e.seq.Add(1), strings.Join(defTasks, ","))
	def, err := model.Decode([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	dv, err := e.defs.Create(t.Context(), e.pool, persistence.NewDefinition{Name: def.Name, Version: 1, Definition: []byte(raw), CreatedBy: "t"})
	if err != nil {
		t.Fatal(err)
	}
	var runID string
	var ids []string
	err = e.tm.WithTx(t.Context(), pgx.TxOptions{}, func(tx pgx.Tx) error {
		run, err := e.runs.CreateWithTasks(t.Context(), tx, persistence.NewRun{DefinitionID: dv.ID, CreatedBy: "t"}, def)
		if err != nil {
			return err
		}
		runID = run.ID
		tasks, err := e.tasks.ListByRun(t.Context(), tx, run.ID)
		if err != nil {
			return err
		}
		ids = ids[:0]
		for _, task := range tasks {
			for _, step := range [][2]model.TaskStatus{{model.TaskPending, model.TaskReady}, {model.TaskReady, model.TaskQueued}} {
				if err := e.tasks.Transition(t.Context(), tx, persistence.TaskTransitionParams{TaskID: task.ID, From: step[0], To: step[1]}); err != nil {
					return err
				}
			}
			ids = append(ids, task.ID)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return runID, ids
}

func (e *env) poll(t *testing.T, worker string, free, waitMS int32) []*workerv1.Task {
	t.Helper()
	resp, err := e.client.PollTask(as(t.Context(), e.worker), &workerv1.PollTaskRequest{WorkerId: worker, FreeCapacity: free, WaitMs: waitMS})
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	return resp.GetTasks()
}

func (e *env) scalar(t *testing.T, sql string, args ...any) string {
	t.Helper()
	var v string
	if err := e.pool.QueryRow(t.Context(), sql, args...).Scan(&v); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return v
}

// wantCode checks err's gRPC code and, when reason is set, its ErrorInfo reason.
func wantCode(t *testing.T, err error, code codes.Code, reason string) {
	t.Helper()
	st := status.Convert(err)
	if st.Code() != code {
		t.Fatalf("code = %v (%v), want %v", st.Code(), err, code)
	}
	if reason == "" {
		return
	}
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok && info.GetReason() == reason && info.GetDomain() == ErrorDomain {
			return
		}
	}
	t.Errorf("status %v lacks ErrorInfo reason %s", st, reason)
}

// TestClaim_ExactlyOnceUnderRace: 100 QUEUED tasks, 50 concurrent pollers with free_capacity 3.
func TestClaim_ExactlyOnceUnderRace(t *testing.T) {
	e := newEnv(t)
	_, taskIDs := e.queue(t, 100, "a", "q")
	for i := range 50 {
		e.register(t, fmt.Sprintf("w-%02d", i), []string{"a"}, []string{"q"}, 3)
	}
	var mu sync.Mutex
	claims := map[string]int{}
	var total atomic.Int64
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for i := range 50 {
		wg.Go(func() {
			for total.Load() < 100 && ctx.Err() == nil {
				resp, err := e.client.PollTask(as(ctx, e.worker), &workerv1.PollTaskRequest{WorkerId: fmt.Sprintf("w-%02d", i), FreeCapacity: 3, WaitMs: 1})
				if err != nil {
					if ctx.Err() == nil {
						t.Errorf("poll: %v", err)
					}
					return
				}
				if len(resp.GetTasks()) > 3 {
					t.Errorf("poller got %d tasks with free_capacity 3", len(resp.GetTasks()))
				}
				mu.Lock()
				for _, task := range resp.GetTasks() {
					claims[task.GetTaskId()]++
				}
				mu.Unlock()
				total.Add(int64(len(resp.GetTasks())))
			}
		})
	}
	wg.Wait()
	if len(claims) != 100 || total.Load() != 100 {
		t.Fatalf("claimed %d distinct tasks, %d claims; want 100 and 100", len(claims), total.Load())
	}
	for _, id := range taskIDs {
		if claims[id] != 1 {
			t.Errorf("task %s claimed %d times", id, claims[id])
		}
	}
	if n := e.scalar(t, `SELECT count(*)::text FROM task_attempts`); n != "100" {
		t.Errorf("attempt rows = %s, want 100", n)
	}
	if n := e.scalar(t, `SELECT count(*)::text FROM workflow_tasks WHERE status = 'RUNNING' AND attempt_count = 1`); n != "100" {
		t.Errorf("RUNNING tasks with one attempt = %s, want 100", n)
	}
}

func TestClaim_RoutingPausedAndCancelled(t *testing.T) {
	e := newEnv(t)
	_, want := e.queue(t, 1, "a", "q1")
	e.queue(t, 1, "b", "q1")
	e.queue(t, 1, "a", "q2")
	paused, _ := e.queue(t, 1, "a", "q1")
	cancelled, _ := e.queue(t, 1, "a", "q1")
	err := e.tm.WithTx(t.Context(), pgx.TxOptions{}, func(tx pgx.Tx) error {
		if err := e.runs.Transition(t.Context(), tx, persistence.RunTransitionParams{RunID: paused, From: model.RunCreated, To: model.RunRunning}); err != nil {
			return err
		}
		return e.runs.Transition(t.Context(), tx, persistence.RunTransitionParams{RunID: paused, From: model.RunRunning, To: model.RunPaused, Version: 1})
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.pool.Exec(t.Context(), `UPDATE workflow_runs SET cancel_requested = true WHERE id = $1`, cancelled); err != nil {
		t.Fatal(err)
	}
	e.register(t, "router", []string{"a"}, []string{"q1"}, 10)
	got := e.poll(t, "router", 10, 1)
	if len(got) != 1 || got[0].GetTaskId() != want[0] || got[0].GetTaskType() != "a" {
		t.Fatalf("claimed %v; want only the type a / queue q1 task of the active run", got)
	}
	if again := e.poll(t, "router", 10, 1); len(again) != 0 {
		t.Errorf("second poll claimed %d tasks (other type, other queue, paused, cancel-requested)", len(again))
	}
	task := got[0]
	if task.GetAttemptNumber() != 1 || task.GetIdempotencyKey() != task.GetTaskId()+":1" || task.GetFencingToken() <= 0 ||
		!task.GetDeadline().AsTime().After(time.Now()) || string(task.GetInputJson()) != "{}" { //nolint:forbidigo // compares with the database clock
		t.Errorf("task message = %+v", task)
	}
	if n := e.scalar(t, `SELECT count(*)::text FROM workflow_events WHERE event_type IN ('task.started','task.attempt.started')`); n != "2" {
		t.Errorf("claim events = %s, want task.started and task.attempt.started", n)
	}
}

func TestReports_FencingInSQL(t *testing.T) {
	e := newEnv(t)
	e.queue(t, 4, "a", "q")
	e.register(t, "fencer", []string{"a"}, []string{"q"}, 4)
	tasks := e.poll(t, "fencer", 4, 1)
	if len(tasks) != 4 {
		t.Fatalf("claimed %d, want 4", len(tasks))
	}
	ctx := as(t.Context(), e.worker)
	t0, t1, t2 := tasks[0], tasks[1], tasks[2]

	_, err := e.client.CompleteTask(ctx, &workerv1.CompleteTaskRequest{AttemptId: t0.GetAttemptId(), FencingToken: t0.GetFencingToken() + 1})
	wantCode(t, err, codes.FailedPrecondition, "STALE_ATTEMPT")
	if _, err := e.client.CompleteTask(ctx, &workerv1.CompleteTaskRequest{AttemptId: t0.GetAttemptId(), FencingToken: t0.GetFencingToken(), ResultJson: []byte(`{"rows":3}`)}); err != nil {
		t.Fatal(err)
	}
	_, err = e.client.CompleteTask(ctx, &workerv1.CompleteTaskRequest{AttemptId: t0.GetAttemptId(), FencingToken: t0.GetFencingToken()})
	wantCode(t, err, codes.FailedPrecondition, "STALE_ATTEMPT")

	// The server gave up on t1 (as recovery will): the late report is rejected.
	err = e.tm.WithTx(t.Context(), pgx.TxOptions{}, func(tx pgx.Tx) error {
		return e.svc.Attempts.Transition(t.Context(), tx, persistence.AttemptTransitionParams{
			AttemptID: t1.GetAttemptId(), From: model.AttemptRunning, To: model.AttemptAbandoned, FencingToken: t1.GetFencingToken(),
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.client.CompleteTask(ctx, &workerv1.CompleteTaskRequest{AttemptId: t1.GetAttemptId(), FencingToken: t1.GetFencingToken()})
	wantCode(t, err, codes.FailedPrecondition, "STALE_ATTEMPT")

	if _, err := e.client.FailTask(ctx, &workerv1.FailTaskRequest{AttemptId: t2.GetAttemptId(), FencingToken: t2.GetFencingToken(), ErrorCode: "BOOM", Message: "password=hunter2 failed", Retryable: true}); err != nil {
		t.Fatal(err)
	}
	_, err = e.client.FailTask(ctx, &workerv1.FailTaskRequest{AttemptId: "att_nope", FencingToken: 1, ErrorCode: "X"})
	wantCode(t, err, codes.NotFound, "NOT_FOUND")

	if got := promtest.ToFloat64(e.svc.StaleRejections); got != 3 {
		t.Errorf("stale rejections = %v, want 3", got)
	}
	status := func(id string) string { return e.scalar(t, `SELECT status FROM workflow_tasks WHERE id = $1`, id) }
	if status(t0.GetTaskId()) != "SUCCEEDED" || status(t2.GetTaskId()) != "FAILED" || status(t1.GetTaskId()) != "RUNNING" {
		t.Errorf("task statuses = %s %s %s", status(t0.GetTaskId()), status(t1.GetTaskId()), status(t2.GetTaskId()))
	}
	if r := e.scalar(t, `SELECT result::text FROM workflow_tasks WHERE id = $1`, t0.GetTaskId()); r != `{"rows": 3}` {
		t.Errorf("task result = %s", r)
	}
	if m := e.scalar(t, `SELECT error_message FROM task_attempts WHERE id = $1`, t2.GetAttemptId()); strings.Contains(m, "hunter2") {
		t.Errorf("failure message stored unredacted: %q", m)
	}
}

func TestCompleteTask_ResultTooLarge(t *testing.T) {
	e := newEnv(t)
	e.queue(t, 1, "a", "q")
	e.register(t, "big", []string{"a"}, []string{"q"}, 1)
	task := e.poll(t, "big", 1, 1)[0]
	result := []byte(`"` + strings.Repeat("x", MaxResultBytes-1) + `"`) // 256 KiB + 1
	_, err := e.client.CompleteTask(as(t.Context(), e.worker), &workerv1.CompleteTaskRequest{AttemptId: task.GetAttemptId(), FencingToken: task.GetFencingToken(), ResultJson: result})
	wantCode(t, err, codes.ResourceExhausted, "PAYLOAD_TOO_LARGE")
	row := e.scalar(t, `SELECT status || ' ' || error_code || ' ' || retryable::text FROM task_attempts WHERE id = $1`, task.GetAttemptId())
	if row != "FAILED RESULT_TOO_LARGE false" || e.scalar(t, `SELECT status FROM workflow_tasks WHERE id = $1`, task.GetTaskId()) != "FAILED" {
		t.Errorf("attempt = %q; want FAILED RESULT_TOO_LARGE false and the task FAILED", row)
	}
}

func TestPollTask_LongPoll(t *testing.T) {
	e := newEnv(t)
	e.register(t, "longpoll", []string{"a"}, []string{"q"}, 1)

	start := time.Now() //nolint:forbidigo // measures real latency
	if got := e.poll(t, "longpoll", 1, 300); len(got) != 0 {
		t.Fatalf("empty queue returned %d tasks", len(got))
	}
	if d := time.Since(start); d < 300*time.Millisecond || d > 1500*time.Millisecond { //nolint:forbidigo // see above
		t.Errorf("empty poll returned after %v, want about wait_ms (300ms)", d)
	}

	type result struct {
		tasks []*workerv1.Task
		at    time.Time
	}
	done := make(chan result, 1)
	go func() {
		resp, err := e.client.PollTask(as(context.Background(), e.worker), &workerv1.PollTaskRequest{WorkerId: "longpoll", FreeCapacity: 1, WaitMs: 10000})
		if err != nil {
			t.Errorf("long poll: %v", err)
		}
		done <- result{resp.GetTasks(), time.Now()} //nolint:forbidigo // see above
	}()
	time.Sleep(200 * time.Millisecond) //nolint:forbidigo // let the poll start waiting first
	e.queue(t, 1, "a", "q")
	if _, err := e.pool.Exec(t.Context(), "NOTIFY "+TaskQueuedChannel); err != nil {
		t.Fatal(err)
	}
	notified := time.Now() //nolint:forbidigo // see above
	r := <-done
	if len(r.tasks) != 1 || r.at.Sub(notified) > 200*time.Millisecond {
		t.Errorf("long poll returned %d tasks %v after the NOTIFY, want 1 within 200ms", len(r.tasks), r.at.Sub(notified))
	}

	ctx, cancel := context.WithCancel(as(context.Background(), e.worker))
	go func() { time.Sleep(100 * time.Millisecond); cancel() }() //nolint:forbidigo // cancel while waiting
	start = time.Now()                                           //nolint:forbidigo // see above
	_, err := e.client.PollTask(ctx, &workerv1.PollTaskRequest{WorkerId: "longpoll", FreeCapacity: 1, WaitMs: 10000})
	wantCode(t, err, codes.Canceled, "")
	if d := time.Since(start); d > time.Second { //nolint:forbidigo // see above
		t.Errorf("cancelled poll took %v to return", d)
	}
}

type stubCancels struct{ ids []string }

func (s stubCancels) CancelledAttempts(_ context.Context, live []string) ([]string, error) {
	var out []string
	for _, id := range live {
		for _, c := range s.ids {
			if id == c {
				out = append(out, id)
			}
		}
	}
	return out, nil
}

func TestHeartbeat_StaleAndCancelled(t *testing.T) {
	var cancelIDs []string
	e := newEnv(t, func(s *Service) { s.Cancels = stubCancelsFunc(&cancelIDs) })
	e.queue(t, 2, "a", "q")
	e.register(t, "beater", []string{"a"}, []string{"q"}, 2)
	tasks := e.poll(t, "beater", 2, 1)
	cancelIDs = []string{tasks[0].GetAttemptId()}
	resp, err := e.client.Heartbeat(as(t.Context(), e.worker), &workerv1.HeartbeatRequest{WorkerId: "beater", Running: []*workerv1.RunningAttempt{
		{AttemptId: tasks[0].GetAttemptId(), FencingToken: tasks[0].GetFencingToken()},
		{AttemptId: tasks[1].GetAttemptId(), FencingToken: tasks[1].GetFencingToken() + 1},
		{AttemptId: "att_nope", FencingToken: 1},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(resp.GetStaleAttemptIds()) != fmt.Sprint([]string{tasks[1].GetAttemptId(), "att_nope"}) {
		t.Errorf("stale = %v", resp.GetStaleAttemptIds())
	}
	if fmt.Sprint(resp.GetCancelAttemptIds()) != fmt.Sprint(cancelIDs) || resp.GetLeaseTtlMs() != 15000 || !resp.GetServerTime().IsValid() {
		t.Errorf("heartbeat response = %+v", resp)
	}
	if v := e.scalar(t, `SELECT (last_heartbeat_at IS NOT NULL)::text FROM task_attempts WHERE id = $1`, tasks[0].GetAttemptId()); v != "true" {
		t.Error("last_heartbeat_at not recorded for the live attempt")
	}
	if v := e.scalar(t, `SELECT active_tasks::text FROM worker_observations WHERE worker_id = 'beater'`); v != "1" {
		t.Errorf("active_tasks = %s, want 1", v)
	}
}

func stubCancelsFunc(ids *[]string) CancelSource { return stubCancelsPtr{ids} }

type stubCancelsPtr struct{ ids *[]string }

func (s stubCancelsPtr) CancelledAttempts(ctx context.Context, live []string) ([]string, error) {
	return stubCancels{*s.ids}.CancelledAttempts(ctx, live)
}

func TestShipLogs_RedactTruncateCapAndAuthorize(t *testing.T) {
	e := newEnv(t)
	e.queue(t, 1, "a", "q")
	e.register(t, "logger", []string{"a"}, []string{"q"}, 1)
	task := e.poll(t, "logger", 1, 1)[0]
	stream, err := e.client.ShipLogs(as(t.Context(), e.worker))
	if err != nil {
		t.Fatal(err)
	}
	line := func(msg string) *workerv1.LogLine {
		return &workerv1.LogLine{AttemptId: task.GetAttemptId(), FencingToken: task.GetFencingToken(), Level: "info", Message: msg}
	}
	secret := "ff_abcdefghijkl_" + strings.Repeat("s", 43)
	first := line("connecting with " + secret)
	first.FieldsJson = []byte(`{"password":"hunter2","rows":3}`)
	if err := stream.Send(first); err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(line(strings.Repeat("y", MaxLogLineBytes+100))); err != nil {
		t.Fatal(err)
	}
	for i := range MaxLogLinesPerAttempt { // lines 3..1002: 998 stored, the 1001st is the marker, the rest dropped
		if err := stream.Send(line(fmt.Sprintf("line %d", i))); err != nil {
			t.Fatal(err)
		}
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetAccepted() != MaxLogLinesPerAttempt || resp.GetDropped() != 2 {
		t.Errorf("accepted %d dropped %d, want %d and 2", resp.GetAccepted(), resp.GetDropped(), MaxLogLinesPerAttempt)
	}
	rows, err := pg.TaskLogRepo{}.List(t.Context(), e.pool, task.GetAttemptId(), 0, 200)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(rows[0].Message, secret) || strings.Contains(string(rows[0].Fields), "hunter2") || rows[0].Level != "INFO" {
		t.Errorf("first line not redacted/normalized: %+v", rows[0])
	}
	if len(rows[1].Message) > MaxLogLineBytes || !strings.HasSuffix(rows[1].Message, "…[truncated]") {
		t.Errorf("long line = %d bytes, suffix %q", len(rows[1].Message), rows[1].Message[len(rows[1].Message)-20:])
	}
	if n := e.scalar(t, `SELECT count(*)::text FROM task_logs WHERE attempt_id = $1`, task.GetAttemptId()); n != "1001" {
		t.Errorf("stored lines = %s, want 1001 (1000 + marker)", n)
	}
	if m := e.scalar(t, `SELECT level || ' ' || message FROM task_logs WHERE attempt_id = $1 ORDER BY id DESC LIMIT 1`, task.GetAttemptId()); m != "WARN log limit reached" {
		t.Errorf("last line = %q, want the limit marker", m)
	}

	bad, err := e.client.ShipLogs(as(t.Context(), e.worker))
	if err != nil {
		t.Fatal(err)
	}
	_ = bad.Send(&workerv1.LogLine{AttemptId: task.GetAttemptId(), FencingToken: task.GetFencingToken() + 1, Message: "x"})
	_, err = bad.CloseAndRecv()
	wantCode(t, err, codes.FailedPrecondition, "STALE_ATTEMPT")

	foreign, err := e.client.ShipLogs(as(t.Context(), e.other))
	if err != nil {
		t.Fatal(err)
	}
	_ = foreign.Send(line("not mine"))
	_, err = foreign.CloseAndRecv()
	wantCode(t, err, codes.PermissionDenied, "PERMISSION_DENIED")
}

func TestAuthAndRegistration(t *testing.T) {
	e := newEnv(t)
	req := &workerv1.RegisterRequest{WorkerId: "solo", TaskTypes: []string{"a"}, Queues: []string{"q"}, Capacity: 1}
	_, err := e.client.Register(t.Context(), req)
	wantCode(t, err, codes.Unauthenticated, "UNAUTHENTICATED")
	_, err = e.client.Register(as(t.Context(), "ff_garbage"), req)
	wantCode(t, err, codes.Unauthenticated, "UNAUTHENTICATED")
	_, err = e.client.Register(as(t.Context(), e.viewer), req)
	wantCode(t, err, codes.PermissionDenied, "PERMISSION_DENIED")
	logs, err := e.client.ShipLogs(as(t.Context(), e.viewer))
	if err == nil {
		_, err = logs.CloseAndRecv()
	}
	wantCode(t, err, codes.PermissionDenied, "PERMISSION_DENIED")

	_, err = e.client.Register(as(t.Context(), e.worker), &workerv1.RegisterRequest{WorkerId: "X!", TaskTypes: []string{"a"}, Queues: []string{"q"}, Capacity: 1})
	wantCode(t, err, codes.InvalidArgument, "INVALID_ARGUMENT")
	_, err = e.client.PollTask(as(t.Context(), e.worker), &workerv1.PollTaskRequest{WorkerId: "solo", FreeCapacity: 1, WaitMs: 1})
	wantCode(t, err, codes.FailedPrecondition, "INVALID_STATE")

	e.register(t, "solo", []string{"a"}, []string{"q"}, 1)
	_, err = e.client.Register(as(t.Context(), e.other), req)
	wantCode(t, err, codes.AlreadyExists, "CONFLICT")
	_, err = e.client.PollTask(as(t.Context(), e.other), &workerv1.PollTaskRequest{WorkerId: "solo", FreeCapacity: 1, WaitMs: 1})
	wantCode(t, err, codes.PermissionDenied, "PERMISSION_DENIED")
	if _, err := e.client.Deregister(as(t.Context(), e.worker), &workerv1.DeregisterRequest{WorkerId: "solo"}); err != nil {
		t.Fatal(err)
	}
	_, err = e.client.Heartbeat(as(t.Context(), e.worker), &workerv1.HeartbeatRequest{WorkerId: "solo"})
	wantCode(t, err, codes.FailedPrecondition, "INVALID_STATE")
	if _, err := e.client.Register(as(t.Context(), e.other), req); err != nil {
		t.Errorf("re-registering a deregistered id under another key = %v, want ok", err)
	}
}

func TestPGLeaseProvider_ExpiryAndAlive(t *testing.T) {
	e := newEnv(t)
	clk := clock.NewFake(time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC))
	lp := &PGLeaseProvider{DB: e.pool, Workers: pg.WorkerRepo{}, Clock: clk, TTL: 15 * time.Second}
	ctx := t.Context()
	for _, id := range []string{"keep", "lose"} {
		if err := lp.Register(ctx, persistence.WorkerRegistration{WorkerID: id, KeyID: "k1", TaskTypes: []string{"a"}, Queues: []string{"q"}, Capacity: 1}); err != nil {
			t.Fatal(err)
		}
	}
	clk.Advance(10 * time.Second)
	if err := lp.Renew(ctx, "keep", "k1", 0); err != nil {
		t.Fatal(err)
	}
	clk.Advance(10 * time.Second)
	expired, err := lp.Expired(ctx)
	if err != nil || fmt.Sprint(expired) != "[lose]" {
		t.Errorf("Expired = %v, %v; want [lose]", expired, err)
	}
	alive, err := lp.Alive(ctx)
	if err != nil || len(alive) != 1 || alive[0].WorkerID != "keep" {
		t.Errorf("Alive = %+v, %v; want keep", alive, err)
	}
	if err := lp.Register(ctx, persistence.WorkerRegistration{WorkerID: "keep", KeyID: "k2", TaskTypes: []string{"a"}, Queues: []string{"q"}, Capacity: 1}); err == nil {
		t.Error("live id registered by another key, want CONFLICT")
	}
	if err := lp.Register(ctx, persistence.WorkerRegistration{WorkerID: "lose", KeyID: "k2", TaskTypes: []string{"a"}, Queues: []string{"q"}, Capacity: 1}); err != nil {
		t.Errorf("expired id taken over by another key = %v, want ok", err)
	}
	if err := lp.Renew(ctx, "nobody", "k1", 0); err == nil {
		t.Error("renewing an unknown worker succeeded")
	}
}
