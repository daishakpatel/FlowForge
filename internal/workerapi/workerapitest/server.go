// Package workerapitest serves a real WorkerService (task 03.01) for worker tests (03.03, 03.09):
// the production interceptors, repositories and LISTEN waker over a freshly migrated database, on
// a loopback TCP address that survives Restart, so a test can take the API down and bring it
// back. It counts the RPCs it served per method.
package workerapitest

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
	"google.golang.org/grpc"

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
	"github.com/daishakpatel/flowforge/internal/workerapi"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
)

// Options adjust the served WorkerService. Zero values take the defaults noted.
type Options struct {
	HeartbeatInterval time.Duration          // returned by Register; 200 ms
	LeaseTTL          time.Duration          // 15 s
	Cancels           workerapi.CancelSource // NoCancels
}

// Server is a running WorkerService plus the repositories to set up and inspect its database.
type Server struct {
	Pool     *pgxpool.Pool
	TM       *pg.TxManager
	Runs     *pg.RunRepo
	Tasks    *pg.TaskRepo
	Attempts *pg.AttemptRepo
	Defs     *pg.DefinitionRepo
	Service  *workerapi.Service
	Addr     string // host:port, stable across Restart
	Token    string // an API key with role worker

	ic    *workerapi.Interceptors
	calls sync.Map // method name → *atomic.Int64
	seq   atomic.Int64

	mu     sync.Mutex
	srv    *grpc.Server
	served chan struct{}
}

// Start serves a WorkerService on 127.0.0.1 against a new migrated database. Everything stops
// in t.Cleanup.
func Start(t testing.TB, o Options) *Server {
	t.Helper()
	db := testutil.NewFlowforgeDB(t)
	pool, err := pg.NewPool(t.Context(), config.Secret(db.DSN))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	gen := ids.NewGenerator(clock.Real{}, crand.Reader)
	rec := events.NewRecorder(clock.Real{}, gen, "workerapitest")
	s := &Server{
		Pool: pool, TM: pg.NewTxManager(pool, clock.Real{}, nil), Runs: pg.NewRunRepo(gen, rec),
		Tasks: pg.NewTaskRepo(rec), Attempts: pg.NewAttemptRepo(gen, rec), Defs: pg.NewDefinitionRepo(gen),
	}
	s.Token = s.key(t, auth.RoleWorker)

	waker := workerapi.NewWaker(func(ctx context.Context) (*pgx.Conn, error) { return pgx.Connect(ctx, db.DSN) }, clock.Real{})
	wctx, stopWaker := context.WithCancel(context.Background())
	wakerDone := make(chan struct{})
	go func() { defer close(wakerDone); _ = waker.Run(wctx) }() // returns nil once wctx ends
	t.Cleanup(func() { stopWaker(); <-wakerDone })

	reg := obs.NewRegistry()
	stale, err := reg.Counter(prometheus.CounterOpts{Name: "flowforge_stale_attempt_rejections_total", Help: "test"})
	if err != nil {
		t.Fatal(err)
	}
	cancels := o.Cancels
	if cancels == nil {
		cancels = workerapi.NoCancels{}
	}
	ttl := orDefault(o.LeaseTTL, 15*time.Second)
	s.Service = &workerapi.Service{
		DB: pool, Tx: s.TM, Tasks: s.Tasks, Attempts: s.Attempts, Workers: pg.WorkerRepo{}, Logs: pg.TaskLogRepo{},
		Outcomes: workerapi.TerminalOutcomeHandler{Tasks: s.Tasks}, Cancels: cancels,
		Leases: &workerapi.PGLeaseProvider{DB: pool, Workers: pg.WorkerRepo{}, Clock: clock.Real{}, TTL: ttl},
		Waker:  waker, Wake: pg.SchedulerRepo{}, Clock: clock.Real{}, LeaseTTL: ttl,
		HeartbeatInterval: orDefault(o.HeartbeatInterval, 200*time.Millisecond), StaleRejections: stale.WithLabelValues(),
	}
	if s.ic, err = workerapi.NewInterceptors(reg, auth.NewKeyAuthenticator(pool, pg.APIKeyRepo{}, clock.Real{}), gen,
		slog.New(slog.DiscardHandler), clock.Real{}); err != nil {
		t.Fatal(err)
	}
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s.Addr = ln.Addr().String()
	s.serve(ln)
	t.Cleanup(s.Stop)
	return s
}

func orDefault(d, def time.Duration) time.Duration {
	if d > 0 {
		return d
	}
	return def
}

func (s *Server) serve(ln net.Listener) {
	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(s.ic.Unary, s.count), grpc.ChainStreamInterceptor(s.ic.Stream, s.countStream))
	workerv1.RegisterWorkerServiceServer(srv, s.Service)
	served := make(chan struct{})
	go func() { defer close(served); _ = srv.Serve(ln) }() // Serve returns once Stop is called
	s.mu.Lock()
	s.srv, s.served = srv, served
	s.mu.Unlock()
}

// Stop takes the API down: open calls fail and new ones are refused until Restart.
func (s *Server) Stop() {
	s.mu.Lock()
	srv, served := s.srv, s.served
	s.srv = nil
	s.mu.Unlock()
	if srv != nil {
		srv.Stop()
		<-served
	}
}

// Restart stops the API if it is up and serves it again on the same address, keeping the
// database: as an API process restart looks to a worker.
func (s *Server) Restart(t testing.TB) {
	t.Helper()
	s.Stop()
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", s.Addr)
	if err != nil {
		t.Fatalf("listen again on %s: %v", s.Addr, err)
	}
	s.serve(ln)
}

// Calls returns how many calls of method ("Register", "CompleteTask", …) the API has served.
func (s *Server) Calls(method string) int64 {
	if c, ok := s.calls.Load(method); ok {
		return c.(*atomic.Int64).Load()
	}
	return 0
}

func (s *Server) bump(fullMethod string) {
	name := fullMethod[strings.LastIndex(fullMethod, "/")+1:]
	c, _ := s.calls.LoadOrStore(name, new(atomic.Int64))
	c.(*atomic.Int64).Add(1)
}

func (s *Server) count(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
	s.bump(info.FullMethod)
	return h(ctx, req)
}

func (s *Server) countStream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, h grpc.StreamHandler) error {
	s.bump(info.FullMethod)
	return h(srv, ss)
}

func (s *Server) key(t testing.TB, role string) string {
	t.Helper()
	k, err := auth.GenerateKey(crand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := (pg.APIKeyRepo{}).Create(t.Context(), s.Pool, persistence.NewAPIKey{
		ID: k.ID, Name: role, Role: role, SecretHash: k.Hash, Salt: k.Salt, CreatedBy: "workerapitest",
	}); err != nil {
		t.Fatal(err)
	}
	return k.Token
}

// Queue starts a run with one independent task of typ per input (task timeout timeoutSeconds),
// moves each to QUEUED with that input as it would be resolved, and wakes long polls. It returns
// the task ids in input order.
func (s *Server) Queue(t testing.TB, typ string, timeoutSeconds int, inputs ...string) []string {
	t.Helper()
	parts := make([]string, len(inputs))
	for i := range inputs {
		parts[i] = fmt.Sprintf(`{"id":"t%03d","type":%q,"timeout_seconds":%d}`, i, typ, timeoutSeconds)
	}
	raw := fmt.Sprintf(`{"name":"wf-%d","version":1,"tasks":[%s]}`, s.seq.Add(1), strings.Join(parts, ","))
	def, err := model.Decode([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	dv, err := s.Defs.Create(t.Context(), s.Pool, persistence.NewDefinition{Name: def.Name, Version: 1, Definition: []byte(raw), CreatedBy: "t"})
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	err = s.TM.WithTx(t.Context(), pgx.TxOptions{}, func(tx pgx.Tx) error {
		ids = nil
		run, err := s.Runs.CreateWithTasks(t.Context(), tx, persistence.NewRun{DefinitionID: dv.ID, CreatedBy: "t"}, def)
		if err != nil {
			return err
		}
		tasks, err := s.Tasks.ListByRun(t.Context(), tx, run.ID)
		if err != nil {
			return err
		}
		for i, task := range tasks { // insertion order is definition order here: no dependencies
			input := []byte(inputs[i])
			for _, p := range []persistence.TaskTransitionParams{
				{TaskID: task.ID, From: model.TaskPending, To: model.TaskReady},
				{TaskID: task.ID, From: model.TaskReady, To: model.TaskQueued, Mutate: func(m *persistence.TaskMutation) { m.Input = input }},
			} {
				if err := s.Tasks.Transition(t.Context(), tx, p); err != nil {
					return err
				}
			}
			ids = append(ids, task.ID)
		}
		return pg.SchedulerRepo{}.Notify(t.Context(), tx, persistence.TaskQueuedChannel)
	})
	if err != nil {
		t.Fatal(err)
	}
	return ids
}

// Attempt returns the latest attempt of a task, if it has one.
func (s *Server) Attempt(t testing.TB, taskID string) (persistence.AttemptView, bool) {
	t.Helper()
	list, err := s.Attempts.ListByTask(t.Context(), s.Pool, taskID)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) == 0 {
		return persistence.AttemptView{}, false
	}
	return list[len(list)-1], true
}

// WaitRunning waits until the task has a RUNNING attempt and returns it.
func (s *Server) WaitRunning(t testing.TB, taskID string) persistence.AttemptView {
	t.Helper()
	var a persistence.AttemptView
	testutil.WaitFor(t, func() bool {
		var ok bool
		a, ok = s.Attempt(t, taskID)
		return ok && a.Status == model.AttemptRunning
	}, 15*time.Second)
	return a
}

// WaitFinished waits until the task's latest attempt has left RUNNING and returns it.
func (s *Server) WaitFinished(t testing.TB, taskID string) persistence.AttemptView {
	t.Helper()
	var a persistence.AttemptView
	testutil.WaitFor(t, func() bool {
		var ok bool
		a, ok = s.Attempt(t, taskID)
		return ok && a.Status != model.AttemptRunning
	}, 30*time.Second)
	return a
}

// Scalar returns one value of a test-only query as text.
func (s *Server) Scalar(t testing.TB, sql string, args ...any) string {
	t.Helper()
	var v any
	if err := s.Pool.QueryRow(t.Context(), sql, args...).Scan(&v); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return fmt.Sprint(v)
}
