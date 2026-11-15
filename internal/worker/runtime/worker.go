// Package runtime is flowforge-worker's engine (task 03.03, ADR-004, ADR-007, ADR-021): it
// registers with flowforge-api, long-polls for tasks within its free capacity, runs each attempt
// in its own goroutine through the handlers registry, heartbeats the running attempts, reports
// results with the attempt's fencing token, ships attempt logs, and shuts down gracefully.
//
// Goroutines and their owners (all joined before Run returns):
//   - Run: the poll loop and the heartbeat loop;
//   - one goroutine per attempt, tracked by Worker.attempts.
//
// Heartbeats never take a lock a handler holds: handlers only write to their own log buffer,
// and the running-attempt map is locked for snapshots and updates only.
package runtime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	workerv1 "github.com/daishakpatel/flowforge/internal/gen/proto/worker/v1"

	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/worker/handlers"
)

// Config is a worker's identity and timing. Zero durations take the defaults noted.
type Config struct {
	ID       string   // FF_WORKER_ID
	Queues   []string // FF_WORKER_QUEUES
	Capacity int      // FF_WORKER_CAPACITY: attempts run at once
	Version  string   // reported in Register
	// Heartbeat is used until Register returns the API's interval (FF_WORKER_HEARTBEAT); 5 s.
	Heartbeat time.Duration
	// ShutdownGrace is how long running attempts may finish after shutdown starts before they
	// are cancelled (FF_WORKER_SHUTDOWN_GRACE); 30 s.
	ShutdownGrace time.Duration
	PollWait      time.Duration // PollTask wait_ms; 10 s
	RPCTimeout    time.Duration // per unary call; 10 s
	LogLines      int           // log lines kept per attempt, oldest dropped first; 1000
}

// Deps are the worker's collaborators.
type Deps struct {
	Client   workerv1.WorkerServiceClient
	Registry *handlers.Registry
	Clock    clock.Clock
	Logger   *slog.Logger
	Metrics  *Metrics   // optional
	Rand     *rand.Rand // backoff jitter
}

// Cancellation causes of an attempt's context; each decides how the outcome is reported.
var (
	errServerCancel = errors.New("cancelled by the server")  // report CANCELLED, not retryable
	errStale        = errors.New("attempt is stale")         // drop the result, report nothing
	errShutdown     = errors.New("worker shutting down")     // report WORKER_SHUTDOWN, retryable
	errReportCutoff = errors.New("worker stopped reporting") // ends report retries at shutdown
)

const (
	deadlineMargin = time.Second      // handlers stop this long before the server's deadline
	reportGrace    = 10 * time.Second // after cancelling at shutdown, time left for reports
)

// Worker is one flowforge-worker instance. Run it once.
type Worker struct {
	cfg     Config
	d       Deps
	backoff *backoffPolicy

	regMu        sync.Mutex  // serializes Register calls from the poll and heartbeat loops
	needRegister atomic.Bool // set when the API stopped knowing us or was unreachable
	beatEvery    atomic.Int64

	mu       sync.Mutex
	running  map[string]*attempt // attempt id → attempt
	freed    chan struct{}       // signalled (non-blocking) when an attempt ends
	attempts sync.WaitGroup

	reportCtx context.Context // bounds report retries; cancelled reportGrace after shutdown cancels
	stop      context.CancelCauseFunc
}

// New returns a Worker. cfg.ID, Queues and Capacity must be set; Deps.Registry must hold at
// least one handler.
func New(cfg Config, d Deps) *Worker {
	cfg.Heartbeat = orDefault(cfg.Heartbeat, 5*time.Second)
	cfg.ShutdownGrace = orDefault(cfg.ShutdownGrace, 30*time.Second)
	cfg.PollWait = orDefault(cfg.PollWait, 10*time.Second)
	cfg.RPCTimeout = orDefault(cfg.RPCTimeout, 10*time.Second)
	if cfg.LogLines <= 0 {
		cfg.LogLines = 1000
	}
	w := &Worker{
		cfg: cfg, d: d, running: map[string]*attempt{}, freed: make(chan struct{}, 1),
		backoff: &backoffPolicy{base: 100 * time.Millisecond, maxDelay: 5 * time.Second, rng: d.Rand},
	}
	w.beatEvery.Store(int64(cfg.Heartbeat))
	return w
}

func orDefault(d, def time.Duration) time.Duration {
	if d > 0 {
		return d
	}
	return def
}

// Run registers, then polls, executes and heartbeats until ctx ends, then shuts down: it stops
// polling, lets running attempts finish for ShutdownGrace, cancels the rest (reported as
// WORKER_SHUTDOWN), stops heartbeating and deregisters. It returns nil after a clean shutdown,
// or the error that kept the first registration from succeeding (bad key, invalid config).
func (w *Worker) Run(ctx context.Context) error {
	if err := w.registerFirst(ctx); err != nil {
		return err
	}
	keep := context.WithoutCancel(ctx) // attempts, heartbeats and reports outlive ctx
	w.reportCtx, w.stop = context.WithCancelCause(keep)
	defer w.stop(errReportCutoff)
	beatCtx, stopBeats := context.WithCancel(keep)
	polled, beaten := make(chan struct{}), make(chan struct{})
	go func() { defer close(polled); w.pollLoop(ctx) }()
	go func() { defer close(beaten); w.heartbeatLoop(beatCtx) }()
	<-ctx.Done()
	w.d.Logger.Info("shutting down", "running", w.activeCount(), "grace", w.cfg.ShutdownGrace)
	<-polled // no attempt starts after this, so drain's Wait cannot race a start
	w.drain()
	stopBeats() // heartbeats kept the attempts alive through the grace period
	<-beaten
	w.deregister(keep)
	return nil
}

// registerFirst registers, retrying while the API is unreachable. A rejection (bad key,
// invalid registration, the id held by another key) is returned: retrying cannot fix it.
func (w *Worker) registerFirst(ctx context.Context) error {
	for n := 0; ; n++ {
		err := w.register(ctx)
		if err == nil {
			return nil
		}
		if !transient(err) {
			return fmt.Errorf("register worker %s: %w", w.cfg.ID, err)
		}
		w.d.Logger.Warn("API unreachable; retrying registration", "err", err)
		if err := w.d.Clock.Sleep(ctx, w.backoff.delay(n)); err != nil {
			return fmt.Errorf("register worker %s: %w", w.cfg.ID, err)
		}
	}
}

// register calls Register and adopts the API's heartbeat interval.
func (w *Worker) register(ctx context.Context) error {
	w.regMu.Lock()
	defer w.regMu.Unlock()
	rctx, cancel := context.WithTimeout(ctx, w.cfg.RPCTimeout)
	defer cancel()
	resp, err := w.d.Client.Register(rctx, &workerv1.RegisterRequest{
		WorkerId: w.cfg.ID, TaskTypes: w.d.Registry.Types(), Queues: w.cfg.Queues,
		Capacity: int32(min(w.cfg.Capacity, 1000)), Version: w.cfg.Version, //nolint:gosec // G115: capped at the API's 1000
	})
	if err != nil {
		return err
	}
	if ms := resp.GetHeartbeatIntervalMs(); ms > 0 {
		w.beatEvery.Store(int64(time.Duration(ms) * time.Millisecond))
	}
	w.needRegister.Store(false)
	w.d.Logger.Info("registered", "worker_id", w.cfg.ID, "types", w.d.Registry.Types(), "queues", w.cfg.Queues)
	return nil
}

// ensureRegistered registers again when a call showed it is needed, and reports whether the
// worker is registered. On failure it backs off for failures n, so callers can just loop.
func (w *Worker) ensureRegistered(ctx context.Context, n int) bool {
	if !w.needRegister.Load() {
		return true
	}
	if err := w.register(ctx); err != nil {
		w.d.Logger.Warn("re-registration failed", "err", err)
		_ = w.d.Clock.Sleep(ctx, w.backoff.delay(n)) // a cancelled ctx ends the caller's loop
		return false
	}
	return true
}

// noteCallError flags re-registration after an error that may mean the API lost or restarted.
func (w *Worker) noteCallError(err error) {
	if unregistered(err) || transient(err) {
		w.needRegister.Store(true)
	}
}

func (w *Worker) deregister(ctx context.Context) {
	rctx, cancel := context.WithTimeout(ctx, w.cfg.RPCTimeout)
	defer cancel()
	if _, err := w.d.Client.Deregister(rctx, &workerv1.DeregisterRequest{WorkerId: w.cfg.ID}); err != nil {
		w.d.Logger.Warn("deregister failed; the lease will expire", "err", err)
		return
	}
	w.d.Logger.Info("deregistered", "worker_id", w.cfg.ID)
}

// activeCount is the number of attempts running now.
func (w *Worker) activeCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.running)
}

// snapshot returns the running attempts for a heartbeat.
func (w *Worker) snapshot() []*workerv1.RunningAttempt {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]*workerv1.RunningAttempt, 0, len(w.running))
	for id, a := range w.running {
		out = append(out, &workerv1.RunningAttempt{AttemptId: id, FencingToken: a.task.GetFencingToken()})
	}
	return out
}

// cancelAll cancels every running attempt's context with cause.
func (w *Worker) cancelAll(cause error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, a := range w.running {
		a.cancel(cause)
	}
}

// cancelAttempt cancels one running attempt's context with cause; unknown ids are ignored (the
// attempt already ended).
func (w *Worker) cancelAttempt(id string, cause error) {
	w.mu.Lock()
	a, ok := w.running[id]
	w.mu.Unlock()
	if ok {
		a.cancel(cause)
	}
}
