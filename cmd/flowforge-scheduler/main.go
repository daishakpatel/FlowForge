// Command flowforge-scheduler runs the leader loops: the decision loop (task 03.02) promotes
// tasks PENDING → READY/SKIPPED → QUEUED, requeues due retries and drives run status; the
// timeout and recovery loops (03.04) end overdue attempts and recover lost workers' work. It
// never talks to workers.
//
// Leadership comes from scheduler.AlwaysLeader (single-scheduler mode, epoch 0) until task 04.07
// adds the Raft-backed Gate. It refuses to start on a schema older than its migrations, serves
// /metrics, /healthz and /readyz on FF_SCHED_HTTP_ADDR, and on SIGTERM or SIGINT stops its loops
// (an open decision transaction rolls back) and exits. Configuration: 00E §6.
package main

import (
	"context"
	crand "crypto/rand"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/config"
	"github.com/daishakpatel/flowforge/internal/events"
	"github.com/daishakpatel/flowforge/internal/ids"
	"github.com/daishakpatel/flowforge/internal/logging"
	"github.com/daishakpatel/flowforge/internal/obs"
	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/persistence/migrate"
	"github.com/daishakpatel/flowforge/internal/persistence/pg"
	"github.com/daishakpatel/flowforge/internal/recovery"
	"github.com/daishakpatel/flowforge/internal/scheduler"
	"github.com/daishakpatel/flowforge/internal/version"
	"github.com/daishakpatel/flowforge/internal/workerapi"
	"github.com/daishakpatel/flowforge/internal/workflow/expr"
)

// shutdownTimeout bounds the drain of the operational HTTP server.
const shutdownTimeout = 10 * time.Second

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, config.Options{}, os.Stderr, (&net.ListenConfig{}).Listen)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "flowforge-scheduler:", err)
		os.Exit(1)
	}
}

// run is main without process exit, so tests can drive it with their own environment and
// listener. It returns nil when ctx is cancelled and everything has stopped, or the error that
// stopped the scheduler (for example a stale epoch).
func run(ctx context.Context, opts config.Options, logOut io.Writer, listen func(ctx context.Context, network, addr string) (net.Listener, error)) error {
	cfg, err := config.Load[config.Scheduler](opts)
	if err != nil {
		return err
	}
	logger := logging.New(logging.Config{
		Level: cfg.LogLevel, Format: cfg.LogFormat, Service: "flowforge-scheduler", Version: version.Version(), Writer: logOut,
	})
	logger.Info("starting", "config", cfg.Redacted())
	ctx = logging.NewContext(ctx, logger)
	dsn := cfg.PGDSN.Reveal()
	if err := migrate.RequireLatest(ctx, dsn, migrate.Flowforge); err != nil {
		return err
	}
	pool, err := pg.NewPool(ctx, cfg.PGDSN)
	if err != nil {
		return err
	}
	defer pool.Close()

	reg := obs.NewRegistry()
	metrics, err := scheduler.NewMetrics(reg)
	if err != nil {
		return err
	}
	loopErrors, err := scheduler.NewLoopErrors(reg)
	if err != nil {
		return err
	}
	gen := ids.NewGenerator(clock.Real{}, crand.Reader)
	rec := events.NewRecorder(clock.Real{}, gen, "flowforge-scheduler")
	//nolint:gosec // G404: jitter for transaction retries needs no cryptographic randomness
	rng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 0))
	waker := workerapi.NewChannelWaker(func(ctx context.Context) (*pgx.Conn, error) {
		return pgx.Connect(ctx, dsn)
	}, clock.Real{}, persistence.SchedulerWakeChannel)
	txm, runs, tasks := pg.NewTxManager(pool, clock.Real{}, rng), pg.NewRunRepo(gen, rec), pg.NewTaskRepo(rec)
	//nolint:gosec // G404: retry jitter needs no cryptographic randomness
	jitter := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 1))
	ender := workerapi.AttemptEnder{
		Attempts: pg.NewAttemptRepo(gen, rec), Tasks: tasks,
		Outcomes: workerapi.NewRetryOutcomeHandler(tasks, runs, clock.Real{}, jitter),
	}
	runner := scheduler.NewRunner(scheduler.AlwaysLeader{})
	runner.RegisterLoop(&scheduler.DecisionLoop{
		DB: pool, Tx: txm, Cluster: pg.ClusterRepo{}, Sched: pg.SchedulerRepo{}, Notify: pg.SchedulerRepo{},
		Runs: runs, Tasks: tasks, Defs: pg.NewDefinitionRepo(gen),
		Inputs: scheduler.TemplateResolver{}, Eval: expr.Evaluator{}, Clock: clock.Real{}, Tick: cfg.Tick,
		QueueLimits: cfg.QueueLimitMap(), Wake: waker, Metrics: metrics, Errors: loopErrors,
	})
	runner.RegisterLoop(&scheduler.TimeoutLoop{
		DB: pool, Tx: txm, Cluster: pg.ClusterRepo{}, Recovery: pg.RecoveryRepo{}, Ender: ender,
		Notify: pg.SchedulerRepo{}, Clock: clock.Real{}, Tick: time.Second, Errors: loopErrors,
		Runs: runs, Sched: pg.SchedulerRepo{}, RunControl: pg.RunControlRepo{},
	})
	runner.RegisterLoop(&recovery.Loop{
		DB: pool, Tx: txm, Cluster: pg.ClusterRepo{}, Recovery: pg.RecoveryRepo{},
		Leases: &workerapi.PGLeaseProvider{DB: pool, Workers: pg.WorkerRepo{}, Clock: clock.Real{}, TTL: cfg.WorkerLeaseTTL},
		Ender:  ender, Notify: pg.SchedulerRepo{}, Events: rec, Clock: clock.Real{}, Tick: 2 * time.Second,
		LeaseTTL: cfg.WorkerLeaseTTL, Heartbeat: cfg.WorkerHeartbeat, Errors: loopErrors,
	})

	ln, err := listen(ctx, "tcp", cfg.HTTPAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.HTTPAddr, err)
	}
	srv := obs.NewServer("", reg, obs.ReadinessCheck{Name: "postgres", Check: func(ctx context.Context) error {
		if err := pool.Ping(ctx); err != nil {
			return err
		}
		return migrate.RequireLatest(ctx, dsn, migrate.Flowforge)
	}})
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ln) }()
	logger.Info("listening", "addr", ln.Addr().String())

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	wakerDone := make(chan struct{})
	go func() {
		defer close(wakerDone)
		_ = waker.Run(ctx) // returns nil once ctx is done; it logs its own reconnects
	}()
	err = runner.Run(ctx)
	cancel()
	<-wakerDone
	if serr := shutdown(context.WithoutCancel(ctx), srv, served); serr != nil && err == nil {
		err = serr
	}
	logger.Info("stopped", logging.Err(err))
	return err
}

// shutdown stops the operational server, waiting up to shutdownTimeout for open requests.
func shutdown(ctx context.Context, srv *http.Server, served <-chan error) error {
	ctx, cancel := context.WithTimeout(ctx, shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		return fmt.Errorf("shut down http server: %w", err)
	}
	if err := <-served; !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("http server: %w", err)
	}
	return nil
}
