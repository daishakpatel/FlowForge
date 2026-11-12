// Command flowforge-api serves the FlowForge REST API (task 02.04).
//
// It refuses to start while the flowforge schema is behind the migrations it was built with
// (run flowforge-migrate up first), and on SIGTERM or SIGINT it stops accepting connections and
// lets in-flight requests finish for up to 30 s. Configuration: 00E §6 through internal/config.
package main

import (
	"context"
	crand "crypto/rand"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"

	"github.com/daishakpatel/flowforge/internal/api/auth"
	"github.com/daishakpatel/flowforge/internal/api/rest"
	"github.com/daishakpatel/flowforge/internal/api/service"
	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/config"
	"github.com/daishakpatel/flowforge/internal/events"
	workerv1 "github.com/daishakpatel/flowforge/internal/gen/proto/worker/v1"
	"github.com/daishakpatel/flowforge/internal/ids"
	"github.com/daishakpatel/flowforge/internal/logging"
	"github.com/daishakpatel/flowforge/internal/obs"
	"github.com/daishakpatel/flowforge/internal/persistence/migrate"
	"github.com/daishakpatel/flowforge/internal/persistence/pg"
	"github.com/daishakpatel/flowforge/internal/version"
	"github.com/daishakpatel/flowforge/internal/workerapi"
)

// shutdownTimeout bounds the drain of in-flight requests after SIGTERM.
const shutdownTimeout = 30 * time.Second

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, config.Options{}, os.Stderr, (&net.ListenConfig{}).Listen)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "flowforge-api:", err)
		os.Exit(1)
	}
}

// run is main without process exit, so tests can drive it with their own environment and
// listener. It returns when ctx is cancelled and the server has drained.
func run(ctx context.Context, opts config.Options, logOut io.Writer, listen func(ctx context.Context, network, addr string) (net.Listener, error)) error {
	cfg, err := config.Load[config.API](opts)
	if err != nil {
		return err
	}
	logger := logging.New(logging.Config{
		Level: cfg.LogLevel, Format: cfg.LogFormat, Service: "flowforge-api", Version: version.Version(), Writer: logOut,
	})
	logger.Info("starting", "config", cfg.Redacted())
	authDisabled := cfg.AuthDisabled == "true"
	if authDisabled {
		logger.Warn("authentication is disabled (FF_AUTH_DISABLED=true): every request runs as the dev admin")
	}
	if err := checkSchema(ctx, cfg.PGDSN); err != nil {
		return err
	}
	pool, err := pg.NewPool(ctx, cfg.PGDSN)
	if err != nil {
		return err
	}
	defer pool.Close()

	keys := pg.APIKeyRepo{}
	generated, err := auth.Bootstrap(ctx, pool, keys, crand.Reader, cfg.Env, cfg.AdminBootstrapKey)
	if err != nil {
		return fmt.Errorf("bootstrap admin key: %w", err)
	}
	if generated != "" {
		// Written past the logger on purpose: the logger masks API keys, and this one must be
		// readable once. Dev only (auth.Bootstrap); it is not stored anywhere in clear text.
		_, _ = fmt.Fprintf(logOut, "WARN flowforge-api: no admin API key existed, generated one (dev only, shown once): %s\n", generated)
	}
	authn := auth.NewStub(true)
	if !authDisabled {
		authn = auth.NewKeyAuthenticator(pool, keys, clock.Real{})
	}

	gen := ids.NewGenerator(clock.Real{}, crand.Reader)
	rec := events.NewRecorder(clock.Real{}, gen, "flowforge-api")
	defs := pg.NewDefinitionRepo(gen)
	//nolint:gosec // G404: jitter for transaction retries needs no cryptographic randomness
	rng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 0))
	txm := pg.NewTxManager(pool, clock.Real{}, rng)
	reg := obs.NewRegistry()
	router, err := rest.NewRouter(rest.Deps{
		Workflows: &service.WorkflowService{DB: pool, DefRepo: defs},
		Runs: &service.RunService{
			DB: pool, Tx: txm, DefRepo: defs,
			RunRepo: pg.NewRunRepo(gen, rec), TaskRepo: pg.NewTaskRepo(rec),
			AttemptRepo: pg.NewAttemptRepo(gen, rec), EventRepo: pg.EventRepo{},
		},
		Auth: authn, IDs: gen, Clock: clock.Real{}, Logger: logger,
		Metrics: reg,
		DB:      pool, Tx: txm, APIKeys: keys, Audit: pg.AuditRepo{}, RateLimitRPS: float64(cfg.RateLimitRPS),
		Workers: pg.WorkerRepo{}, Logs: pg.TaskLogRepo{}, WorkerLeaseTTL: cfg.WorkerLeaseTTL,
		Ready: func(ctx context.Context) error {
			if err := pool.Ping(ctx); err != nil {
				return err
			}
			return checkSchema(ctx, cfg.PGDSN)
		},
	})
	if err != nil {
		return err
	}

	grpcSrv, waker, err := newWorkerServer(reg, authn, gen, logger, pool, txm, rec, cfg)
	if err != nil {
		return err
	}
	gln, err := listen(ctx, "tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.GRPCAddr, err)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	grpcDone, wakerDone := make(chan error, 1), make(chan struct{})
	go func() {
		defer close(wakerDone)
		_ = waker.Run(ctx) // returns nil once ctx is done; it logs its own reconnects
	}()
	go func() {
		err := grpcSrv.Serve(gln)
		cancel() // a dead gRPC listener takes the HTTP server down too
		grpcDone <- err
	}()
	logger.Info("grpc listening", "addr", gln.Addr().String())

	ln, err := listen(ctx, "tcp", cfg.HTTPAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.HTTPAddr, err)
	}
	srv := &http.Server{
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       rest.RequestTimeout,
		WriteTimeout:      rest.RequestTimeout + 5*time.Second,
		IdleTimeout:       60 * time.Second,
	}
	logger.Info("listening", "addr", ln.Addr().String())
	err = serve(ctx, srv, ln, shutdownTimeout)
	stopGRPC(grpcSrv, shutdownTimeout)
	if gerr := <-grpcDone; gerr != nil && err == nil {
		err = fmt.Errorf("grpc server: %w", gerr)
	}
	<-wakerDone
	logger.Info("stopped", logging.Err(err))
	return err
}

// serve runs srv on ln until ctx is done, then shuts it down gracefully: no new connections,
// in-flight requests get up to drain to finish.
func serve(ctx context.Context, srv *http.Server, ln net.Listener, drain time.Duration) error {
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	select {
	case err := <-errc:
		return fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
	}
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), drain)
	defer cancel()
	if err := srv.Shutdown(sctx); err != nil {
		return fmt.Errorf("graceful shutdown: %w", err)
	}
	if err := <-errc; !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("http server: %w", err)
	}
	return nil
}

// checkSchema fails unless the flowforge schema is at least the newest embedded migration;
// services never migrate themselves (task 02.01 handoff).
func checkSchema(ctx context.Context, dsn config.Secret) error {
	have, err := migrate.Version(ctx, dsn.Reveal(), migrate.Flowforge)
	if err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}
	want, err := migrate.Latest(migrate.Flowforge)
	if err != nil {
		return err
	}
	if have < want {
		return fmt.Errorf("database schema is at version %d but this build needs %d: run flowforge-migrate up", have, want)
	}
	return nil
}

// newWorkerServer builds the worker gRPC server (task 03.01): interceptors, the 4 MiB message cap,
// WorkerService with the default seams, and the LISTEN waker on its own connection.
func newWorkerServer(reg *obs.Registry, authn auth.Authenticator, gen ids.Generator, logger *slog.Logger,
	pool *pgxpool.Pool, txm *pg.TxManager, rec *events.Recorder, cfg config.API) (*grpc.Server, *workerapi.Waker, error) {
	ic, err := workerapi.NewInterceptors(reg, authn, gen, logger, clock.Real{})
	if err != nil {
		return nil, nil, err
	}
	stale, err := reg.Counter(prometheus.CounterOpts{
		Name: "flowforge_stale_attempt_rejections_total", Help: "Worker reports refused for a stale fencing token.",
	})
	if err != nil {
		return nil, nil, fmt.Errorf("register stale attempt metric: %w", err)
	}
	waker := workerapi.NewWaker(func(ctx context.Context) (*pgx.Conn, error) {
		return pgx.Connect(ctx, cfg.PGDSN.Reveal())
	}, clock.Real{})
	tasks := pg.NewTaskRepo(rec)
	svc := &workerapi.Service{
		DB: pool, Tx: txm, Tasks: tasks, Attempts: pg.NewAttemptRepo(gen, rec), Workers: pg.WorkerRepo{},
		Logs: pg.TaskLogRepo{}, Outcomes: workerapi.TerminalOutcomeHandler{Tasks: tasks}, Cancels: workerapi.NoCancels{},
		Leases: &workerapi.PGLeaseProvider{DB: pool, Workers: pg.WorkerRepo{}, Clock: clock.Real{}, TTL: cfg.WorkerLeaseTTL},
		Waker:  waker, Clock: clock.Real{}, LeaseTTL: cfg.WorkerLeaseTTL, HeartbeatInterval: cfg.WorkerHeartbeat,
		StaleRejections: stale.WithLabelValues(),
	}
	srv := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxGRPCMessage), grpc.MaxSendMsgSize(maxGRPCMessage),
		grpc.ChainUnaryInterceptor(ic.Unary), grpc.ChainStreamInterceptor(ic.Stream),
	)
	workerv1.RegisterWorkerServiceServer(srv, svc)
	return srv, waker, nil
}

// maxGRPCMessage caps worker messages (task 03.01 §5).
const maxGRPCMessage = 4 << 20

// stopGRPC drains the gRPC server (long polls end within their wait), forcing it after drain.
func stopGRPC(srv *grpc.Server, drain time.Duration) {
	done := make(chan struct{})
	go func() { srv.GracefulStop(); close(done) }()
	t := time.NewTimer(drain)
	defer t.Stop()
	select {
	case <-done:
	case <-t.C:
		srv.Stop()
		<-done
	}
}
