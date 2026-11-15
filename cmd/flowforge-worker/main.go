// Command flowforge-worker runs task handlers for FlowForge (task 03.03): it registers with
// flowforge-api's WorkerService (FF_API_GRPC_TARGET, key FF_WORKER_TOKEN), polls within
// FF_WORKER_CAPACITY, and runs the built-in handlers (noop, sleep, fail, flaky, echo, plus crash
// with FF_WORKER_ENABLE_CRASH_HANDLER=true). It serves /metrics, /healthz and /readyz on
// FF_WORKER_HTTP_ADDR. On SIGTERM or SIGINT it stops polling, lets running attempts finish for
// FF_WORKER_SHUTDOWN_GRACE, cancels the rest, deregisters and exits 0. Configuration: 00E §6.
package main

import (
	"context"
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

	workerv1 "github.com/daishakpatel/flowforge/internal/gen/proto/worker/v1"

	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/config"
	"github.com/daishakpatel/flowforge/internal/logging"
	"github.com/daishakpatel/flowforge/internal/obs"
	"github.com/daishakpatel/flowforge/internal/version"
	"github.com/daishakpatel/flowforge/internal/worker/handlers"
	"github.com/daishakpatel/flowforge/internal/worker/runtime"
)

// shutdownTimeout bounds the drain of the operational HTTP server once the worker has stopped.
const shutdownTimeout = 5 * time.Second

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, config.Options{}, os.Stderr, (&net.ListenConfig{}).Listen, os.Exit)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "flowforge-worker:", err)
		os.Exit(1)
	}
}

// run is main without process exit, so tests can drive it with their own environment, listener
// and exit function (used only by the crash handler). It returns nil after a graceful shutdown.
func run(ctx context.Context, opts config.Options, logOut io.Writer,
	listen func(ctx context.Context, network, addr string) (net.Listener, error), exit func(int)) error {
	cfg, err := config.Load[config.Worker](opts)
	if err != nil {
		return err
	}
	logger := logging.New(logging.Config{
		Level: cfg.LogLevel, Format: cfg.LogFormat, Service: "flowforge-worker", Version: version.Version(), Writer: logOut,
	}).With("worker_id", cfg.ID)
	logger.Info("starting", "config", cfg.Redacted())
	registry, err := newRegistry(cfg, exit, logger)
	if err != nil {
		return err
	}
	conn, err := runtime.Dial(cfg.APIGRPCTarget, cfg.Token.Reveal())
	if err != nil {
		return fmt.Errorf("dial %s: %w", cfg.APIGRPCTarget, err)
	}
	defer func() { _ = conn.Close() }() // closing a client connection only releases resources

	reg := obs.NewRegistry()
	metrics, err := runtime.NewMetrics(reg)
	if err != nil {
		return err
	}
	ln, err := listen(ctx, "tcp", cfg.HTTPAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.HTTPAddr, err)
	}
	srv := obs.NewServer("", reg) // ready once the process serves; Register proves the API
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ln) }()
	logger.Info("listening", "addr", ln.Addr().String())

	//nolint:gosec // G404: backoff jitter needs no cryptographic randomness
	rng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 0))
	w := runtime.New(runtime.Config{
		ID: cfg.ID, Queues: cfg.Queues, Capacity: cfg.Capacity, Version: version.Version(),
		Heartbeat: cfg.Heartbeat, ShutdownGrace: cfg.ShutdownGrace,
	}, runtime.Deps{
		Client: workerv1.NewWorkerServiceClient(conn), Registry: registry, Clock: clock.Real{},
		Logger: logger, Metrics: metrics, Rand: rng,
	})
	err = w.Run(ctx)
	if serr := shutdown(context.WithoutCancel(ctx), srv, served); serr != nil && err == nil {
		err = serr
	}
	logger.Info("stopped", logging.Err(err))
	return err
}

// newRegistry registers the built-in handlers; crash only when enabled (dev and test only).
func newRegistry(cfg config.Worker, exit func(int), logger *slog.Logger) (*handlers.Registry, error) {
	if cfg.EnableCrashHandler != "true" {
		exit = nil
	}
	r := handlers.NewRegistry()
	for _, h := range handlers.Builtins(clock.Real{}, exit) {
		if err := r.Register(h); err != nil {
			return nil, err
		}
	}
	if exit != nil {
		logger.Warn("crash handler enabled: tasks of type crash exit this process")
	}
	return r, nil
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
