//go:build integration

package main

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/daishakpatel/flowforge/internal/config"
	"github.com/daishakpatel/flowforge/internal/testutil"
	"github.com/daishakpatel/flowforge/internal/workerapi/workerapitest"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
)

func TestMain(m *testing.M) { testutil.LeakCheck(m) }

// syncBuffer collects the binary's log output from its goroutines.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}
func (s *syncBuffer) String() string { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

func envOpts(env map[string]string) config.Options {
	return config.Options{LookupEnv: func(k string) (string, bool) { v, ok := env[k]; return v, ok }}
}

func workerEnv(srv *workerapitest.Server, token string) map[string]string {
	return map[string]string{
		"FF_WORKER_ID": "w-binary", "FF_WORKER_QUEUES": model.DefaultQueue, "FF_WORKER_CAPACITY": "2",
		"FF_API_GRPC_TARGET": srv.Addr, "FF_WORKER_TOKEN": token, "FF_WORKER_SHUTDOWN_GRACE": "5s",
		"FF_WORKER_ENABLE_CRASH_HANDLER": "true", "FF_LOG_LEVEL": "debug",
	}
}

func loopback(addr chan<- string) func(context.Context, string, string) (net.Listener, error) {
	return func(ctx context.Context, network, _ string) (net.Listener, error) {
		ln, err := (&net.ListenConfig{}).Listen(ctx, network, "127.0.0.1:0")
		if err == nil {
			addr <- ln.Addr().String()
		}
		return ln, err
	}
}

// TestRun_ExecutesAndShutsDownGracefully runs the real binary wiring: it executes tasks
// (including crash, with a recording exit), serves its metrics, and on cancellation (SIGTERM)
// lets an in-flight attempt finish, deregisters and returns nil (exit 0).
func TestRun_ExecutesAndShutsDownGracefully(t *testing.T) {
	srv := workerapitest.Start(t, workerapitest.Options{})
	addr := make(chan string, 1)
	exited := make(chan int, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var logs syncBuffer
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, envOpts(workerEnv(srv, srv.Token)), &logs, loopback(addr), func(code int) { exited <- code })
	}()
	base := "http://" + <-addr

	noop := srv.Queue(t, "noop", 60, `{"n":1}`)[0]
	if a := srv.WaitFinished(t, noop); a.Status != model.AttemptSucceeded {
		t.Fatalf("noop attempt %s; logs:\n%s", a.Status, logs.String())
	}
	srv.WaitFinished(t, srv.Queue(t, "crash", 60, `{}`)[0])
	if code := <-exited; code != 1 {
		t.Errorf("crash handler exit code %d, want 1", code)
	}
	if _, body := get(t, base+"/metrics"); !strings.Contains(body, `flowforge_worker_task_outcomes_total{outcome="succeeded"} 1`) {
		t.Errorf("/metrics lacks the outcome counter:\n%s", body)
	}

	inflight := srv.Queue(t, "sleep", 60, `{"duration_ms":2000}`)[0]
	srv.WaitRunning(t, inflight)
	cancel() // SIGTERM
	if err := <-done; err != nil {
		t.Fatalf("run = %v, want nil (exit 0); logs:\n%s", err, logs.String())
	}
	if a := srv.WaitFinished(t, inflight); a.Status != model.AttemptSucceeded {
		t.Errorf("in-flight attempt %s, want SUCCEEDED within the grace", a.Status)
	}
	if s := srv.Scalar(t, `SELECT status FROM worker_observations WHERE worker_id = 'w-binary'`); s != "DEREGISTERED" {
		t.Errorf("worker status %s, want DEREGISTERED", s)
	}
}

func TestRun_RejectedKeyFailsFast(t *testing.T) {
	srv := workerapitest.Start(t, workerapitest.Options{})
	addr := make(chan string, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := run(ctx, envOpts(workerEnv(srv, "ff_bogus_key")), io.Discard, loopback(addr), func(int) {})
	if err == nil || !strings.Contains(err.Error(), "Unauthenticated") || ctx.Err() != nil {
		t.Fatalf("run = %v, want an UNAUTHENTICATED registration error before the deadline", err)
	}
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(body)
}
