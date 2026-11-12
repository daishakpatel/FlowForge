package main

import (
	"context"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/daishakpatel/flowforge/internal/testutil"
)

func TestMain(m *testing.M) { testutil.LeakCheck(m) }

// TestServe_DrainsInFlightRequestOnShutdown: a request in progress when the stop signal arrives
// (ctx is the NotifyContext of SIGTERM in main) completes with its response, and serve returns
// nil afterwards.
func TestServe_DrainsInFlightRequestOnShutdown(t *testing.T) {
	inHandler, release := make(chan struct{}), make(chan struct{})
	srv := &http.Server{
		ReadHeaderTimeout: time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			close(inHandler)
			<-release
			_, _ = io.WriteString(w, "done")
		}),
	}
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- serve(ctx, srv, ln, 10*time.Second) }()

	type result struct {
		body string
		err  error
	}
	got := make(chan result, 1)
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	go func() {
		req, err := http.NewRequestWithContext(context.Background(), "GET", "http://"+ln.Addr().String()+"/slow", nil)
		if err != nil {
			got <- result{err: err}
			return
		}
		resp, err := client.Do(req)
		if err != nil {
			got <- result{err: err}
			return
		}
		defer func() { _ = resp.Body.Close() }()
		b, err := io.ReadAll(resp.Body)
		got <- result{string(b), err}
	}()

	<-inHandler
	stop() // SIGTERM
	select {
	case err := <-served:
		t.Fatalf("serve returned %v before the in-flight request finished", err)
	case <-time.After(100 * time.Millisecond): //nolint:forbidigo // bounded wait proving serve is still draining
	}
	close(release)
	if r := <-got; r.err != nil || r.body != "done" {
		t.Errorf("in-flight request = %q, %v; want it to complete", r.body, r.err)
	}
	if err := <-served; err != nil {
		t.Errorf("serve = %v, want nil after a graceful drain", err)
	}
}
