//go:build integration

package main

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daishakpatel/flowforge/internal/api/auth"
	"github.com/daishakpatel/flowforge/internal/config"
	"github.com/daishakpatel/flowforge/internal/testutil"
)

func envOpts(env map[string]string) config.Options {
	return config.Options{LookupEnv: func(k string) (string, bool) { v, ok := env[k]; return v, ok }}
}

// TestRun_RefusesToStartWhenSchemaIsBehind: against an empty database run fails before
// listening, naming the fix.
func TestRun_RefusesToStartWhenSchemaIsBehind(t *testing.T) {
	db := testutil.NewPostgres(t)
	listened := false
	listen := func(ctx context.Context, network, addr string) (net.Listener, error) {
		listened = true
		return (&net.ListenConfig{}).Listen(ctx, network, addr)
	}
	var logs bytes.Buffer
	err := run(t.Context(), envOpts(map[string]string{"FF_PG_DSN": db.DSN}), &logs, listen)
	if err == nil || !strings.Contains(err.Error(), "flowforge-migrate up") || listened {
		t.Errorf("run = %v (listened %v); want a schema error before listening", err, listened)
	}
}

// TestRun_ServesAndStopsCleanly starts the real binary's wiring on a migrated database, checks
// readiness and an authenticated route, then stops it as SIGTERM would.
func TestRun_ServesAndStopsCleanly(t *testing.T) {
	db := testutil.NewFlowforgeDB(t)
	addr := make(chan string, 1)
	listen := func(ctx context.Context, network, a string) (net.Listener, error) {
		ln, err := (&net.ListenConfig{}).Listen(ctx, network, "127.0.0.1:0")
		if err == nil && a == ":8080" { // the HTTP listener; the gRPC one (:9090) is not reported
			addr <- ln.Addr().String()
		}
		return ln, err
	}
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	var logs bytes.Buffer
	go func() {
		done <- run(ctx, envOpts(map[string]string{"FF_PG_DSN": db.DSN, "FF_AUTH_DISABLED": "true"}), &logs, listen)
	}()
	var base string
	select {
	case a := <-addr:
		base = "http://" + a
	case err := <-done:
		t.Fatalf("run exited early: %v\n%s", err, logs.String())
	case <-time.After(30 * time.Second): //nolint:forbidigo // bounded startup wait
		t.Fatal("server did not start")
	}
	client := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	for path, want := range map[string]int{"/readyz": 200, "/api/v1/runs": 200, "/openapi.json": 200} {
		req, err := http.NewRequestWithContext(t.Context(), "GET", base+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != want {
			t.Errorf("GET %s = %d, want %d", path, resp.StatusCode, want)
		}
	}
	stop()
	if err := <-done; err != nil {
		t.Errorf("run = %v, want nil after shutdown", err)
	}
	if !strings.Contains(logs.String(), "authentication is disabled") {
		t.Error("no startup warning about disabled authentication")
	}
}

// startAPI runs the binary's wiring with env until the returned stop is called; it returns the
// base URL and the captured log output.
func startAPI(t *testing.T, env map[string]string) (base string, logs *bytes.Buffer, stop func()) {
	t.Helper()
	addr := make(chan string, 1)
	listen := func(ctx context.Context, network, a string) (net.Listener, error) {
		ln, err := (&net.ListenConfig{}).Listen(ctx, network, "127.0.0.1:0")
		if err == nil && a == ":8080" { // the HTTP listener; the gRPC one (:9090) is not reported
			addr <- ln.Addr().String()
		}
		return ln, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	logs = &bytes.Buffer{}
	go func() { done <- run(ctx, envOpts(env), logs, listen) }()
	select {
	case a := <-addr:
		base = "http://" + a
	case err := <-done:
		cancel()
		t.Fatalf("run exited early: %v\n%s", err, logs.String())
	case <-time.After(30 * time.Second): //nolint:forbidigo // bounded startup wait
		cancel()
		t.Fatal("server did not start")
	}
	return base, logs, func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("run = %v, want nil after shutdown", err)
		}
	}
}

func get(t *testing.T, url, token string) int {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), "GET", url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	return resp.StatusCode
}

func adminRows(t *testing.T, dsn string) int {
	t.Helper()
	conn, err := pgx.Connect(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	var n int
	if err := conn.QueryRow(t.Context(), `SELECT count(*) FROM api_keys WHERE role = 'admin'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// TestRun_BootstrapIsIdempotent: starting twice with FF_ADMIN_BOOTSTRAP_KEY stores one admin key,
// which authenticates; in dev without one, the first start prints a generated key and the
// second start neither generates nor prints another.
func TestRun_BootstrapIsIdempotent(t *testing.T) {
	db := testutil.NewFlowforgeDB(t)
	k, err := auth.GenerateKey(crand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	env := map[string]string{"FF_PG_DSN": db.DSN, "FF_ENV": "prod", "FF_ADMIN_BOOTSTRAP_KEY": k.Token}
	for i := range 2 {
		base, logs, stop := startAPI(t, env)
		if code := get(t, base+"/api/v1/runs", k.Token); code != 200 {
			t.Errorf("start %d: bootstrap key → %d, want 200", i+1, code)
		}
		if code := get(t, base+"/api/v1/runs", ""); code != 401 {
			t.Errorf("start %d: no key → %d, want 401", i+1, code)
		}
		stop()
		if strings.Contains(logs.String(), k.Token[16:]) {
			t.Errorf("start %d logged the configured bootstrap key", i+1)
		}
	}
	if n := adminRows(t, db.DSN); n != 1 {
		t.Errorf("admin keys after two starts = %d, want 1", n)
	}

	dev := testutil.NewFlowforgeDB(t)
	_, first, stop := startAPI(t, map[string]string{"FF_PG_DSN": dev.DSN})
	stop()
	_, second, stop := startAPI(t, map[string]string{"FF_PG_DSN": dev.DSN})
	stop()
	if !strings.Contains(first.String(), "generated one (dev only, shown once): ff_") {
		t.Errorf("first dev start did not print a generated key:\n%s", first.String())
	}
	if strings.Contains(second.String(), "generated one") || adminRows(t, dev.DSN) != 1 {
		t.Errorf("second dev start generated another key (admin rows %d)", adminRows(t, dev.DSN))
	}
}
