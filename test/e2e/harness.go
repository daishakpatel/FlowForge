//go:build e2e

package e2e

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/daishakpatel/flowforge/internal/api/auth"
	"github.com/daishakpatel/flowforge/internal/persistence/migrate"
	"github.com/daishakpatel/flowforge/internal/testutil"
)

// Timings shorten the production defaults so that recovery fits in a test.
const (
	leaseTTL  = 3 * time.Second
	heartbeat = time.Second
	schedTick = "100ms"
	readyWait = time.Minute
	// In raft mode (task 04.07) the scheduler leader lease is 6 s, renewed every 2 s: shorter than
	// the 10 s default, so a takeover (about TTL + renew) fits the 10 s target, and long enough
	// to be realistic. The worker lease (leaseTTL) is the 3 s minimum raft mode allows.
	leaderTTL   = 6 * time.Second
	leaderRenew = 2 * time.Second
)

// Harness is one FlowForge deployment for one test: its own Postgres container, and the
// flowforge-api, flowforge-scheduler and flowforge-worker binaries as subprocesses on random
// ports. Everything stops when the test ends; on failure each process's log tail is printed.
type Harness struct {
	t         *testing.T
	pg        *postgres.PostgresContainer
	DSN       string
	DB        *pgxpool.Pool
	AdminKey  string // the bootstrap admin key
	workerKey string // a worker-role key, created at the first StartAPI
	APIURL    string // http://127.0.0.1:port, stable across API restarts
	apiGRPC   string
	logDir    string
	http      *http.Client

	// coordination is FF_COORDINATION for the API and schedulers: local, or raft with the
	// 3-node cluster Raft (task 04.07).
	coordination  string
	raftEndpoints string
	Raft          []*Proc

	mu    sync.Mutex
	procs []*Proc
}

// Proc is one subprocess. Restart reuses its name, ports and environment.
type Proc struct {
	Name     string
	bin      string
	env      []string
	httpAddr string // serves /readyz
	logPath  string

	mu   sync.Mutex
	cmd  *exec.Cmd
	done chan struct{} // closed when the current incarnation exits
}

// New starts Postgres, migrates it and returns a harness with no processes yet. The coordination
// mode comes from FF_E2E_COORDINATION (local, the default, or raft: then a 3-node Raft cluster
// starts too), so the whole suite runs in either mode (make test-e2e, make test-e2e-raft).
func New(t *testing.T) *Harness {
	t.Helper()
	return newHarness(t, os.Getenv("FF_E2E_COORDINATION"))
}

// NewRaft is New in raft mode, for the tests that need Raft whatever the suite's mode.
func NewRaft(t *testing.T) *Harness {
	t.Helper()
	return newHarness(t, "raft")
}

func newHarness(t *testing.T, mode string) *Harness {
	t.Helper()
	if mode == "" {
		mode = "local"
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	c, err := postgres.Run(ctx, testutil.PGImage, postgres.WithUsername("flowforge"),
		postgres.WithPassword("flowforge"), postgres.WithDatabase("flowforge"), postgres.BasicWaitStrategies())
	if err != nil {
		t.Fatalf("e2e: start postgres (the suite needs Docker): %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) }) // registered first, so it runs last
	dsn, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("e2e: postgres DSN: %v", err)
	}
	if err := testutil.ApplyMigrations(t, dsn, string(migrate.Flowforge)); err != nil {
		t.Fatalf("e2e: migrate: %v", err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("e2e: pool: %v", err)
	}
	t.Cleanup(pool.Close)
	key, err := auth.GenerateKey(crand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	h := &Harness{t: t, pg: c, DSN: dsn, DB: pool, AdminKey: key.Token, logDir: logDir(t),
		http: &http.Client{Timeout: 30 * time.Second}, coordination: mode}
	t.Cleanup(h.stopAll)
	if mode == "raft" {
		h.StartRaft()
	}
	return h
}

// StartRaft starts a 3-node flowforge-raft cluster with static peers, each with its own data
// directory (kept across Restart), and waits until every node knows a leader. All three start
// before any is waited on: a node is ready only once a quorum elected a leader.
func (h *Harness) StartRaft() {
	h.t.Helper()
	type addrs struct{ client, peer, http string }
	nodes := make([]addrs, 3)
	var peers, endpoints []string
	for i := range nodes {
		nodes[i] = addrs{freeAddr(h.t), freeAddr(h.t), freeAddr(h.t)}
		peers = append(peers, fmt.Sprintf("%d=%s", i+1, nodes[i].peer))
		endpoints = append(endpoints, nodes[i].client)
	}
	h.raftEndpoints = strings.Join(endpoints, ",")
	var dones []chan struct{}
	for i, n := range nodes {
		name := fmt.Sprintf("raft-%d", i+1)
		p := &Proc{Name: name, bin: "flowforge-raft", httpAddr: n.http, logPath: filepath.Join(h.logDir, name+".log"),
			env: common(map[string]string{
				"FF_RAFT_NODE_ID": fmt.Sprint(i + 1), "FF_RAFT_PEERS": strings.Join(peers, ","), "FF_RAFT_DATA_DIR": h.t.TempDir(),
				"FF_RAFT_CLIENT_ADDR": n.client, "FF_RAFT_PEER_ADDR": n.peer, "FF_RAFT_HTTP_ADDR": n.http,
			})}
		h.mu.Lock()
		h.procs = append(h.procs, p)
		h.mu.Unlock()
		h.Raft = append(h.Raft, p)
		dones = append(dones, h.spawn(p))
	}
	for i, p := range h.Raft {
		h.waitReady(p, dones[i])
	}
}

// coordinationEnv is what the API and schedulers need for the harness's coordination mode.
func (h *Harness) coordinationEnv() map[string]string {
	if h.coordination != "raft" {
		return nil
	}
	return map[string]string{"FF_COORDINATION": "raft", "FF_RAFT_ENDPOINTS": h.raftEndpoints,
		"FF_LEADER_TTL": leaderTTL.String(), "FF_LEADER_RENEW": leaderRenew.String()}
}

// logDir is FF_E2E_LOG_DIR/<test> when set (kept after the run, e.g. as a CI artifact), else a
// temporary directory.
func logDir(t *testing.T) string {
	base := os.Getenv("FF_E2E_LOG_DIR")
	if base == "" {
		return t.TempDir()
	}
	dir := filepath.Join(base, strings.NewReplacer("/", "_", " ", "_").Replace(t.Name()))
	if err := os.MkdirAll(dir, 0o750); err != nil { //nolint:gosec // the developer's own log directory
		t.Fatal(err)
	}
	return dir
}

// freeAddr reserves a free loopback port. Another process could take it before the child binds;
// the child then fails to start and the readiness wait reports it.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

// common is the environment of every process.
func common(extra map[string]string) []string {
	env := []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"), "FF_ENV=test",
		"FF_LOG_FORMAT=text", "FF_LOG_LEVEL=debug",
		"FF_WORKER_LEASE_TTL=" + leaseTTL.String(), "FF_WORKER_HEARTBEAT=" + heartbeat.String()}
	for k, v := range extra {
		env = append(env, k+"="+v)
	}
	return env
}

// StartAPI starts flowforge-api and, the first time, creates the workers' API key.
func (h *Harness) StartAPI() *Proc {
	h.t.Helper()
	httpAddr, grpcAddr := freeAddr(h.t), freeAddr(h.t)
	h.APIURL, h.apiGRPC = "http://"+httpAddr, grpcAddr
	p := h.start("api", "flowforge-api", httpAddr, merge(map[string]string{
		"FF_PG_DSN": h.DSN, "FF_API_HTTP_ADDR": httpAddr, "FF_API_GRPC_ADDR": grpcAddr,
		"FF_ADMIN_BOOTSTRAP_KEY": h.AdminKey, "FF_CANCEL_GRACE": "2s", "FF_API_RATE_LIMIT_RPS": "100000",
	}, h.coordinationEnv()))
	if h.workerKey == "" {
		var k struct {
			Key string `json:"key"`
		}
		h.MustDo("POST", "/api/v1/api-keys", map[string]any{"name": "e2e-workers", "role": "worker"}, &k, http.StatusCreated)
		h.workerKey = k.Key
	}
	return p
}

// StartScheduler starts flowforge-scheduler named "scheduler". In local mode start at most one;
// in raft mode it is one leader candidate (see StartSchedulers).
func (h *Harness) StartScheduler() *Proc {
	h.t.Helper()
	return h.startScheduler("scheduler")
}

// StartSchedulers starts n leader candidates scheduler-1..n (raft mode, task 04.07); the
// process name is also its FF_SCHED_NODE_ID, the leader key's node_id.
func (h *Harness) StartSchedulers(n int) []*Proc {
	h.t.Helper()
	var out []*Proc
	for i := 1; i <= n; i++ {
		out = append(out, h.startScheduler(fmt.Sprintf("scheduler-%d", i)))
	}
	return out
}

func (h *Harness) startScheduler(name string) *Proc {
	h.t.Helper()
	addr := freeAddr(h.t)
	return h.start(name, "flowforge-scheduler", addr, merge(map[string]string{
		"FF_PG_DSN": h.DSN, "FF_SCHED_HTTP_ADDR": addr, "FF_SCHED_TICK": schedTick, "FF_EVENT_PUBLISHER": "log",
		"FF_SCHED_NODE_ID": name,
	}, h.coordinationEnv()))
}

// merge returns a with b's entries added.
func merge(a, b map[string]string) map[string]string {
	for k, v := range b {
		a[k] = v
	}
	return a
}

// StartWorker starts flowforge-worker id with capacity slots on queues (default: "default").
// StartAPI must have run.
func (h *Harness) StartWorker(id string, capacity int, queues ...string) *Proc {
	h.t.Helper()
	if len(queues) == 0 {
		queues = []string{"default"}
	}
	addr := freeAddr(h.t)
	return h.start(id, "flowforge-worker", addr, map[string]string{
		"FF_WORKER_ID": id, "FF_WORKER_QUEUES": strings.Join(queues, ","), "FF_WORKER_CAPACITY": fmt.Sprint(capacity),
		"FF_API_GRPC_TARGET": h.apiGRPC, "FF_WORKER_TOKEN": h.workerKey, "FF_WORKER_HTTP_ADDR": addr,
		"FF_WORKER_SHUTDOWN_GRACE": "2s", "FF_WORKER_ENABLE_CRASH_HANDLER": "true",
	})
}

// StartAll starts the API, the scheduler and workers w1..wn of the given capacity.
func (h *Harness) StartAll(workers, capacity int) (api, sched *Proc, ws []*Proc) {
	h.t.Helper()
	api, sched = h.StartAPI(), h.StartScheduler()
	for i := 1; i <= workers; i++ {
		ws = append(ws, h.StartWorker(fmt.Sprintf("worker-%d", i), capacity))
	}
	return api, sched, ws
}

func (h *Harness) start(name, bin, httpAddr string, env map[string]string) *Proc {
	h.t.Helper()
	p := &Proc{Name: name, bin: bin, env: common(env), httpAddr: httpAddr, logPath: filepath.Join(h.logDir, name+".log")}
	h.mu.Lock()
	h.procs = append(h.procs, p)
	h.mu.Unlock()
	h.launch(p)
	return p
}

// launch starts p's next incarnation and waits for its /readyz.
func (h *Harness) launch(p *Proc) {
	h.t.Helper()
	h.waitReady(p, h.spawn(p))
}

// spawn starts p's next incarnation and returns the channel closed when it exits.
func (h *Harness) spawn(p *Proc) chan struct{} {
	h.t.Helper()
	f, err := os.OpenFile(p.logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		h.t.Fatal(err)
	}
	_, _ = fmt.Fprintf(f, "=== e2e: start %s at %s\n", p.Name, time.Now().Format(time.RFC3339Nano))
	// Not tied to a context: the process outlives this call and ends through Kill or Stop.
	cmd := exec.CommandContext(context.Background(), filepath.Join(binDir, p.bin)) //nolint:gosec // a binary this suite built
	cmd.Env, cmd.Stdout, cmd.Stderr = p.env, f, f
	if err := cmd.Start(); err != nil {
		_ = f.Close()
		h.t.Fatalf("e2e: start %s: %v", p.Name, err)
	}
	done := make(chan struct{})
	go func() {
		_ = cmd.Wait() // the exit status is visible in the log; Kill expects a signal exit
		_ = f.Close()
		close(done)
	}()
	p.mu.Lock()
	p.cmd, p.done = cmd, done
	p.mu.Unlock()
	return done
}

func (h *Harness) waitReady(p *Proc, done chan struct{}) {
	h.t.Helper()
	deadline := time.Now().Add(readyWait)
	for {
		resp, err := h.http.Get("http://" + p.httpAddr + "/readyz") //nolint:noctx // bounded by the client timeout
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		select {
		case <-done:
			h.t.Fatalf("e2e: %s exited before it was ready; log:\n%s", p.Name, tail(p.logPath, 40))
		case <-time.After(50 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("e2e: %s not ready within %s; log:\n%s", p.Name, readyWait, tail(p.logPath, 40))
		}
	}
}

// Running reports whether p's current incarnation is alive (possibly stopped by SIGSTOP).
func (p *Proc) Running() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.done == nil {
		return false
	}
	select {
	case <-p.done:
		return false
	default:
		return true
	}
}

func (h *Harness) stopAll() {
	h.mu.Lock()
	procs := append([]*Proc(nil), h.procs...)
	h.mu.Unlock()
	for _, p := range procs {
		h.Kill(p)
	}
	if h.t.Failed() {
		for _, p := range procs {
			h.t.Logf("=== %s log tail (%s):\n%s", p.Name, p.logPath, tail(p.logPath, 60))
		}
	}
}

// tail returns the last n lines of the file at path.
func tail(path string, n int) string {
	raw, err := os.ReadFile(path) //nolint:gosec // a log this harness wrote
	if err != nil {
		return err.Error()
	}
	lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

// Do calls the REST API with the admin key. body is JSON-encoded unless it is []byte; a 2xx
// answer is decoded into out (when non-nil). It returns the status and the raw answer.
func (h *Harness) Do(method, path string, body, out any, headers ...string) (int, []byte, error) {
	var rd io.Reader
	switch b := body.(type) {
	case nil:
	case []byte:
		rd = bytes.NewReader(b)
	default:
		raw, err := json.Marshal(b)
		if err != nil {
			return 0, nil, err
		}
		rd = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(h.t.Context(), method, h.APIURL+path, rd)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+h.AdminKey)
	if rd != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := h.http.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, err
	}
	if out != nil && resp.StatusCode < 300 && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			return resp.StatusCode, raw, fmt.Errorf("decode %s %s: %w", method, path, err)
		}
	}
	return resp.StatusCode, raw, nil
}

// MustDo is Do that fails the test unless the status is want.
func (h *Harness) MustDo(method, path string, body, out any, want int, headers ...string) {
	h.t.Helper()
	status, raw, err := h.Do(method, path, body, out, headers...)
	if err != nil || status != want {
		h.t.Fatalf("e2e: %s %s: status %d (want %d), err %v, body %s", method, path, status, want, err, raw)
	}
}

// CLI runs flowforgectl against this deployment as admin.
func (h *Harness) CLI(args ...string) (code int, stdout, stderr string) {
	h.t.Helper()
	cmd := exec.CommandContext(h.t.Context(), filepath.Join(binDir, "flowforgectl"), args...) //nolint:gosec // a binary this suite built
	cmd.Env = []string{"FF_API_URL=" + h.APIURL, "FF_API_KEY=" + h.AdminKey}
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	err := cmd.Run()
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return ee.ExitCode(), out.String(), errb.String()
	} else if err != nil {
		h.t.Fatalf("e2e: run flowforgectl: %v", err)
	}
	return 0, out.String(), errb.String()
}
