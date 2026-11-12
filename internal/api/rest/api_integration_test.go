//go:build integration

package rest

import (
	"context"
	crand "crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daishakpatel/flowforge/internal/api/auth"
	"github.com/daishakpatel/flowforge/internal/api/service"
	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/config"
	"github.com/daishakpatel/flowforge/internal/events"
	"github.com/daishakpatel/flowforge/internal/ids"
	"github.com/daishakpatel/flowforge/internal/obs"
	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/persistence/pg"
	"github.com/daishakpatel/flowforge/internal/testutil"
)

// apiEnv is the API wired to its own migrated database, as cmd/flowforge-api wires it, with one
// API key per role. router authenticates as the admin key unless a request carries its own
// Authorization header; raw is the router itself.
type apiEnv struct {
	db     *testutil.PG
	pool   *pgxpool.Pool
	router http.Handler
	raw    chi.Router
	runs   *service.RunService
	deps   Deps
	clk    *clock.Fake
	authn  *auth.KeyAuthenticator
	tokens map[string]string // role → token
}

// bearer returns the header arguments for c.do that authenticate as role.
func (e *apiEnv) bearer(role string) []string {
	return []string{"Authorization", "Bearer " + e.tokens[role]}
}

func (e *apiEnv) keyID(role string) string { id, _, _ := auth.ParseToken(e.tokens[role]); return id }

// newKey stores a new key for role and returns its token.
func (e *apiEnv) newKey(t *testing.T, role string, expires *time.Time) string {
	t.Helper()
	k, err := auth.GenerateKey(crand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	_, err = pg.APIKeyRepo{}.Create(t.Context(), e.pool, persistence.NewAPIKey{
		ID: k.ID, Name: role + "-test", Role: role, SecretHash: k.Hash, Salt: k.Salt, CreatedBy: "test", ExpiresAt: expires,
	})
	if err != nil {
		t.Fatal(err)
	}
	return k.Token
}

// router builds another router on the same database with changed dependencies.
func (e *apiEnv) routerWith(t *testing.T, change func(*Deps)) chi.Router {
	t.Helper()
	d := e.deps
	d.Metrics = obs.NewRegistry()
	change(&d)
	r, err := NewRouter(d)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func newAPIEnv(t *testing.T) *apiEnv {
	t.Helper()
	db := testutil.NewFlowforgeDB(t)
	pool, err := pg.NewPool(t.Context(), config.Secret(db.DSN))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	g := &ids.Fake{}
	rec := events.NewRecorder(clock.Real{}, g, "flowforge-api")
	wf := &service.WorkflowService{DB: pool, DefRepo: pg.NewDefinitionRepo(g)}
	runs := &service.RunService{
		DB: pool, Tx: pg.NewTxManager(pool, clock.Real{}, nil), DefRepo: pg.NewDefinitionRepo(g),
		RunRepo: pg.NewRunRepo(g, rec), TaskRepo: pg.NewTaskRepo(rec), AttemptRepo: pg.NewAttemptRepo(g, rec),
		EventRepo: pg.EventRepo{},
	}
	deps := testDeps(nil, wf, runs, func(ctx context.Context) error { return pool.Ping(ctx) })
	clk := deps.Clock.(*clock.Fake)
	authn := auth.NewKeyAuthenticator(pool, pg.APIKeyRepo{}, clk)
	deps.Auth, deps.DB, deps.Tx, deps.APIKeys, deps.Audit = authn, pool, runs.Tx, pg.APIKeyRepo{}, pg.AuditRepo{}
	deps.Workers, deps.Logs, deps.WorkerLeaseTTL = pg.WorkerRepo{}, pg.TaskLogRepo{}, 15*time.Second
	raw, err := NewRouter(deps)
	if err != nil {
		t.Fatal(err)
	}
	e := &apiEnv{db: db, pool: pool, raw: raw, runs: runs, deps: deps, clk: clk, authn: authn, tokens: map[string]string{}}
	for _, role := range auth.Roles() {
		e.tokens[role] = e.newKey(t, role, nil)
	}
	e.router = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			r.Header.Set("Authorization", "Bearer "+e.tokens[auth.RoleAdmin])
		}
		raw.ServeHTTP(w, r)
	})
	return e
}

func decodeBody[T any](t *testing.T, body []byte) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(body, &v); err != nil {
		t.Fatalf("decode %s: %v", body, err)
	}
	return v
}

func want(t *testing.T, got, wantCode int, what string) {
	t.Helper()
	if got != wantCode {
		t.Errorf("%s: status %d, want %d", what, got, wantCode)
	}
}

// marketDataHourly is the 00E §3.5 example definition.
func marketDataHourly(t *testing.T) []byte {
	t.Helper()
	b, err := os.ReadFile("../../workflow/model/testdata/market-data-hourly.json")
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestContract_API exercises every operation's success and error responses against a real
// database; every response is validated against the OpenAPI document, and the test fails if any
// documented response was not produced (make openapi-check step 3).
func TestContract_API(t *testing.T) {
	e := newAPIEnv(t)
	c := newContract(t)
	r := e.router
	do := func(method, path string, body []byte, headers ...string) (int, []byte, http.Header) {
		rec := c.do(t, r, method, path, body, headers...)
		return rec.Code, rec.Body.Bytes(), rec.Header()
	}

	for _, p := range []string{"/healthz", "/readyz", "/metrics", "/openapi.json"} {
		code, _, _ := do("GET", p, nil)
		want(t, code, 200, p)
	}

	// Definitions.
	code, body, h := do("POST", "/api/v1/workflows", marketDataHourly(t))
	want(t, code, 201, "create market-data-hourly")
	wf := decodeBody[workflowResponse](t, body)
	if h.Get("Location") != "/api/v1/workflows/"+wf.ID || wf.Version != 1 || wf.CreatedBy != e.keyID(auth.RoleAdmin) {
		t.Errorf("created workflow = %+v, Location %q", wf, h.Get("Location"))
	}
	code, _, _ = do("POST", "/api/v1/workflows", marketDataHourly(t))
	want(t, code, 409, "duplicate (name, version)")
	code, body, _ = do("POST", "/api/v1/workflows", []byte(`{"name":"market-data-hourly","tasks":[{"id":"a","type":"t"}]}`))
	want(t, code, 201, "omitted version")
	if v := decodeBody[workflowResponse](t, body); v.Version != 2 {
		t.Errorf("omitted version became %d, want 2", v.Version)
	}
	code, _, _ = do("POST", "/api/v1/workflows", []byte(`{"name":"x","version":1,"tasks":[],"surprise":true}`))
	want(t, code, 400, "unknown field")
	checkIssue := func(def, issue string) {
		t.Helper()
		code, body, _ := do("POST", "/api/v1/workflows", []byte(def))
		want(t, code, 422, issue)
		env := decodeBody[errorEnvelope](t, body)
		found := false
		for _, d := range env.Error.Details {
			found = found || (d.Code == issue && d.TaskID != "")
		}
		if env.Error.Code != "INVALID_WORKFLOW" || !found {
			t.Errorf("%s: envelope %+v lacks a %s detail with a task id", issue, env.Error, issue)
		}
	}
	checkIssue(`{"name":"cyc","version":1,"tasks":[{"id":"a","type":"t","depends_on":["b"]},{"id":"b","type":"t","depends_on":["a"]}]}`, "CYCLE")
	checkIssue(`{"name":"dup","version":1,"tasks":[{"id":"a","type":"t"},{"id":"a","type":"t"}]}`, "DUPLICATE_TASK_ID")
	big := fmt.Sprintf(`{"name":"big","version":1,"tasks":[{"id":"a","type":"t","config":{"pad":%q}}]}`, strings.Repeat("x", 600<<10))
	code, _, _ = do("POST", "/api/v1/workflows", []byte(big))
	want(t, code, 413, "definition over 512 KiB")
	code, _, _ = do("POST", "/api/v1/workflows", []byte(strings.Repeat(" ", MaxBodyBytes+1)))
	want(t, code, 413, "body over 1 MiB")

	code, body, _ = do("GET", "/api/v1/workflows?limit=1", nil)
	want(t, code, 200, "list workflows")
	if l := decodeBody[listResponse[workflowResponse]](t, body); len(l.Items) != 1 || l.NextCursor == nil {
		t.Errorf("first page = %d items, next %v", len(l.Items), l.NextCursor)
	}
	code, _, _ = do("GET", "/api/v1/workflows?cursor=garbage!", nil)
	want(t, code, 400, "bad cursor")
	code, _, _ = do("GET", "/api/v1/workflows/"+wf.ID, nil)
	want(t, code, 200, "get workflow")
	code, _, _ = do("GET", "/api/v1/workflows/wf_missing", nil)
	want(t, code, 404, "missing workflow")
	code, body, _ = do("GET", "/api/v1/workflows/by-name/market-data-hourly", nil)
	want(t, code, 200, "by name latest")
	if v := decodeBody[workflowResponse](t, body); v.Version != 2 {
		t.Errorf("latest version = %d, want 2", v.Version)
	}
	code, _, _ = do("GET", "/api/v1/workflows/by-name/market-data-hourly?version=1", nil)
	want(t, code, 200, "by name v1")
	code, _, _ = do("GET", "/api/v1/workflows/by-name/market-data-hourly?version=one", nil)
	want(t, code, 400, "bad version")
	code, _, _ = do("GET", "/api/v1/workflows/by-name/nope", nil)
	want(t, code, 404, "unknown name")

	// Runs.
	runsPath := "/api/v1/workflows/" + wf.ID + "/runs"
	code, body, h = do("POST", runsPath, nil)
	want(t, code, 201, "start run without body")
	run := decodeBody[runResponse](t, body)
	if h.Get("Location") != "/api/v1/runs/"+run.ID || run.Status != "CREATED" || run.CreatedBy != e.keyID(auth.RoleAdmin) {
		t.Errorf("started run = %+v, Location %q", run, h.Get("Location"))
	}
	input := []byte(`{"input":{"symbols":["AAPL"]}}`)
	code, body, _ = do("POST", runsPath, input, "Idempotency-Key", "key-1")
	want(t, code, 201, "start with key")
	first := decodeBody[runResponse](t, body)
	code, body, h = do("POST", runsPath, []byte(`{"input": {"symbols": ["AAPL"]}}`), "Idempotency-Key", "key-1")
	want(t, code, 200, "replay")
	if replay := decodeBody[runResponse](t, body); replay.ID != first.ID || h.Get("Idempotent-Replay") != "true" {
		t.Errorf("replay = %s (header %q), want %s with Idempotent-Replay: true", replay.ID, h.Get("Idempotent-Replay"), first.ID)
	}
	code, _, _ = do("POST", runsPath, []byte(`{"input":{"symbols":["MSFT"]}}`), "Idempotency-Key", "key-1")
	want(t, code, 409, "same key, different input")
	code, _, _ = do("POST", runsPath, []byte(`{"input":[1]}`))
	want(t, code, 400, "array input")
	code, _, _ = do("POST", runsPath, []byte(fmt.Sprintf(`{"input":{"pad":%q}}`, strings.Repeat("x", 300<<10))))
	want(t, code, 413, "input over 256 KiB")
	code, _, _ = do("POST", "/api/v1/workflows/wf_missing/runs", nil)
	want(t, code, 404, "run of a missing workflow")

	code, _, _ = do("GET", "/api/v1/runs?status=CREATED&workflow=market-data-hourly&since=2020-01-01T00:00:00Z", nil)
	want(t, code, 200, "list runs")
	code, _, _ = do("GET", "/api/v1/runs?status=BOGUS", nil)
	want(t, code, 400, "bad status filter")
	code, _, _ = do("GET", "/api/v1/runs/"+run.ID, nil)
	want(t, code, 200, "get run")
	code, _, _ = do("GET", "/api/v1/runs/run_missing", nil)
	want(t, code, 404, "missing run")

	code, body, _ = do("GET", "/api/v1/runs/"+run.ID+"/tasks", nil)
	want(t, code, 200, "run tasks")
	tasks := decodeBody[listResponse[taskResponse]](t, body)
	if len(tasks.Items) != 1 || tasks.Items[0].Key != "fetch" || tasks.Items[0].Status != "PENDING" {
		t.Fatalf("run tasks = %+v", tasks.Items)
	}
	code, _, _ = do("GET", "/api/v1/runs/run_missing/tasks", nil)
	want(t, code, 404, "tasks of a missing run")
	code, body, _ = do("GET", "/api/v1/runs/"+run.ID+"/events?limit=1", nil)
	want(t, code, 200, "run events")
	if evs := decodeBody[listResponse[eventResponse]](t, body); len(evs.Items) != 1 || evs.Items[0].Type != "run.created" || evs.NextCursor == nil {
		t.Errorf("events page = %+v", evs)
	} else {
		code, body, _ = do("GET", "/api/v1/runs/"+run.ID+"/events?cursor="+*evs.NextCursor, nil)
		want(t, code, 200, "events page 2")
		if evs2 := decodeBody[listResponse[eventResponse]](t, body); len(evs2.Items) != 0 || evs2.NextCursor != nil {
			t.Errorf("events after the only event = %+v", evs2)
		}
	}
	code, _, _ = do("GET", "/api/v1/runs/"+run.ID+"/events?cursor=garbage", nil)
	want(t, code, 400, "bad events cursor")
	code, _, _ = do("GET", "/api/v1/runs/run_missing/events", nil)
	want(t, code, 404, "events of a missing run")

	taskID := tasks.Items[0].ID
	err := e.runs.Tx.WithTx(t.Context(), pgx.TxOptions{}, func(tx pgx.Tx) error {
		_, err := e.runs.AttemptRepo.Create(t.Context(), tx, persistence.NewAttempt{TaskID: taskID, RunID: run.ID, AttemptNumber: 1, WorkerID: "w1", TimeoutSeconds: 60})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	code, _, _ = do("GET", "/api/v1/tasks/"+taskID, nil)
	want(t, code, 200, "get task")
	code, _, _ = do("GET", "/api/v1/tasks/task_missing", nil)
	want(t, code, 404, "missing task")
	code, body, _ = do("GET", "/api/v1/tasks/"+taskID+"/attempts", nil)
	want(t, code, 200, "attempts")
	if a := decodeBody[listResponse[attemptResponse]](t, body); len(a.Items) != 1 || a.Items[0].Status != "RUNNING" {
		t.Errorf("attempts = %+v", a.Items)
	}
	code, _, _ = do("GET", "/api/v1/tasks/task_missing/attempts", nil)
	want(t, code, 404, "attempts of a missing task")

	// Workers and attempt logs (03.01).
	code, _, _ = do("GET", "/api/v1/workers", nil)
	want(t, code, 200, "list workers")
	_, body, _ = do("GET", "/api/v1/tasks/"+taskID+"/attempts", nil)
	attID := decodeBody[listResponse[attemptResponse]](t, body).Items[0].ID
	err = pg.TaskLogRepo{}.Insert(t.Context(), e.pool, []persistence.NewTaskLog{{
		AttemptID: attID, TaskID: taskID, RunID: run.ID, TS: time.Now(), Level: "INFO", Message: "hello", //nolint:forbidigo // test fixture
		Fields: []byte(`{"k":1}`),
	}})
	if err != nil {
		t.Fatal(err)
	}
	code, body, _ = do("GET", "/api/v1/attempts/"+attID+"/logs?limit=1", nil)
	want(t, code, 200, "attempt logs")
	if l := decodeBody[listResponse[logLineResponse]](t, body); len(l.Items) != 1 || l.Items[0].Message != "hello" || l.NextCursor == nil {
		t.Errorf("attempt logs = %+v", l)
	}
	code, _, _ = do("GET", "/api/v1/attempts/"+attID+"/logs?after_id=-1", nil)
	want(t, code, 400, "negative after_id")
	code, _, _ = do("GET", "/api/v1/attempts/att_missing/logs", nil)
	want(t, code, 404, "logs of a missing attempt")

	// API keys.
	code, body, h = do("POST", "/api/v1/api-keys", []byte(`{"name":"ci","role":"operator"}`))
	want(t, code, 201, "create api key")
	created := decodeBody[createdAPIKeyResponse](t, body)
	if created.Role != "operator" || h.Get("Location") != "/api/v1/api-keys/"+created.ID || !strings.HasPrefix(created.Key, "ff_"+created.ID+"_") {
		t.Errorf("created key = %+v, Location %q", created.apiKeyResponse, h.Get("Location"))
	}
	code, _, _ = do("POST", "/api/v1/api-keys", []byte(`{"name":"x","role":"root"}`))
	want(t, code, 400, "unknown role")
	code, body, _ = do("GET", "/api/v1/api-keys", nil)
	want(t, code, 200, "list api keys")
	if strings.Contains(string(body), "ff_") || strings.Contains(string(body), `"key"`) || strings.Contains(string(body), "hash") {
		t.Errorf("key list leaks secrets: %s", body)
	}
	code, _, _ = do("GET", "/api/v1/api-keys?cursor=garbage!", nil)
	want(t, code, 400, "bad api-keys cursor")
	code, _, _ = do("DELETE", "/api/v1/api-keys/"+created.ID, nil)
	want(t, code, 204, "revoke key")
	code, _, _ = do("DELETE", "/api/v1/api-keys/"+created.ID, nil)
	want(t, code, 204, "revoke again (idempotent)")
	code, _, _ = do("DELETE", "/api/v1/api-keys/nokeyhere234", nil)
	want(t, code, 404, "revoke unknown key")
	code, _, _ = do("DELETE", "/api/v1/api-keys/"+e.keyID(auth.RoleAdmin), nil)
	want(t, code, 409, "revoke the last admin key")

	// Every /api/v1 operation: 401 without a key, 403 for a worker key (no human route allows
	// it), 429 once the key's bucket is empty.
	limited := e.routerWith(t, func(d *Deps) { d.RateLimitRPS = 0.001 })
	for _, s := range c.operations() {
		if !strings.HasPrefix(s.path, "/api/v1/") {
			continue
		}
		what := s.method + " " + s.path
		want(t, c.do(t, e.raw, s.method, s.samplePath(), nil).Code, 401, what+" without a key")
		want(t, c.do(t, e.raw, s.method, s.samplePath(), nil, e.bearer(auth.RoleWorker)...).Code, 403, what+" as worker")
		want(t, c.do(t, limited, s.method, s.samplePath(), nil, e.bearer(auth.RoleAdmin)...).Code, 429, what+" rate limited")
	}

	// Postgres gone: /readyz is 503 with Retry-After, /healthz stays 200, writes are 503.
	dropDatabase(t, e.db)
	code, body, h = do("GET", "/readyz", nil)
	want(t, code, 503, "readyz with the database gone")
	if h.Get("Retry-After") == "" || decodeBody[errorEnvelope](t, body).Error.Code != "UNAVAILABLE" {
		t.Errorf("readyz 503 lacks Retry-After or UNAVAILABLE: %s %v", body, h)
	}
	code, _, _ = do("GET", "/healthz", nil)
	want(t, code, 200, "healthz with the database gone")
	code, _, h = do("POST", "/api/v1/workflows", []byte(`{"name":"late","version":1,"tasks":[{"id":"a","type":"t"}]}`))
	if code != 503 || h.Get("Retry-After") == "" {
		t.Errorf("write with the database gone = %d (Retry-After %q), want 503 with Retry-After", code, h.Get("Retry-After"))
	}

	if m := c.missing(); len(m) > 0 {
		t.Errorf("documented responses never produced: %v", m)
	}
}

func dropDatabase(t *testing.T, db *testutil.PG) {
	t.Helper()
	conn, err := pgx.Connect(t.Context(), db.AdminDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	if _, err := conn.Exec(t.Context(), "DROP DATABASE "+pgx.Identifier{db.Database}.Sanitize()+" WITH (FORCE)"); err != nil {
		t.Fatal(err)
	}
}

// TestStartRun_ConcurrentIdempotentRequestsCreateOneRun: 20 identical requests with one key
// race; exactly one run exists afterwards and every response returns it.
func TestStartRun_ConcurrentIdempotentRequestsCreateOneRun(t *testing.T) {
	e := newAPIEnv(t)
	c := newContract(t)
	rec := c.do(t, e.router, "POST", "/api/v1/workflows", marketDataHourly(t))
	wf := decodeBody[workflowResponse](t, rec.Body.Bytes())

	const n = 20
	ids := make([]string, n)
	codes := make([]int, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			<-start
			rec := c.do(t, e.router, "POST", "/api/v1/workflows/"+wf.ID+"/runs", []byte(`{"input":{"k":1}}`), "Idempotency-Key", "same")
			codes[i] = rec.Code
			ids[i] = decodeBody[runResponse](t, rec.Body.Bytes()).ID
		})
	}
	close(start)
	wg.Wait()

	created := 0
	for i := range n {
		if codes[i] == 201 {
			created++
		} else if codes[i] != 200 {
			t.Errorf("request %d: status %d", i, codes[i])
		}
		if ids[i] != ids[0] {
			t.Errorf("request %d got run %s, request 0 got %s", i, ids[i], ids[0])
		}
	}
	var rows int
	if err := e.pool.QueryRow(t.Context(), `SELECT count(*) FROM workflow_runs`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if created != 1 || rows != 1 {
		t.Errorf("201 responses = %d, run rows = %d; want exactly 1 and 1", created, rows)
	}
}

// TestListRuns_Pagination: 120 runs, limit=50 gives three pages with no duplicate or missing run;
// limit=1000 is clamped to 200, so one page holds all 120.
func TestListRuns_Pagination(t *testing.T) {
	e := newAPIEnv(t)
	c := newContract(t)
	rec := c.do(t, e.router, "POST", "/api/v1/workflows", marketDataHourly(t))
	wf := decodeBody[workflowResponse](t, rec.Body.Bytes())
	const total = 120
	created := map[string]bool{}
	for range total {
		rec := c.do(t, e.router, "POST", "/api/v1/workflows/"+wf.ID+"/runs", nil)
		created[decodeBody[runResponse](t, rec.Body.Bytes()).ID] = true
	}

	seen := map[string]bool{}
	var sizes []int
	cursor := ""
	for {
		path := "/api/v1/runs?limit=50"
		if cursor != "" {
			path += "&cursor=" + cursor
		}
		page := decodeBody[listResponse[runResponse]](t, c.do(t, e.router, "GET", path, nil).Body.Bytes())
		sizes = append(sizes, len(page.Items))
		for _, r := range page.Items {
			if seen[r.ID] {
				t.Errorf("run %s on two pages", r.ID)
			}
			seen[r.ID] = true
		}
		if page.NextCursor == nil {
			break
		}
		cursor = *page.NextCursor
	}
	if fmt.Sprint(sizes) != "[50 50 20]" || len(seen) != total {
		t.Errorf("page sizes = %v, distinct runs = %d; want [50 50 20] and %d", sizes, len(seen), total)
	}
	for id := range created {
		if !seen[id] {
			t.Errorf("run %s missing from the pages", id)
		}
	}
	page := decodeBody[listResponse[runResponse]](t, c.do(t, e.router, "GET", "/api/v1/runs?limit=1000", nil).Body.Bytes())
	if len(page.Items) != total || page.NextCursor != nil {
		t.Errorf("limit=1000 → %d items, next %v; want all %d on one page", len(page.Items), page.NextCursor, total)
	}
}
