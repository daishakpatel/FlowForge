package rest

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers"
	"github.com/getkin/kin-openapi/routers/legacy"

	"github.com/daishakpatel/flowforge/api/openapi"
	"github.com/daishakpatel/flowforge/internal/api/auth"
	"github.com/daishakpatel/flowforge/internal/api/service"
	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/ids"
	"github.com/daishakpatel/flowforge/internal/obs"
	"github.com/daishakpatel/flowforge/internal/persistence"
)

// contract validates every response a test receives against the OpenAPI document and records
// which (operation, status) pairs were seen, so a test can demand every documented response.
type contract struct {
	doc    *openapi3.T
	router routers.Router

	mu   sync.Mutex
	seen map[string]map[int]bool // operationId → status
}

func loadSpec(t *testing.T) *openapi3.T {
	t.Helper()
	doc, err := openapi3.NewLoader().LoadFromData(openapi.Spec)
	if err != nil {
		t.Fatalf("load openapi spec: %v", err)
	}
	if err := doc.Validate(context.Background()); err != nil {
		t.Fatalf("openapi spec is invalid: %v", err)
	}
	return doc
}

func newContract(t *testing.T) *contract {
	t.Helper()
	doc := loadSpec(t)
	router, err := legacy.NewRouter(doc)
	if err != nil {
		t.Fatalf("openapi router: %v", err)
	}
	return &contract{doc: doc, router: router, seen: map[string]map[int]bool{}}
}

// do sends a request to h and fails the test unless the response matches the spec.
func (c *contract) do(t *testing.T, h http.Handler, method, path string, body []byte, headers ...string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(context.Background(), method, path, bytes.NewReader(body))
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	c.check(t, req, rec)
	return rec
}

// reporter is the part of testing.TB that check uses, so a test can capture its failures.
type reporter interface {
	Helper()
	Errorf(format string, args ...any)
}

func (c *contract) check(t reporter, req *http.Request, rec *httptest.ResponseRecorder) {
	t.Helper()
	if rec.Header().Get("X-Request-ID") == "" {
		t.Errorf("%s %s: no X-Request-ID header", req.Method, req.URL.Path)
	}
	route, params, err := c.router.FindRoute(req)
	if err != nil {
		t.Errorf("%s %s: no OpenAPI operation: %v", req.Method, req.URL.Path, err)
		return
	}
	in := &openapi3filter.RequestValidationInput{Request: req, PathParams: params, Route: route}
	err = openapi3filter.ValidateResponse(req.Context(), &openapi3filter.ResponseValidationInput{
		RequestValidationInput: in, Status: rec.Code, Header: rec.Header(),
		Body:    io.NopCloser(bytes.NewReader(rec.Body.Bytes())),
		Options: &openapi3filter.Options{IncludeResponseStatus: true},
	})
	if err != nil {
		t.Errorf("%s %s → %d violates the OpenAPI contract: %v\nbody: %s", req.Method, req.URL.Path, rec.Code, err, rec.Body.String())
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	op := route.Operation.OperationID
	if c.seen[op] == nil {
		c.seen[op] = map[int]bool{}
	}
	c.seen[op][rec.Code] = true
}

// operations returns every (method, path, operation) of the spec, sorted.
func (c *contract) operations() []specOp {
	var ops []specOp
	for path, item := range c.doc.Paths.Map() {
		for method, op := range item.Operations() {
			ops = append(ops, specOp{method: method, path: path, op: op})
		}
	}
	sort.Slice(ops, func(i, j int) bool { return ops[i].path+ops[i].method < ops[j].path+ops[j].method })
	return ops
}

type specOp struct {
	method, path string
	op           *openapi3.Operation
}

// samplePath fills path parameters with placeholder ids.
func (s specOp) samplePath() string {
	r := strings.NewReplacer("{id}", "wf_x", "{name}", "nope", "{run_id}", "run_x", "{task_id}", "task_x", "{key_id}", "nokeyhere234")
	return r.Replace(s.path)
}

// missing lists every documented numeric response no test produced.
func (c *contract) missing() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, s := range c.operations() {
		for code := range s.op.Responses.Map() {
			n, err := strconv.Atoi(code)
			if err != nil {
				continue // default
			}
			if !c.seen[s.op.OperationID][n] {
				out = append(out, s.op.OperationID+" "+code)
			}
		}
	}
	slices.Sort(out)
	return out
}

// testDeps returns router dependencies with fakes for everything but the services. The rate
// limit is high enough never to trigger; audit rows go to memAudit.
func testDeps(a auth.Authenticator, wf *service.WorkflowService, runs *service.RunService, ready func(context.Context) error) Deps {
	return Deps{
		Workflows: wf, Runs: runs, Auth: a, IDs: &ids.Fake{},
		Clock: clock.NewFake(time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC)), Logger: slog.New(slog.DiscardHandler),
		Metrics: obs.NewRegistry(), Ready: ready, Audit: &memAudit{}, RateLimitRPS: 1e6,
	}
}

// memAudit is an in-memory persistence.AuditRepo.
type memAudit struct {
	mu      sync.Mutex
	entries []persistence.AuditEntry
	fail    error
}

func (m *memAudit) Record(_ context.Context, _ persistence.Querier, e persistence.AuditEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = append(m.entries, e)
	return m.fail
}
