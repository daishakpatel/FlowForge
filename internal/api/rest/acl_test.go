package rest

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/daishakpatel/flowforge/internal/api/auth"
	"github.com/daishakpatel/flowforge/internal/clock"
)

// TestACL_EveryRouteHasAnEntry is the CI hook of FR-SEC-02: every /api/v1 route registered on
// the router has an ACL entry, and every entry names a registered route.
func TestACL_EveryRouteHasAnEntry(t *testing.T) {
	t.Parallel()
	acl := routeACL()
	var routes []string
	err := chi.Walk(newUnauthRouter(t), func(method, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		if strings.HasPrefix(route, "/api/v1/") {
			routes = append(routes, method+" "+route)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range routes {
		if _, ok := acl[r]; !ok {
			t.Errorf("route %s has no entry in routeACL (routes_acl.go)", r)
		}
	}
	for entry, role := range acl {
		if !slices.Contains(routes, entry) {
			t.Errorf("routeACL entry %s matches no route", entry)
		}
		if !slices.Contains(auth.Roles(), role) {
			t.Errorf("routeACL entry %s has unknown role %q", entry, role)
		}
	}
}

func TestRequiredRole_UnknownRouteIsAdminOnly(t *testing.T) {
	t.Parallel()
	if got := requiredRole(routeACL(), "DELETE", "/api/v1/runs/{run_id}"); got != auth.RoleAdmin {
		t.Errorf("unlisted route requires %q, want admin", got)
	}
	if got := requiredRole(routeACL(), "GET", "/api/v1/runs"); got != auth.RoleViewer {
		t.Errorf("GET /api/v1/runs requires %q, want viewer", got)
	}
}

func TestLimiter_BurstRefillAndSeparateBuckets(t *testing.T) {
	t.Parallel()
	clk := clock.NewFake(time.Unix(0, 0))
	l := newLimiter(clk, 2, 4) // 2/s, burst 4
	for i := range 4 {
		if ok, _ := l.allow("a", auth.RoleViewer); !ok {
			t.Fatalf("request %d within the burst denied", i)
		}
	}
	ok, wait := l.allow("a", auth.RoleViewer)
	if ok || wait != 500*time.Millisecond {
		t.Errorf("after the burst = %v, wait %v; want denied, 500ms", ok, wait)
	}
	if ok, _ := l.allow("b", auth.RoleViewer); !ok {
		t.Error("key b denied by key a's bucket")
	}
	clk.Advance(500 * time.Millisecond)
	if ok, _ := l.allow("a", auth.RoleViewer); !ok {
		t.Error("no token after the refill interval")
	}
	for i := range 4 * serviceRoleFactor {
		if ok, _ := l.allow("svc", auth.RoleWorker); !ok {
			t.Fatalf("service key request %d within its larger burst denied", i)
		}
	}
	if ok, _ := l.allow("svc", auth.RoleWorker); ok {
		t.Error("service key exceeded 4x the burst")
	}
}

// TestRateLimit_429WithRetryAfterAndHealthExempt runs a router whose limit never grants a token.
func TestRateLimit_429WithRetryAfterAndHealthExempt(t *testing.T) {
	t.Parallel()
	d := testDeps(auth.NewStub(true), nil, nil, func(context.Context) error { return nil })
	d.RateLimitRPS = 0.001 // burst 0.002 < 1 token
	r, err := NewRouter(d)
	if err != nil {
		t.Fatal(err)
	}
	c := newContract(t)
	rec := c.do(t, r, "GET", "/api/v1/runs", nil)
	if rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") == "" {
		t.Errorf("limited request = %d (Retry-After %q), want 429 with Retry-After", rec.Code, rec.Header().Get("Retry-After"))
	}
	for range 5 {
		if code := c.do(t, r, "GET", "/healthz", nil).Code; code != http.StatusOK {
			t.Errorf("/healthz under the limit = %d, want 200 (exempt)", code)
		}
	}
}

func TestAudit_RecordsMutationsAndDenialsOnly(t *testing.T) {
	t.Parallel()
	d := testDeps(auth.NewStub(false), nil, nil, func(context.Context) error { return nil })
	mem := d.Audit.(*memAudit)
	r, err := NewRouter(d)
	if err != nil {
		t.Fatal(err)
	}
	send := func(method, path string) {
		req := httptest.NewRequestWithContext(context.Background(), method, path, strings.NewReader(`{"secret":"x"}`))
		r.ServeHTTP(httptest.NewRecorder(), req)
	}
	send("GET", "/healthz")               // operational, not audited
	send("POST", "/api/v1/workflows")     // 401 → denied
	send("GET", "/api/v1/runs/run_42")    // 401 → denied, even for a GET
	send("DELETE", "/api/v1/api-keys/k1") // 401 → denied
	if len(mem.entries) != 3 {
		t.Fatalf("audit entries = %d (%+v), want 3", len(mem.entries), mem.entries)
	}
	e := mem.entries[1]
	if e.Action != "GET /api/v1/runs/{run_id}" || e.ResourceType != "runs" || e.ResourceID != "run_42" ||
		e.Outcome != auditDenied || e.ActorKeyID != "" || !strings.HasPrefix(e.RequestID, "req_") {
		t.Errorf("audit entry = %+v", e)
	}
	for _, e := range mem.entries {
		for k := range e.Details {
			if k != "status" && k != "body_bytes" {
				t.Errorf("audit details carry %q; only status and body_bytes are allowed", k)
			}
		}
	}
}

func TestAudit_WriteFailureIsCountedAndRequestProceeds(t *testing.T) {
	t.Parallel()
	failures := prometheus.NewCounter(prometheus.CounterOpts{Name: "test_audit_failures_total"})
	mem := &memAudit{fail: errors.New("audit table locked")}
	h := auditMiddleware(mem, nil, failures)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), "POST", "/api/v1/workflows", nil))
	if rec.Code != http.StatusCreated {
		t.Errorf("status = %d, want the handler's 201", rec.Code)
	}
	if got := promtest.ToFloat64(failures); got != 1 {
		t.Errorf("audit failures counter = %v, want 1", got)
	}
}
