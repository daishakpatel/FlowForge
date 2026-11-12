package rest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/daishakpatel/flowforge/internal/api/auth"
	"github.com/daishakpatel/flowforge/internal/apperrors"
)

func newUnauthRouter(t *testing.T) chi.Router {
	t.Helper()
	r, err := NewRouter(testDeps(auth.NewStub(false), nil, nil, func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// TestOpenAPI_RouteParity is make openapi-check step 2: the router serves exactly the spec's
// operations.
func TestOpenAPI_RouteParity(t *testing.T) {
	t.Parallel()
	c := newContract(t)
	var routes, ops []string
	err := chi.Walk(newUnauthRouter(t), func(method, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		routes = append(routes, method+" "+route)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range c.operations() {
		ops = append(ops, s.method+" "+s.path)
	}
	slices.Sort(routes)
	slices.Sort(ops)
	if !slices.Equal(routes, ops) {
		t.Errorf("router and OpenAPI differ\nrouter:  %v\nopenapi: %v", routes, ops)
	}
}

// TestOpenAPI_SpecServedAsJSON checks /openapi.json is the embedded document.
func TestOpenAPI_SpecServedAsJSON(t *testing.T) {
	t.Parallel()
	c := newContract(t)
	rec := c.do(t, newUnauthRouter(t), "GET", "/openapi.json", nil)
	var doc map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &doc); err != nil || doc["openapi"] != "3.1.0" {
		t.Errorf("openapi.json = %.80s…, %v", rec.Body.String(), err)
	}
}

// TestRouter_EveryAPIRouteRequiresAuth: every /api/v1 operation answers 401 with the envelope
// when the caller is not authenticated.
func TestRouter_EveryAPIRouteRequiresAuth(t *testing.T) {
	t.Parallel()
	c := newContract(t)
	r := newUnauthRouter(t)
	for _, s := range c.operations() {
		if !strings.HasPrefix(s.path, "/api/v1/") {
			continue
		}
		rec := c.do(t, r, s.method, s.samplePath(), nil)
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("%s %s = %d, want 401", s.method, s.path, rec.Code)
		}
	}
}

type capture struct{ errs []string }

func (*capture) Helper()                        {}
func (c *capture) Errorf(f string, args ...any) { c.errs = append(c.errs, fmt.Sprintf(f, args...)) }

// TestContract_RejectsViolations guards the contract checker itself: responses that break the
// spec must be reported, or every contract test would pass vacuously.
func TestContract_RejectsViolations(t *testing.T) {
	t.Parallel()
	c := newContract(t)
	cases := map[string]struct {
		path, body string
		status     int
	}{
		"wrong field value":   {"/healthz", `{"status":"meh"}`, 200},
		"extra field":         {"/healthz", `{"status":"ok","x":1}`, 200},
		"envelope missing id": {"/api/v1/runs/run_x", `{"error":{"code":"NOT_FOUND","message":"m","details":[]}}`, 404},
		"undocumented 2xx":    {"/api/v1/runs/run_x", `{}`, 204},
		"unknown error code":  {"/api/v1/runs/run_x", `{"error":{"code":"OOPS","message":"m","details":[],"request_id":"req_1"}}`, 404},
	}
	for name, tc := range cases {
		rec := httptest.NewRecorder()
		rec.Header().Set("Content-Type", "application/json")
		rec.Header().Set("X-Request-ID", "req_1")
		rec.WriteHeader(tc.status)
		rec.Body.WriteString(tc.body)
		var got capture
		c.check(&got, httptest.NewRequestWithContext(context.Background(), "GET", tc.path, nil), rec)
		if len(got.errs) == 0 {
			t.Errorf("%s: violation not reported", name)
		}
	}
}

func TestRouter_RequestIDInHeaderAndEnvelope(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	newUnauthRouter(t).ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), "GET", "/nope", nil))
	var env errorEnvelope
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatal(err)
	}
	id := rec.Header().Get("X-Request-ID")
	if rec.Code != http.StatusNotFound || env.Error.Code != apperrors.CodeNotFound || !strings.HasPrefix(id, "req_") || env.Error.RequestID != id {
		t.Errorf("unknown route = %d %+v, header %q; want 404 envelope carrying the request id", rec.Code, env, id)
	}
}

func TestRecoverMiddleware_PanicIsInternalEnvelope(t *testing.T) {
	t.Parallel()
	h := recoverMiddleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("boom") }))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), "GET", "/", nil))
	if rec.Code != http.StatusInternalServerError || !strings.Contains(rec.Body.String(), `"code":"INTERNAL"`) || strings.Contains(rec.Body.String(), "boom") {
		t.Errorf("panic → %d %s; want a sanitized INTERNAL envelope", rec.Code, rec.Body.String())
	}
}

func TestClassify(t *testing.T) {
	t.Parallel()
	cases := map[string]struct {
		err  error
		want apperrors.Code
	}{
		"max bytes":  {&http.MaxBytesError{Limit: 1}, apperrors.CodePayloadTooLarge},
		"app error":  {apperrors.NotFound("x"), apperrors.CodeNotFound},
		"deadline":   {context.DeadlineExceeded, apperrors.CodeUnavailable},
		"unknown":    {errors.New("pq: secret host 10.0.0.1"), apperrors.CodeInternal},
		"wrapped ok": {errors.Join(errors.New("ctx"), apperrors.Conflict("dup")), apperrors.CodeConflict},
	}
	for name, c := range cases {
		e := classify(c.err)
		if e.Code != c.want {
			t.Errorf("%s: %s, want %s", name, e.Code, c.want)
		}
		if c.want == apperrors.CodeInternal && strings.Contains(e.Message, "10.0.0.1") {
			t.Errorf("%s: message leaks internals: %q", name, e.Message)
		}
	}
}

func TestParseLimit(t *testing.T) {
	t.Parallel()
	for in, want := range map[string]int{"": 50, "1": 1, "200": 200, "1000": 200} {
		if got, err := parseLimit(in); err != nil || got != want {
			t.Errorf("parseLimit(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, in := range []string{"0", "-3", "ten"} {
		if _, err := parseLimit(in); apperrors.CodeOf(err) != apperrors.CodeInvalidArgument {
			t.Errorf("parseLimit(%q) = %v, want INVALID_ARGUMENT", in, err)
		}
	}
}

func TestEventCursor_RoundTrip(t *testing.T) {
	t.Parallel()
	if id, err := decodeEventCursor(encodeEventCursor(42)); err != nil || id != 42 {
		t.Errorf("round trip = %d, %v", id, err)
	}
	for _, bad := range []string{"!", encodeEventCursor(0), "YWJj"} {
		if _, err := decodeEventCursor(bad); apperrors.CodeOf(err) != apperrors.CodeInvalidArgument {
			t.Errorf("decode(%q) = %v, want INVALID_ARGUMENT", bad, err)
		}
	}
}
