package rest

import (
	"context"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/daishakpatel/flowforge/internal/api/auth"
	"github.com/daishakpatel/flowforge/internal/logging"
	"github.com/daishakpatel/flowforge/internal/persistence"
)

// Audit outcomes (audit_log.outcome).
const (
	auditOK     = "ok"
	auditDenied = "denied"
	auditError  = "error"
)

const auditWriteTimeout = 2 * time.Second

// auditState lets the outer audit middleware learn who the caller was: the auth middleware,
// which runs inside it, fills in the principal.
type auditState struct{ principal *auth.Principal }

type auditStateKey struct{}

func setAuditPrincipal(ctx context.Context, p auth.Principal) {
	if s, ok := ctx.Value(auditStateKey{}).(*auditState); ok {
		s.principal = &p
	}
}

// auditMiddleware records an audit_log row for every non-GET request and every denied request
// (401 or 403), with the actor, the action ("METHOD route"), the resource, the request id and
// the outcome (FR-SEC-03). Details hold only the status and the body size: never a body, query
// or header, so no secret can reach the audit log.
//
// The row is written after the response, with its own short deadline. A failed write is logged
// at error level and counted in flowforge_audit_write_failures_total, and the request is not
// affected: availability over audit completeness (docs/api/authentication.md).
func auditMiddleware(audit persistence.AuditRepo, db persistence.Querier, failures prometheus.Counter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			state := &auditState{}
			ctx := context.WithValue(r.Context(), auditStateKey{}, state)
			body := &countingReader{r: r.Body}
			r.Body = body
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			next.ServeHTTP(ww, r.WithContext(ctx))

			status := ww.Status()
			if status == 0 {
				status = http.StatusOK
			}
			outcome := auditOutcome(status)
			if r.Method == http.MethodGet && outcome != auditDenied {
				return
			}
			e := auditEntry(r, ww.Header().Get("Location"), status, outcome, body.n)
			if p := state.principal; p != nil {
				e.ActorKeyID, e.ActorRole = p.KeyID, p.Role
			}
			wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), auditWriteTimeout)
			defer cancel()
			if err := audit.Record(wctx, db, e); err != nil {
				failures.Inc()
				logging.FromContext(ctx).ErrorContext(ctx, "audit write failed", "action", e.Action, logging.Err(err))
			}
		})
	}
}

func auditOutcome(status int) string {
	switch {
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return auditDenied
	case status >= http.StatusBadRequest:
		return auditError
	}
	return auditOK
}

// auditEntry builds the row. The resource is the one the route names: what a 201's Location
// points at, else the route's first path parameter, typed by the collection it sits in.
func auditEntry(r *http.Request, location string, status int, outcome string, bodyBytes int64) persistence.AuditEntry {
	rctx := chi.RouteContext(r.Context())
	if rctx == nil { // outside a chi router (only in tests); fall back to the raw path
		rctx = chi.NewRouteContext()
	}
	pattern := rctx.RoutePattern()
	if pattern == "" {
		pattern = r.URL.Path
	}
	e := persistence.AuditEntry{
		Action: r.Method + " " + pattern, RequestID: requestID(r.Context()), Outcome: outcome,
		Details: map[string]any{"status": status, "body_bytes": bodyBytes},
	}
	if typ, id, ok := strings.Cut(strings.TrimPrefix(location, "/api/v1/"), "/"); ok && location != "" {
		e.ResourceType, e.ResourceID = typ, id
		return e
	}
	if typ, _, _ := strings.Cut(strings.TrimPrefix(pattern, "/api/v1/"), "/"); typ != "" {
		e.ResourceType = typ
	}
	for i, k := range rctx.URLParams.Keys { // skip the "*" a mounted subrouter adds
		if k != "*" && rctx.URLParams.Values[i] != "" {
			e.ResourceID = rctx.URLParams.Values[i]
			break
		}
	}
	return e
}

// countingReader counts the bytes a handler reads from the request body.
type countingReader struct {
	r io.ReadCloser
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

func (c *countingReader) Close() error { return c.r.Close() }
