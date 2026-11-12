// Package rest is flowforge-api's HTTP layer (task 02.04): the chi router, the middleware chain,
// the 00E §4 error envelope, and handlers that translate between JSON and internal/api/service.
// Handlers contain no SQL and no business rules beyond input parsing.
package rest

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/daishakpatel/flowforge/internal/api/auth"
	"github.com/daishakpatel/flowforge/internal/api/service"
	"github.com/daishakpatel/flowforge/internal/apperrors"
	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/ids"
	"github.com/daishakpatel/flowforge/internal/obs"
	"github.com/daishakpatel/flowforge/internal/persistence"
)

// Deps is what the router needs. All fields are required.
type Deps struct {
	Workflows *service.WorkflowService
	Runs      *service.RunService
	Auth      auth.Authenticator
	IDs       ids.Generator // request ids
	Clock     clock.Clock   // request latency
	Logger    *slog.Logger
	Metrics   *obs.Registry
	// Ready backs /readyz: nil when Postgres is reachable and the schema is current.
	Ready func(context.Context) error

	// Task 02.05: API keys, audit and rate limiting.
	DB           persistence.Querier
	Tx           persistence.TxManager
	APIKeys      persistence.APIKeyRepo
	Audit        persistence.AuditRepo
	RateLimitRPS float64 // per key; burst is twice this (FF_API_RATE_LIMIT_RPS)

	// Task 03.01: workers and task logs.
	Workers        persistence.WorkerRepo
	Logs           persistence.TaskLogRepo
	WorkerLeaseTTL time.Duration // FF_WORKER_LEASE_TTL; older ACTIVE workers are reported LOST
}

type handlers struct {
	workflows *service.WorkflowService
	runs      *service.RunService
	db        persistence.Querier
	tx        persistence.TxManager
	keys      persistence.APIKeyRepo
	auth      auth.Authenticator
	clock     clock.Clock
	workers   persistence.WorkerRepo
	logs      persistence.TaskLogRepo
	leaseTTL  time.Duration
}

// NewRouter builds the API router. Every request passes RequestID → Recover → Logging →
// BodyLimit(1 MiB) → Timeout(30 s, SSE exempt). Each /api/v1 route then passes Audit → Auth →
// RateLimit → RBAC, as per-route middleware so the matched route pattern is known (an unknown
// /api/v1 path is a plain 404). The operational routes (/healthz, /readyz, /metrics,
// /openapi.json) are unauthenticated and not rate limited (00E §9).
func NewRouter(d Deps) (chi.Router, error) {
	m, err := newHTTPMetrics(d.Metrics)
	if err != nil {
		return nil, err
	}
	spec, err := specJSON()
	if err != nil {
		return nil, err
	}
	h := &handlers{workflows: d.Workflows, runs: d.Runs, db: d.DB, tx: d.Tx, keys: d.APIKeys, auth: d.Auth, clock: d.Clock,
		workers: d.Workers, logs: d.Logs, leaseTTL: d.WorkerLeaseTTL}
	acl := routeACL()
	lim := newLimiter(d.Clock, d.RateLimitRPS, 2*d.RateLimitRPS)

	r := chi.NewRouter()
	r.Use(requestIDMiddleware(d.IDs, d.Logger), recoverMiddleware, loggingMiddleware(m, d.Clock),
		bodyLimitMiddleware, timeoutMiddleware)
	r.NotFound(func(w http.ResponseWriter, r *http.Request) {
		writeError(w, r, apperrors.NotFound("no such route"))
	})
	r.MethodNotAllowed(func(w http.ResponseWriter, r *http.Request) {
		writeError(w, r, apperrors.InvalidArgument("method not allowed on this route"))
	})

	r.Get("/healthz", healthz)
	r.Get("/readyz", readyz(d.Ready))
	r.Method(http.MethodGet, "/metrics", obs.NewServer("", d.Metrics).Handler)
	r.Get("/openapi.json", serveSpec(spec))

	r.Route("/api/v1", func(r chi.Router) {
		r.Group(func(r chi.Router) {
			r.Use(auditMiddleware(d.Audit, d.DB, m.auditFailures), authMiddleware(d.Auth), rateLimitMiddleware(lim), rbacMiddleware(acl))
			r.Post("/workflows", h.createWorkflow)
			r.Get("/workflows", h.listWorkflows)
			r.Get("/workflows/{id}", h.getWorkflow)
			r.Get("/workflows/by-name/{name}", h.getWorkflowByName)
			r.Post("/workflows/{id}/runs", h.startRun)
			r.Get("/runs", h.listRuns)
			r.Get("/runs/{run_id}", h.getRun)
			r.Get("/runs/{run_id}/tasks", h.listRunTasks)
			r.Get("/runs/{run_id}/events", h.listRunEvents)
			r.Get("/tasks/{task_id}", h.getTask)
			r.Get("/tasks/{task_id}/attempts", h.listTaskAttempts)
			r.Post("/api-keys", h.createAPIKey)
			r.Get("/api-keys", h.listAPIKeys)
			r.Delete("/api-keys/{key_id}", h.revokeAPIKey)
			r.Get("/workers", h.listWorkers)
			r.Get("/attempts/{attempt_id}/logs", h.listAttemptLogs)
		})
	})
	return r, nil
}

func newHTTPMetrics(reg *obs.Registry) (httpMetrics, error) {
	requests, err := reg.Counter(prometheus.CounterOpts{
		Name: "flowforge_http_requests_total", Help: "HTTP requests by route pattern, method and status code.",
	}, "route", "method", "code")
	if err != nil {
		return httpMetrics{}, fmt.Errorf("register http requests metric: %w", err)
	}
	duration, err := reg.Histogram(prometheus.HistogramOpts{
		Name: "flowforge_http_request_duration_seconds", Help: "HTTP request latency by route pattern and method.",
		Buckets: prometheus.DefBuckets,
	}, "route", "method")
	if err != nil {
		return httpMetrics{}, fmt.Errorf("register http duration metric: %w", err)
	}
	auditFailures, err := reg.Counter(prometheus.CounterOpts{
		Name: "flowforge_audit_write_failures_total", Help: "Audit rows that could not be written (the request still proceeded).",
	})
	if err != nil {
		return httpMetrics{}, fmt.Errorf("register audit failures metric: %w", err)
	}
	return httpMetrics{requests: requests, duration: duration, auditFailures: auditFailures.WithLabelValues()}, nil
}
