package rest

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/daishakpatel/flowforge/internal/api/auth"
	"github.com/daishakpatel/flowforge/internal/apperrors"
	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/ids"
	"github.com/daishakpatel/flowforge/internal/logging"
)

// Request limits (FR-SEC-05).
const (
	MaxBodyBytes   = 1 << 20
	RequestTimeout = 30 * time.Second
)

// ssePath is exempt from RequestTimeout: the stream (task 03.07) is long-lived by design.
const ssePath = "/api/v1/events/stream"

// requestIDMiddleware gives every request a fresh req_<ulid>, returned in X-Request-ID and
// carried in the context's log fields, so every log line and error envelope has it. It also
// stores the base logger in the context.
func requestIDMiddleware(g ids.Generator, logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id := g.New(ids.PrefixRequest)
			w.Header().Set("X-Request-ID", id)
			ctx := logging.WithFields(logging.NewContext(r.Context(), logger), logging.KeyRequestID, id)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// recoverMiddleware turns a handler panic into a logged INTERNAL error instead of a dropped
// connection (00C §10.1).
func recoverMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() { //nolint:contextcheck // writeError takes the context from r
			p := recover()
			if p == nil {
				return
			}
			if p == http.ErrAbortHandler { //nolint:errorlint // sentinel compared as net/http documents
				panic(p)
			}
			ctx := r.Context()
			logging.FromContext(ctx).ErrorContext(ctx, "handler panic", "panic", fmt.Sprint(p), "stack", string(debug.Stack()))
			writeError(w, r, apperrors.Internal(fmt.Errorf("panic: %v", p)))
		}()
		next.ServeHTTP(w, r)
	})
}

// httpMetrics are the 00E §11 API metrics.
type httpMetrics struct {
	requests      *prometheus.CounterVec
	duration      *prometheus.HistogramVec
	auditFailures prometheus.Counter
}

// loggingMiddleware logs one line per request and records the HTTP metrics, labelled by the
// route pattern (never the raw path, which carries IDs).
func loggingMiddleware(m httpMetrics, clk clock.Clock) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := clk.Now()
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			next.ServeHTTP(ww, r)
			elapsed := clk.Now().Sub(start)
			route := chi.RouteContext(r.Context()).RoutePattern()
			if route == "" {
				route = "unmatched"
			}
			status := ww.Status()
			m.requests.WithLabelValues(route, r.Method, strconv.Itoa(status)).Inc()
			m.duration.WithLabelValues(route, r.Method).Observe(elapsed.Seconds())
			ctx := r.Context()
			logging.FromContext(ctx).InfoContext(ctx, "http request", "method", r.Method, "route", route,
				"status", status, "duration_ms", elapsed.Milliseconds())
		})
	}
}

// bodyLimitMiddleware caps request bodies at MaxBodyBytes; reading past it fails with
// *http.MaxBytesError, which writeError reports as PAYLOAD_TOO_LARGE.
func bodyLimitMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, MaxBodyBytes)
		next.ServeHTTP(w, r)
	})
}

// timeoutMiddleware bounds each request's context by RequestTimeout, except the SSE stream.
func timeoutMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == ssePath {
			next.ServeHTTP(w, r)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), RequestTimeout)
		defer cancel()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// authMiddleware authenticates the caller and stores the principal for auth.PrincipalFrom (and
// for the audit row).
func authMiddleware(a auth.Authenticator) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			p, err := a.Authenticate(r)
			if err != nil {
				writeError(w, r, err)
				return
			}
			setAuditPrincipal(r.Context(), p)
			next.ServeHTTP(w, r.WithContext(auth.WithPrincipal(r.Context(), p)))
		})
	}
}
