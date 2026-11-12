package rest

import (
	"context"
	"net/http"
	"time"

	"github.com/daishakpatel/flowforge/internal/apperrors"
)

// readinessTimeout bounds one /readyz check.
const readinessTimeout = 3 * time.Second

type healthResponse struct {
	Status string `json:"status"`
}

// healthz is liveness: 200 while the process serves HTTP, whatever its dependencies do.
func healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, healthResponse{Status: "ok"})
}

// readyz is readiness (FR-API-04): 200 when ready reports nil, else 503 UNAVAILABLE with
// Retry-After. The cause is logged, never returned, since the endpoint is unauthenticated.
func readyz(ready func(context.Context) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), readinessTimeout)
		defer cancel()
		if err := ready(ctx); err != nil {
			writeError(w, r, apperrors.Unavailable("not ready", dbRetryAfter, err))
			return
		}
		writeJSON(w, http.StatusOK, healthResponse{Status: "ok"})
	}
}
