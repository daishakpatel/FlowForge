package obs

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// readinessTimeout bounds one /readyz request across all checks.
const readinessTimeout = 5 * time.Second

// ReadinessCheck is one dependency /readyz verifies, such as Postgres or the Raft quorum.
// Check returns nil when the dependency is usable and must respect ctx's deadline.
type ReadinessCheck struct {
	Name  string
	Check func(ctx context.Context) error
}

// NewServer returns an HTTP server (not yet started) for the operational endpoints:
//
//	GET /metrics  Prometheus exposition of reg
//	GET /healthz  200 while the process is alive (liveness)
//	GET /readyz   200 when every check passes, else 503 (readiness, FR-API-04)
//
// The /readyz body names each check as "ok" or "fail" but never includes error text, because
// the endpoint is unauthenticated and errors can carry hostnames (00C §10.1). The caller runs
// ListenAndServe and Shutdown.
func NewServer(addr string, reg *Registry, readiness ...ReadinessCheck) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg.reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), readinessTimeout)
		defer cancel()
		status, code := "ok", http.StatusOK
		checks := map[string]string{}
		for _, c := range readiness {
			checks[c.Name] = "ok"
			if err := c.Check(ctx); err != nil {
				checks[c.Name] = "fail"
				status, code = "unavailable", http.StatusServiceUnavailable
			}
		}
		writeJSON(w, code, map[string]any{"status": status, "checks": checks})
	})
	return &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body) // the client may be gone; nothing useful to do with the error
}
