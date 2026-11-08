package obs

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func get(t *testing.T, srv *http.Server, path string) (int, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, path, nil))
	body, _ := io.ReadAll(rec.Body)
	return rec.Code, string(body)
}

func TestServer_Healthz(t *testing.T) {
	t.Parallel()
	code, body := get(t, NewServer(":0", NewRegistry()), "/healthz")
	if code != http.StatusOK || !strings.Contains(body, `"ok"`) {
		t.Errorf("/healthz = %d %s", code, body)
	}
}

func TestServer_Readyz(t *testing.T) {
	t.Parallel()
	ok := ReadinessCheck{Name: "postgres", Check: func(context.Context) error { return nil }}
	down := ReadinessCheck{Name: "raft", Check: func(context.Context) error {
		return errors.New("dial raft-1.internal:7000: connection refused")
	}}

	if code, body := get(t, NewServer(":0", NewRegistry(), ok), "/readyz"); code != http.StatusOK {
		t.Errorf("all checks pass: /readyz = %d %s", code, body)
	}
	if code, _ := get(t, NewServer(":0", NewRegistry()), "/readyz"); code != http.StatusOK {
		t.Errorf("no checks: /readyz = %d", code)
	}

	code, body := get(t, NewServer(":0", NewRegistry(), ok, down), "/readyz")
	if code != http.StatusServiceUnavailable {
		t.Fatalf("failing check: /readyz = %d, want 503", code)
	}
	var got struct {
		Status string            `json:"status"`
		Checks map[string]string `json:"checks"`
	}
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status != "unavailable" || got.Checks["postgres"] != "ok" || got.Checks["raft"] != "fail" {
		t.Errorf("body = %+v", got)
	}
	if strings.Contains(body, "raft-1.internal") {
		t.Error("/readyz leaks error text")
	}
}

func TestServer_MetricsExposesRegisteredAndRuntimeMetrics(t *testing.T) {
	t.Parallel()
	reg := NewRegistry()
	c, err := reg.Counter(prometheus.CounterOpts{Name: "flowforge_tasks_total", Help: "Tasks by status."}, "status")
	if err != nil {
		t.Fatal(err)
	}
	c.WithLabelValues("SUCCEEDED").Inc()
	g, err := reg.Gauge(prometheus.GaugeOpts{Namespace: "flowforge", Subsystem: "scheduler", Name: "is_leader", Help: "1 when leader."})
	if err != nil {
		t.Fatal(err)
	}
	g.WithLabelValues().Set(1)
	h, err := reg.Histogram(prometheus.HistogramOpts{Name: "flowforge_http_request_duration_seconds", Help: "Latency."}, "route", "method", "code")
	if err != nil {
		t.Fatal(err)
	}
	h.WithLabelValues("/healthz", "GET", "200").Observe(0.01)

	code, body := get(t, NewServer(":0", reg), "/metrics")
	if code != http.StatusOK {
		t.Fatalf("/metrics = %d", code)
	}
	for _, want := range []string{
		`flowforge_tasks_total{status="SUCCEEDED"} 1`,
		"flowforge_scheduler_is_leader 1",
		"flowforge_http_request_duration_seconds_bucket",
		"go_goroutines",
		"process_",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics is missing %q", want)
		}
	}
}

func TestRegistry_RejectsConventionViolations(t *testing.T) {
	t.Parallel()
	reg := NewRegistry()
	for name, register := range map[string]func() error{
		"label not allowed": func() error {
			_, err := reg.Counter(prometheus.CounterOpts{Name: "flowforge_x_total", Help: "h"}, "run_id")
			return err
		},
		"duplicate label": func() error {
			_, err := reg.Gauge(prometheus.GaugeOpts{Name: "flowforge_x", Help: "h"}, "queue", "queue")
			return err
		},
		"counter without _total": func() error {
			_, err := reg.Counter(prometheus.CounterOpts{Name: "flowforge_x", Help: "h"})
			return err
		},
		"gauge with _total": func() error {
			_, err := reg.Gauge(prometheus.GaugeOpts{Name: "flowforge_x_total", Help: "h"})
			return err
		},
		"wrong prefix": func() error {
			_, err := reg.Histogram(prometheus.HistogramOpts{Name: "marketdata_batch_size", Help: "h"})
			return err
		},
		"upper case": func() error {
			_, err := reg.Histogram(prometheus.HistogramOpts{Name: "flowforge_Latency_seconds", Help: "h"})
			return err
		},
		"histogram label not allowed": func() error {
			_, err := reg.Histogram(prometheus.HistogramOpts{Name: "flowforge_y_seconds", Help: "h"}, "user")
			return err
		},
	} {
		if err := register(); !errors.Is(err, ErrInvalidMetric) {
			t.Errorf("%s: err = %v, want ErrInvalidMetric", name, err)
		}
	}
}

func TestRegistry_DuplicateRegistration(t *testing.T) {
	t.Parallel()
	reg := NewRegistry()
	opts := prometheus.CounterOpts{Name: "flowforge_dup_total", Help: "h"}
	if _, err := reg.Counter(opts); err != nil {
		t.Fatal(err)
	}
	_, err := reg.Counter(opts)
	if _, ok := errors.AsType[prometheus.AlreadyRegisteredError](err); !ok {
		t.Errorf("second registration: %v, want AlreadyRegisteredError", err)
	}
}

func TestServer_TimeoutsSet(t *testing.T) {
	t.Parallel()
	srv := NewServer("127.0.0.1:9100", NewRegistry())
	if srv.Addr != "127.0.0.1:9100" || srv.ReadHeaderTimeout == 0 || srv.WriteTimeout == 0 {
		t.Errorf("server = %+v", srv)
	}
	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/healthz", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST /healthz = %d, want 405", rec.Code)
	}
}
