package runtime

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/daishakpatel/flowforge/internal/obs"
)

// Metrics are the worker's metrics (00E §11, §11.1). A nil *Metrics records nothing.
type Metrics struct {
	active        prometheus.Gauge
	outcomes      *prometheus.CounterVec
	logDrops      prometheus.Counter
	leaseFailures prometheus.Counter
}

// NewMetrics registers the worker metrics on reg:
//   - flowforge_worker_active_tasks: attempts running now;
//   - flowforge_worker_task_outcomes_total{outcome}: succeeded, failed, cancelled, shutdown,
//     stale (result dropped), report_failed (report rejected or abandoned);
//   - flowforge_worker_log_lines_dropped_total: attempt log lines not delivered;
//   - flowforge_worker_lease_renewal_failures_total: failed heartbeats.
func NewMetrics(reg *obs.Registry) (*Metrics, error) {
	active, err := reg.Gauge(prometheus.GaugeOpts{Name: "flowforge_worker_active_tasks", Help: "Attempts this worker is running."})
	if err != nil {
		return nil, fmt.Errorf("register worker metrics: %w", err)
	}
	outcomes, err := reg.Counter(prometheus.CounterOpts{
		Name: "flowforge_worker_task_outcomes_total", Help: "Finished attempts by how they ended for this worker.",
	}, "outcome")
	if err != nil {
		return nil, fmt.Errorf("register worker metrics: %w", err)
	}
	drops, err := reg.Counter(prometheus.CounterOpts{
		Name: "flowforge_worker_log_lines_dropped_total", Help: "Attempt log lines dropped on buffer overflow or failed shipping.",
	})
	if err != nil {
		return nil, fmt.Errorf("register worker metrics: %w", err)
	}
	lease, err := reg.Counter(prometheus.CounterOpts{
		Name: "flowforge_worker_lease_renewal_failures_total", Help: "Heartbeats that failed.",
	})
	if err != nil {
		return nil, fmt.Errorf("register worker metrics: %w", err)
	}
	return &Metrics{
		active: active.WithLabelValues(), outcomes: outcomes,
		logDrops: drops.WithLabelValues(), leaseFailures: lease.WithLabelValues(),
	}, nil
}

func (m *Metrics) setActive(n int) {
	if m != nil {
		m.active.Set(float64(n))
	}
}

func (m *Metrics) outcome(o string) {
	if m != nil {
		m.outcomes.WithLabelValues(o).Inc()
	}
}

func (m *Metrics) logsDropped(n int) {
	if m != nil {
		m.logDrops.Add(float64(n))
	}
}

func (m *Metrics) leaseRenewalFailed() {
	if m != nil {
		m.leaseFailures.Inc()
	}
}
