package coordination

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/daishakpatel/flowforge/internal/obs"
)

// ElectorMetrics are the scheduler leadership metrics (00E §11, §11.1). A nil *ElectorMetrics
// records nothing.
type ElectorMetrics struct {
	IsLeader      prometheus.Gauge   // flowforge_scheduler_is_leader: 1 while this instance leads
	Epoch         prometheus.Gauge   // flowforge_scheduler_epoch: the epoch of the current or last term
	LeaderChanges prometheus.Counter // flowforge_leader_changes_total: terms this instance started or ended
}

// NewElectorMetrics registers the elector metrics on reg (the scheduler's registry).
func NewElectorMetrics(reg *obs.Registry) (*ElectorMetrics, error) {
	isLeader, err := reg.Gauge(prometheus.GaugeOpts{Namespace: "flowforge", Subsystem: "scheduler", Name: "is_leader",
		Help: "1 while this scheduler instance holds leadership."})
	if err != nil {
		return nil, fmt.Errorf("register elector metrics: %w", err)
	}
	epoch, err := reg.Gauge(prometheus.GaugeOpts{Namespace: "flowforge", Subsystem: "scheduler", Name: "epoch",
		Help: "Fencing epoch of this instance's current or last leadership term."})
	if err != nil {
		return nil, fmt.Errorf("register elector metrics: %w", err)
	}
	changes, err := reg.Counter(prometheus.CounterOpts{Namespace: "flowforge", Name: "leader_changes_total",
		Help: "Leadership terms this instance started or ended."})
	if err != nil {
		return nil, fmt.Errorf("register elector metrics: %w", err)
	}
	return &ElectorMetrics{IsLeader: isLeader.WithLabelValues(), Epoch: epoch.WithLabelValues(), LeaderChanges: changes.WithLabelValues()}, nil
}

func (m *ElectorMetrics) began(epoch int64) {
	if m != nil {
		m.IsLeader.Set(1)
		m.Epoch.Set(float64(epoch))
		m.LeaderChanges.Inc()
	}
}

func (m *ElectorMetrics) ended() {
	if m != nil {
		m.IsLeader.Set(0)
		m.LeaderChanges.Inc()
	}
}

// RegistryMetrics are the worker lease metrics of the API tier (00E §11). A nil *RegistryMetrics
// records nothing.
type RegistryMetrics struct {
	RenewalFailures prometheus.Counter // flowforge_worker_lease_renewal_failures_total
}

// NewRegistryMetrics registers the registry metrics on reg (the API's registry).
func NewRegistryMetrics(reg *obs.Registry) (*RegistryMetrics, error) {
	c, err := reg.Counter(prometheus.CounterOpts{Namespace: "flowforge", Subsystem: "worker", Name: "lease_renewal_failures_total",
		Help: "Worker lease renewals the Raft KV did not confirm."})
	if err != nil {
		return nil, fmt.Errorf("register registry metrics: %w", err)
	}
	return &RegistryMetrics{RenewalFailures: c.WithLabelValues()}, nil
}

func (m *RegistryMetrics) renewalFailed() {
	if m != nil {
		m.RenewalFailures.Inc()
	}
}
