package outbox

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/daishakpatel/flowforge/internal/obs"
)

// Metrics are the relay's metrics (00E §11 and §11.1).
type Metrics struct {
	Backlog   prometheus.Gauge    // flowforge_outbox_backlog: unpublished rows
	OldestAge prometheus.Gauge    // flowforge_outbox_oldest_age_seconds: age of the oldest unpublished row
	Lag       prometheus.Observer // flowforge_outbox_publish_lag_seconds: row creation → marked published
	Failures  prometheus.Counter  // flowforge_outbox_publish_failures_total: failed Publish calls
}

// NewMetrics registers the relay metrics in reg.
func NewMetrics(reg *obs.Registry) (*Metrics, error) {
	backlog, err := reg.Gauge(prometheus.GaugeOpts{Name: "flowforge_outbox_backlog", Help: "Outbox rows not yet published."})
	if err != nil {
		return nil, fmt.Errorf("register outbox backlog metric: %w", err)
	}
	age, err := reg.Gauge(prometheus.GaugeOpts{
		Name: "flowforge_outbox_oldest_age_seconds", Help: "Age of the oldest unpublished outbox row; 0 when none.",
	})
	if err != nil {
		return nil, fmt.Errorf("register outbox age metric: %w", err)
	}
	lag, err := reg.Histogram(prometheus.HistogramOpts{
		Name: "flowforge_outbox_publish_lag_seconds", Help: "Time from an outbox row's creation to its publication.",
		Buckets: []float64{.01, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60},
	})
	if err != nil {
		return nil, fmt.Errorf("register outbox lag metric: %w", err)
	}
	failures, err := reg.Counter(prometheus.CounterOpts{
		Name: "flowforge_outbox_publish_failures_total", Help: "Publish calls that failed, wholly or after a prefix.",
	})
	if err != nil {
		return nil, fmt.Errorf("register outbox failure metric: %w", err)
	}
	return &Metrics{Backlog: backlog.WithLabelValues(), OldestAge: age.WithLabelValues(),
		Lag: lag.WithLabelValues(), Failures: failures.WithLabelValues()}, nil
}
