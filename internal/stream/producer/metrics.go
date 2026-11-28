package producer

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/daishakpatel/flowforge/internal/obs"
)

// The result label values of flowforge_producer_events_total.
const (
	resultPublished     = "published"      // a valid tick on market.ticks
	resultInvalid       = "invalid"        // an invalid tick sent to market.invalid
	resultForcedInvalid = "forced_invalid" // an invalid tick sent to market.ticks (--allow-invalid)
	resultError         = "error"          // a tick whose batch failed to publish
)

// Metrics are the producer's 00E §11.1 metrics.
type Metrics struct {
	Events  *prometheus.CounterVec // flowforge_producer_events_total{result}
	Latency prometheus.Observer    // flowforge_producer_publish_latency_seconds, per batch
}

// NewMetrics registers the producer metrics on reg.
func NewMetrics(reg *obs.Registry) (*Metrics, error) {
	events, err := reg.Counter(prometheus.CounterOpts{
		Name: "flowforge_producer_events_total", Help: "Ticks the producer handled, by result: published, invalid, forced_invalid, error.",
	}, "result")
	if err != nil {
		return nil, fmt.Errorf("producer metrics: %w", err)
	}
	latency, err := reg.Histogram(prometheus.HistogramOpts{
		Name: "flowforge_producer_publish_latency_seconds", Help: "Time to publish one batch until every record is acknowledged.",
		Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	})
	if err != nil {
		return nil, fmt.Errorf("producer metrics: %w", err)
	}
	for _, r := range []string{resultPublished, resultInvalid, resultForcedInvalid, resultError} {
		events.WithLabelValues(r) // export zeros, so a rate query sees the series from the start
	}
	return &Metrics{Events: events, Latency: latency.WithLabelValues()}, nil
}
