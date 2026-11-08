// Package obs holds FlowForge's metrics registry and the operational HTTP server that serves
// /metrics, /healthz and /readyz (task 01.04, 00E §11, NFR-OBS-01).
package obs

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Registry is a Prometheus registry that only accepts metrics following the naming convention
// and the label allow-list (labels.go). Each service creates one in main; there is no global
// registry. It always includes the Go runtime and process collectors.
type Registry struct {
	reg *prometheus.Registry
}

// NewRegistry returns a Registry with the Go and process collectors registered.
func NewRegistry() *Registry {
	reg := prometheus.NewRegistry()
	// Registering two built-in collectors on a fresh registry cannot conflict.
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return &Registry{reg: reg}
}

// Counter registers a counter vector. The full name (Namespace_Subsystem_Name) must match
// flowforge_…_total and every label must be allowed; otherwise it returns ErrInvalidMetric.
// Registering the same name twice returns Prometheus's AlreadyRegisteredError, wrapped.
func (r *Registry) Counter(opts prometheus.CounterOpts, labels ...string) (*prometheus.CounterVec, error) {
	if err := check(opts.Namespace, opts.Subsystem, opts.Name, true, labels); err != nil {
		return nil, err
	}
	return register(r, prometheus.NewCounterVec(opts, labels))
}

// Gauge registers a gauge vector under the same rules as Counter, except that the name must
// not end in _total.
func (r *Registry) Gauge(opts prometheus.GaugeOpts, labels ...string) (*prometheus.GaugeVec, error) {
	if err := check(opts.Namespace, opts.Subsystem, opts.Name, false, labels); err != nil {
		return nil, err
	}
	return register(r, prometheus.NewGaugeVec(opts, labels))
}

// Histogram registers a histogram vector under the same rules as Gauge. Durations are in
// seconds and their names end in _seconds (00C §10.2).
func (r *Registry) Histogram(opts prometheus.HistogramOpts, labels ...string) (*prometheus.HistogramVec, error) {
	if err := check(opts.Namespace, opts.Subsystem, opts.Name, false, labels); err != nil {
		return nil, err
	}
	return register(r, prometheus.NewHistogramVec(opts, labels))
}

func check(namespace, subsystem, name string, counter bool, labels []string) error {
	fq := prometheus.BuildFQName(namespace, subsystem, name)
	if err := checkName(fq, counter); err != nil {
		return err
	}
	return checkLabels(fq, labels)
}

func register[C prometheus.Collector](r *Registry, c C) (C, error) {
	if err := r.reg.Register(c); err != nil {
		var zero C
		return zero, fmt.Errorf("obs: register metric: %w", err)
	}
	return c, nil
}
