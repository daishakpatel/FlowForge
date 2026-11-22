package server

import (
	"context"
	"errors"
	"fmt"
	"path"
	"runtime/debug"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	"github.com/daishakpatel/flowforge/internal/apperrors"
	"github.com/daishakpatel/flowforge/internal/obs"
)

// Metrics are the KV server's metrics (00E §11 Raft area). The node's own metrics, including
// flowforge_raft_write_duration_seconds (a durable Ready write), are node.Metrics.
type Metrics struct {
	Requests    *prometheus.CounterVec   // flowforge_raft_requests_total{method,code}
	Duration    *prometheus.HistogramVec // flowforge_raft_request_duration_seconds{method}
	ReadIndex   prometheus.Observer      // flowforge_raft_read_index_duration_seconds
	LeasesSwept *prometheus.CounterVec   // flowforge_raft_leases_swept_total{outcome}: expired, error
}

// NewMetrics registers the server metrics on reg. Register them once per process.
func NewMetrics(reg *obs.Registry) (*Metrics, error) {
	m := &Metrics{}
	var err error
	if m.Requests, err = reg.Counter(prometheus.CounterOpts{
		Namespace: "flowforge", Subsystem: "raft", Name: "requests_total",
		Help: "KV and Cluster requests by method and gRPC code.",
	}, "method", "code"); err != nil {
		return nil, fmt.Errorf("register raft server metrics: %w", err)
	}
	if m.Duration, err = reg.Histogram(prometheus.HistogramOpts{
		Namespace: "flowforge", Subsystem: "raft", Name: "request_duration_seconds",
		Help: "KV and Cluster request latency by method.", Buckets: prometheus.ExponentialBuckets(0.0005, 2, 14),
	}, "method"); err != nil {
		return nil, fmt.Errorf("register raft server metrics: %w", err)
	}
	ri, err := reg.Histogram(prometheus.HistogramOpts{
		Namespace: "flowforge", Subsystem: "raft", Name: "read_index_duration_seconds",
		Help: "Latency of the ReadIndex round behind a linearizable read.", Buckets: prometheus.ExponentialBuckets(0.0005, 2, 14),
	})
	if err != nil {
		return nil, fmt.Errorf("register raft server metrics: %w", err)
	}
	m.ReadIndex = ri.WithLabelValues()
	if m.LeasesSwept, err = reg.Counter(prometheus.CounterOpts{
		Namespace: "flowforge", Subsystem: "raft", Name: "leases_swept_total",
		Help: "Expired leases the sweeper deleted (outcome expired) or failed to (outcome error).",
	}, "outcome"); err != nil {
		return nil, fmt.Errorf("register raft server metrics: %w", err)
	}
	return m, nil
}

func (m *Metrics) readIndex(seconds float64) {
	if m != nil {
		m.ReadIndex.Observe(seconds)
	}
}

func (m *Metrics) swept(outcome string, n int) {
	if m != nil && n > 0 {
		m.LeasesSwept.WithLabelValues(outcome).Add(float64(n))
	}
}

// UnaryInterceptor counts and times every request, recovers a handler panic as INTERNAL, and
// logs internal errors once (00C §10.1).
func (s *Server) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (resp any, err error) {
		method := path.Base(info.FullMethod)
		start := s.cfg.Clock.Now()
		defer func() {
			if r := recover(); r != nil {
				s.cfg.Logger.ErrorContext(ctx, "raft kv: handler panicked", "method", method, "panic", r, "stack", string(debug.Stack()))
				resp, err = nil, internal(fmt.Errorf("panic: %v", r))
			}
			if e, ok := errors.AsType[*kvError](err); ok && e.code == apperrors.CodeInternal {
				s.cfg.Logger.ErrorContext(ctx, "raft kv: request failed", "method", method, "err", e.cause)
			}
			if m := s.cfg.Metrics; m != nil {
				m.Requests.WithLabelValues(method, status.Code(err).String()).Inc()
				m.Duration.WithLabelValues(method).Observe(s.cfg.Clock.Now().Sub(start).Seconds())
			}
		}()
		return h(ctx, req)
	}
}
