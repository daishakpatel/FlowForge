package scheduler

import (
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/daishakpatel/flowforge/internal/obs"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
)

// Metrics are the scheduler loop's metrics (00E §11). Counters move only after the decision
// transaction commits, so a retried transaction is never counted twice.
type Metrics struct {
	TickDuration       prometheus.Observer
	QueueDepth         *prometheus.GaugeVec   // {queue}: QUEUED tasks
	Tasks              *prometheus.CounterVec // {status}: tasks the scheduler moved into status
	WorkflowsStarted   prometheus.Counter
	WorkflowsCompleted *prometheus.CounterVec // {status}: runs the scheduler finished
	WorkflowDuration   prometheus.Observer    // started_at → final status
}

// NewMetrics registers the scheduler metrics on reg.
func NewMetrics(reg *obs.Registry) (*Metrics, error) {
	tick, err := reg.Histogram(prometheus.HistogramOpts{
		Name: "flowforge_scheduler_tick_duration_seconds", Help: "Duration of one scheduler loop tick.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14), // 1 ms … 8 s
	})
	if err != nil {
		return nil, fmt.Errorf("register scheduler metrics: %w", err)
	}
	depth, err := reg.Gauge(prometheus.GaugeOpts{
		Name: "flowforge_scheduler_queue_depth", Help: "QUEUED tasks per queue, read once per tick.",
	}, "queue")
	if err != nil {
		return nil, fmt.Errorf("register scheduler metrics: %w", err)
	}
	tasks, err := reg.Counter(prometheus.CounterOpts{
		Name: "flowforge_tasks_total", Help: "Task transitions made by the scheduler, by target status.",
	}, "status")
	if err != nil {
		return nil, fmt.Errorf("register scheduler metrics: %w", err)
	}
	started, err := reg.Counter(prometheus.CounterOpts{
		Name: "flowforge_workflows_started_total", Help: "Runs moved CREATED → RUNNING.",
	})
	if err != nil {
		return nil, fmt.Errorf("register scheduler metrics: %w", err)
	}
	completed, err := reg.Counter(prometheus.CounterOpts{
		Name: "flowforge_workflows_completed_total", Help: "Runs the scheduler moved to a final status (or NEEDS_ATTENTION).",
	}, "status")
	if err != nil {
		return nil, fmt.Errorf("register scheduler metrics: %w", err)
	}
	duration, err := reg.Histogram(prometheus.HistogramOpts{
		Name: "flowforge_workflow_duration_seconds", Help: "Run duration from started_at to its final status.",
		Buckets: prometheus.ExponentialBuckets(1, 2, 16), // 1 s … ~9 h
	})
	if err != nil {
		return nil, fmt.Errorf("register scheduler metrics: %w", err)
	}
	return &Metrics{
		TickDuration: tick.WithLabelValues(), QueueDepth: depth, Tasks: tasks,
		WorkflowsStarted: started.WithLabelValues(), WorkflowsCompleted: completed, WorkflowDuration: duration.WithLabelValues(),
	}, nil
}

// NewLoopErrors registers flowforge_loop_errors_total{loop}: failed iterations of a leader loop
// (scheduler, timeout, recovery), for RunTicking.
func NewLoopErrors(reg *obs.Registry) (*prometheus.CounterVec, error) {
	c, err := reg.Counter(prometheus.CounterOpts{
		Name: "flowforge_loop_errors_total", Help: "Failed iterations of a scheduler loop; the loop backs off and retries.",
	}, "loop")
	if err != nil {
		return nil, fmt.Errorf("register loop error metric: %w", err)
	}
	return c, nil
}

// record applies one committed processRun result.
func (m *Metrics) record(r result) {
	if m == nil {
		return
	}
	for status, n := range r.tasks {
		m.Tasks.WithLabelValues(string(status)).Add(float64(n))
	}
	if r.started {
		m.WorkflowsStarted.Inc()
	}
	if r.completed != "" {
		m.WorkflowsCompleted.WithLabelValues(string(r.completed)).Inc()
		if r.duration > 0 {
			m.WorkflowDuration.Observe(r.duration.Seconds())
		}
	}
}

// setQueueDepth replaces the queue_depth series, so a queue that drained reads 0 and then goes.
func (m *Metrics) setQueueDepth(depth map[string]int) {
	if m == nil {
		return
	}
	m.QueueDepth.Reset()
	for q, n := range depth {
		m.QueueDepth.WithLabelValues(q).Set(float64(n))
	}
}

// result is what one committed processRun did, for metrics.
type result struct {
	started   bool
	completed model.RunStatus // "" unless the run left RUNNING for a derived status
	duration  time.Duration
	tasks     map[model.TaskStatus]int
	queued    int
}
