// Package retention removes old history in bounded batches (task 03.07): published outbox rows,
// workflow events, task log lines, and runs' idempotency keys. It runs on the leader, hourly.
package retention

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/obs"
	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/scheduler"
)

// Defaults (task 03.07 §4).
const (
	DefaultTick  = time.Hour
	DefaultBatch = 5000
	DefaultPause = 200 * time.Millisecond
)

// Policy is how long each kind of row is kept (FF_RETENTION_*).
type Policy struct {
	Outbox          time.Duration // published outbox rows, by creation
	Events          time.Duration // workflow_events
	TaskLogs        time.Duration // task_logs, by the line's timestamp
	IdempotencyKeys time.Duration // runs older than this lose their Idempotency-Key
}

// DefaultPolicy is 3 days, 30 days, 14 days and 24 hours.
func DefaultPolicy() Policy {
	return Policy{Outbox: 72 * time.Hour, Events: 30 * 24 * time.Hour, TaskLogs: 14 * 24 * time.Hour, IdempotencyKeys: 24 * time.Hour}
}

// Loop is the leader-gated retention loop. Each pass works through the targets in turn; each
// target is purged in batches of at most Batch rows, one short statement per batch, with Pause
// between batches so it never holds locks on a hot table for long. Leadership is checked before
// every batch. Deleting history is idempotent, so a pass that stops halfway resumes next time.
type Loop struct {
	DB      persistence.Querier
	Repo    persistence.RetentionRepo
	Clock   clock.Clock
	Tick    time.Duration // DefaultTick
	Policy  Policy
	Batch   int                    // DefaultBatch
	Pause   time.Duration          // DefaultPause
	Deleted *prometheus.CounterVec // flowforge_retention_deleted_rows_total{kind}; optional
	Errors  *prometheus.CounterVec // flowforge_loop_errors_total; optional
}

var _ scheduler.Loop = (*Loop)(nil)

// Name implements scheduler.Loop.
func (l *Loop) Name() string { return "retention" }

// Run implements scheduler.Loop.
func (l *Loop) Run(ctx context.Context, gate scheduler.Gate) error {
	tick := l.Tick
	if tick <= 0 {
		tick = DefaultTick
	}
	return scheduler.RunTicking(ctx, l.Name(), l.Clock, tick, nil, l.Errors, func(ctx context.Context) error { return l.Once(ctx, gate) })
}

// Once makes one retention pass. A target that fails does not stop the others; their errors are
// returned together. A pass interrupted by shutdown returns nil.
func (l *Loop) Once(ctx context.Context, gate scheduler.Gate) error {
	now := l.Clock.Now()
	var errs []error
	for _, job := range []struct {
		target persistence.RetentionTarget
		age    time.Duration
	}{
		{persistence.RetainOutbox, l.Policy.Outbox},
		{persistence.RetainEvents, l.Policy.Events},
		{persistence.RetainTaskLogs, l.Policy.TaskLogs},
		{persistence.RetainIdempotencyKeys, l.Policy.IdempotencyKeys},
	} {
		if ctx.Err() != nil {
			return nil
		}
		if err := l.purge(ctx, gate, job.target, now.Add(-job.age)); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (l *Loop) purge(ctx context.Context, gate scheduler.Gate, target persistence.RetentionTarget, before time.Time) error {
	batch := l.Batch
	if batch <= 0 {
		batch = DefaultBatch
	}
	pause := l.Pause
	if pause <= 0 {
		pause = DefaultPause
	}
	for first := true; gate.IsLeader(); first = false {
		if !first {
			if err := l.Clock.Sleep(ctx, pause); err != nil {
				return nil //nolint:nilerr // ctx ended: the loop is stopping
			}
		}
		n, err := l.Repo.Purge(ctx, l.DB, target, before, batch)
		if err != nil {
			return err
		}
		if l.Deleted != nil {
			l.Deleted.WithLabelValues(string(target)).Add(float64(n))
		}
		if n < int64(batch) {
			return nil
		}
	}
	return nil
}

// NewDeletedRows registers flowforge_retention_deleted_rows_total{kind}.
func NewDeletedRows(reg *obs.Registry) (*prometheus.CounterVec, error) {
	c, err := reg.Counter(prometheus.CounterOpts{
		Name: "flowforge_retention_deleted_rows_total",
		Help: "Rows removed by retention, by kind (idempotency_keys counts keys cleared).",
	}, "kind")
	if err != nil {
		return nil, fmt.Errorf("register retention metric: %w", err)
	}
	return c, nil
}
