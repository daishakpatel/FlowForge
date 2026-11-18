package outbox

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/logging"
	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/scheduler"
	"github.com/daishakpatel/flowforge/internal/testutil/failpoint"
)

// Relay defaults (task 03.07 §4).
const (
	DefaultBatch          = 500
	DefaultPublishTimeout = 10 * time.Second
)

// RelayLoop is the leader-gated outbox relay. Each pass, in one transaction:
//
//  1. assert the scheduler epoch (ADR-003), so a deposed leader publishes nothing;
//  2. lock up to Batch unpublished rows in id order (FOR UPDATE SKIP LOCKED);
//  3. publish them, bounded by PublishTimeout;
//  4. mark published only the prefix the Publisher reports delivered, and commit.
//
// A failure leaves the rest unmarked, so the next pass starts again from the first unpublished
// id: nothing is skipped and each key's order holds. A message delivered but not marked (an
// acknowledgement lost, a crash before commit) is published again, so delivery is at least once
// and consumers dedupe on event_id. Passes repeat until the backlog is drained or leadership ends.
//
// The transaction spans the publish call, as the task asks for the row locks; PublishTimeout
// bounds it. If the transaction manager reruns the pass (a serialization failure), the batch is
// published again, which at-least-once allows.
type RelayLoop struct {
	DB             persistence.Querier // backlog metrics; may be nil without Metrics
	Tx             persistence.TxManager
	Cluster        persistence.ClusterRepo
	Outbox         persistence.OutboxRepo
	Publisher      Publisher
	Clock          clock.Clock
	Tick           time.Duration
	Batch          int           // rows per pass; 0 means DefaultBatch
	PublishTimeout time.Duration // 0 means DefaultPublishTimeout
	Metrics        *Metrics      // optional
	Errors         *prometheus.CounterVec
}

var _ scheduler.Loop = (*RelayLoop)(nil)

// Name implements scheduler.Loop.
func (l *RelayLoop) Name() string { return "outbox" }

// Run implements scheduler.Loop: Once every Tick, backing off after failures (scheduler.RunTicking).
func (l *RelayLoop) Run(ctx context.Context, gate scheduler.Gate) error {
	return scheduler.RunTicking(ctx, l.Name(), l.Clock, l.Tick, nil, l.Errors, func(ctx context.Context) error { return l.Once(ctx, gate) })
}

// Once publishes batches until the backlog is drained, a pass fails, or gate stops leading. It
// does nothing when gate is not the leader. A publish failure (after marking any delivered
// prefix) and a stale epoch are returned.
func (l *RelayLoop) Once(ctx context.Context, gate scheduler.Gate) error {
	defer l.observeBacklog(ctx)
	for gate.IsLeader() {
		n, err := l.pass(ctx, gate.Epoch())
		if err != nil {
			return err
		}
		if n < l.batch() {
			return nil
		}
	}
	return nil
}

// pass publishes one batch and returns how many rows it read.
func (l *RelayLoop) pass(ctx context.Context, epoch int64) (int, error) {
	var (
		read      int
		delivered []persistence.OutboxView
		pubErr    error
		at        time.Time
	)
	err := l.Tx.WithTx(ctx, pgx.TxOptions{}, func(tx pgx.Tx) error {
		delivered, pubErr = nil, nil
		if err := l.Cluster.AssertEpoch(ctx, tx, epoch); err != nil { // ADR-003: same tx as the marks
			return err
		}
		rows, err := l.Outbox.LockUnpublished(ctx, tx, l.batch())
		if err != nil || len(rows) == 0 {
			read = 0
			return err
		}
		read = len(rows)
		pubErr = l.publish(ctx, rows)
		if err := failpoint.Inject(failpoint.OutboxAfterPublish); err != nil {
			return err
		}
		delivered = rows[:Delivered(pubErr, len(rows))]
		if len(delivered) == 0 {
			return nil
		}
		at = l.Clock.Now()
		ids := make([]int64, len(delivered))
		for i, r := range delivered {
			ids[i] = r.ID
		}
		return l.Outbox.MarkPublished(ctx, tx, ids, at)
	})
	if err != nil {
		if pubErr != nil { // the publish failed too: report both, and count the failure
			l.observePublished(nil, at, pubErr)
			return 0, errors.Join(err, fmt.Errorf("publish %d outbox rows: %w", read, pubErr))
		}
		return 0, err
	}
	l.observePublished(delivered, at, pubErr)
	if pubErr != nil {
		return read, fmt.Errorf("publish %d outbox rows (%d delivered): %w", read, len(delivered), pubErr)
	}
	return read, nil
}

func (l *RelayLoop) publish(ctx context.Context, rows []persistence.OutboxView) error {
	msgs := make([]Message, len(rows))
	for i, r := range rows {
		msgs[i] = Message{Topic: r.Topic, Key: r.Key, Value: r.Envelope, Headers: map[string]string{HeaderEventID: r.EventID}}
	}
	timeout := l.PublishTimeout
	if timeout <= 0 {
		timeout = DefaultPublishTimeout
	}
	pctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return l.Publisher.Publish(pctx, msgs)
}

func (l *RelayLoop) batch() int {
	if l.Batch > 0 {
		return l.Batch
	}
	return DefaultBatch
}

func (l *RelayLoop) observePublished(rows []persistence.OutboxView, at time.Time, pubErr error) {
	if l.Metrics == nil {
		return
	}
	for _, r := range rows {
		l.Metrics.Lag.Observe(max(0, at.Sub(r.CreatedAt).Seconds()))
	}
	if pubErr != nil {
		l.Metrics.Failures.Inc()
	}
}

// observeBacklog refreshes the backlog gauges; a failed read is only logged, the next pass retries.
func (l *RelayLoop) observeBacklog(ctx context.Context) {
	if l.Metrics == nil {
		return
	}
	if ctx.Err() != nil { // shutting down: no read, no spurious warning
		return
	}
	n, oldest, err := l.Outbox.Backlog(ctx, l.DB)
	if err != nil { // the gauges keep their last values; the next pass retries
		logging.FromContext(ctx).WarnContext(ctx, "outbox backlog unreadable", logging.Err(err))
		return
	}
	l.Metrics.Backlog.Set(float64(n))
	age := 0.0
	if oldest != nil {
		age = max(0, l.Clock.Now().Sub(*oldest).Seconds())
	}
	l.Metrics.OldestAge.Set(age)
}
