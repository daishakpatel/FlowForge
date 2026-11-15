package recovery

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/daishakpatel/flowforge/internal/events"
	"github.com/daishakpatel/flowforge/internal/logging"
	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/scheduler"
	"github.com/daishakpatel/flowforge/internal/workerapi"
	"github.com/daishakpatel/flowforge/internal/workflow/statemachine"
)

// ReasonLeaseExpired is the worker.lost reason when the LeaseProvider reported the lease expired.
const ReasonLeaseExpired = "lease_expired"

// abandonWorker abandons every RUNNING attempt of a worker whose lease expired, then marks the
// worker LOST and records worker.lost. The mark comes last and only when every attempt was
// handled, so a failure leaves the worker listed as expired and the next pass finishes the job.
func (l *Loop) abandonWorker(ctx context.Context, gate scheduler.Gate, workerID string) error {
	attempts, err := l.Recovery.RunningAttemptsOf(ctx, l.DB, workerID, batch)
	if err != nil {
		return err
	}
	for _, a := range attempts {
		if err := l.abandon(ctx, gate, a); err != nil {
			return err
		}
	}
	if len(attempts) == batch {
		return nil // more to abandon; mark it LOST on a later pass
	}
	return l.Tx.WithTx(ctx, pgx.TxOptions{}, func(tx pgx.Tx) error {
		if err := l.Cluster.AssertEpoch(ctx, tx, gate.Epoch()); err != nil {
			return err
		}
		// Only if still unseen since the lease ran out: a heartbeat meanwhile keeps it ACTIVE.
		marked, err := l.Recovery.MarkWorkerLost(ctx, tx, workerID, l.Clock.Now().Add(-l.LeaseTTL))
		if err != nil || !marked {
			return err
		}
		return l.Events.Record(ctx, tx, events.NewWorkerEvent(statemachine.EventWorkerLost,
			events.WorkerPayload{WorkerID: workerID, Reason: ReasonLeaseExpired}))
	})
}

// abandon ends one RUNNING attempt as ABANDONED (code WORKER_LOST) and lets the OutcomeHandler
// retry or fail its task, in one transaction that asserts the epoch first. An attempt that
// already ended is left alone.
func (l *Loop) abandon(ctx context.Context, gate scheduler.Gate, a persistence.AttemptView) error {
	return l.Tx.WithTx(ctx, pgx.TxOptions{}, func(tx pgx.Tx) error {
		if err := l.Cluster.AssertEpoch(ctx, tx, gate.Epoch()); err != nil {
			return err
		}
		ended, err := l.Ender.End(ctx, tx, a, workerapi.AttemptAbandoned, workerapi.CodeWorkerLost)
		if err != nil || !ended {
			return err
		}
		return l.Notify.Notify(ctx, tx, persistence.SchedulerWakeChannel) // requeue the retry promptly
	})
}

// failed handles one worker's or attempt's error: a stale epoch is returned to stop the loop;
// anything else is logged and counted, and the pass goes on.
func (l *Loop) failed(ctx context.Context, err error, key, id string) error {
	if errors.Is(err, persistence.ErrStaleEpoch) {
		return err
	}
	if ctx.Err() != nil {
		return nil
	}
	logging.FromContext(ctx).ErrorContext(ctx, "recover lost work", key, id, logging.Err(err))
	if l.Errors != nil {
		l.Errors.WithLabelValues(l.Name()).Inc()
	}
	return nil
}
