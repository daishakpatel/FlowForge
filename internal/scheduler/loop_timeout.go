package scheduler

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
	"github.com/daishakpatel/flowforge/internal/workerapi"
)

// CodeTimeout is the attempt error code when the server timed an attempt out.
const CodeTimeout = "TIMEOUT"

// timeoutBatch caps the attempts one TimeoutLoop iteration ends.
const timeoutBatch = 200

// TimeoutLoop ends RUNNING attempts whose deadline has passed (FR-WF-08): each becomes TIMED_OUT
// and its task is retried or ends TIMED_OUT through the OutcomeHandler (03.04 decision table),
// in one transaction that asserts the epoch first (ADR-003).
//
// The server decides: deadline_at was set at claim time, and an attempt past it is timed out
// whatever its worker says (ADR-021); the worker's late report then gets STALE_ATTEMPT. Time
// spent QUEUED is not counted: a task's timeout_seconds bounds an attempt, not the wait for a
// worker.
type TimeoutLoop struct {
	DB       persistence.Querier
	Tx       persistence.TxManager
	Cluster  persistence.ClusterRepo
	Recovery persistence.RecoveryRepo
	Ender    workerapi.AttemptEnder
	Notify   persistence.Notifier
	// Runs, Sched and RunControl enforce run deadlines (03.05): the workflow timeout and the
	// end of a cancel's grace. With RunControl nil, only attempt deadlines are enforced.
	Runs       persistence.RunRepo
	Sched      persistence.SchedulerRepo
	RunControl persistence.RunControlRepo
	Clock      clock.Clock
	Tick       time.Duration          // 1 s
	Errors     *prometheus.CounterVec // flowforge_loop_errors_total; optional
}

var _ Loop = (*TimeoutLoop)(nil)

// Name implements Loop.
func (l *TimeoutLoop) Name() string { return "timeout" }

// Run implements Loop (see RunTicking).
func (l *TimeoutLoop) Run(ctx context.Context, gate Gate) error {
	return RunTicking(ctx, l.Name(), l.Clock, l.Tick, nil, l.Errors, func(ctx context.Context) error { return l.Once(ctx, gate) })
}

// Once times out the overdue attempts found now. Listing failing (the database is down) or a
// stale epoch is returned; one attempt failing is logged, counted and retried next iteration.
func (l *TimeoutLoop) Once(ctx context.Context, gate Gate) error {
	overdue, err := l.Recovery.OverdueAttempts(ctx, l.DB, l.Clock.Now(), timeoutBatch)
	if err != nil {
		return fmt.Errorf("list overdue attempts: %w", err)
	}
	done := gate.Done()
	for _, a := range overdue {
		select {
		case <-ctx.Done():
			return nil
		case <-done:
			return nil // no decision after the term ends (Gate contract)
		default:
		}
		err := l.Tx.WithTx(ctx, pgx.TxOptions{}, func(tx pgx.Tx) error {
			if err := l.Cluster.AssertEpoch(ctx, tx, gate.Epoch()); err != nil {
				return err
			}
			ended, err := l.Ender.End(ctx, tx, a, workerapi.AttemptTimedOut, CodeTimeout)
			if err != nil || !ended {
				return err
			}
			return l.Notify.Notify(ctx, tx, persistence.SchedulerWakeChannel) // retry or finish the run promptly
		})
		switch {
		case errors.Is(err, persistence.ErrStaleEpoch):
			return err
		case err != nil && ctx.Err() == nil:
			logging.FromContext(ctx).ErrorContext(ctx, "time out attempt", "attempt_id", a.ID, "task_id", a.TaskID, logging.Err(err))
			if l.Errors != nil {
				l.Errors.WithLabelValues(l.Name()).Inc()
			}
		}
	}
	return l.endOverdueRuns(ctx, gate)
}

// endOverdueRuns ends the runs whose deadline has passed: a workflow timeout, or a cancel whose
// grace ran out (03.05). Each run is its own transaction asserting the epoch.
func (l *TimeoutLoop) endOverdueRuns(ctx context.Context, gate Gate) error {
	if l.RunControl == nil {
		return nil
	}
	ids, err := l.RunControl.OverdueRuns(ctx, l.DB, l.Clock.Now(), timeoutBatch)
	if err != nil {
		return fmt.Errorf("list overdue runs: %w", err)
	}
	done := gate.Done()
	for _, id := range ids {
		select {
		case <-ctx.Done():
			return nil
		case <-done:
			return nil
		default:
		}
		err := l.Tx.WithTx(ctx, pgx.TxOptions{}, func(tx pgx.Tx) error {
			if err := l.Cluster.AssertEpoch(ctx, tx, gate.Epoch()); err != nil {
				return err
			}
			if err := l.endOverdueRun(ctx, tx, id, gate.Epoch()); err != nil {
				return err
			}
			return l.Notify.Notify(ctx, tx, persistence.SchedulerWakeChannel)
		})
		switch {
		case errors.Is(err, persistence.ErrStaleEpoch):
			return err
		case err != nil && ctx.Err() == nil:
			logging.FromContext(ctx).ErrorContext(ctx, "end overdue run", "run_id", id, logging.Err(err))
			if l.Errors != nil {
				l.Errors.WithLabelValues(l.Name()).Inc()
			}
		}
	}
	return nil
}
