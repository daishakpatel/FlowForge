// Package recovery recovers the work of lost workers (task 03.04, FR-WF-09, master spec §3.4):
// the leader abandons the RUNNING attempts of a worker whose lease expired, and the
// OutcomeHandler retries their tasks; a zombie worker that wakes up later gets STALE_ATTEMPT
// for its old fencing token (ADR-003).
//
// Worker liveness comes only from a workerapi.LeaseProvider: PGLeaseProvider's heartbeats now,
// Raft lease expirations from 04.07. The Loop does not change when the provider does.
package recovery

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/events"
	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/scheduler"
	"github.com/daishakpatel/flowforge/internal/workerapi"
)

// batch caps the attempts one pass handles per source.
const batch = 200

// Loop is the leader-gated recovery loop. Each pass:
//
//  1. asks the LeaseProvider which workers' leases expired, abandons each one's RUNNING attempts
//     and then marks it LOST (event worker.lost);
//  2. as a fallback, abandons RUNNING attempts whose worker holds no lease (unknown,
//     deregistered or already LOST) and that showed no sign of life for
//     3×Heartbeat + LeaseTTL: work left behind by a crash or a cut-off shutdown.
//
// Every decision is its own transaction that asserts the epoch first (ADR-003); abandoning an
// attempt that already ended is a no-op, so passes can repeat after any crash.
type Loop struct {
	DB       persistence.Querier
	Tx       persistence.TxManager
	Cluster  persistence.ClusterRepo
	Recovery persistence.RecoveryRepo
	Leases   workerapi.LeaseProvider
	Ender    workerapi.AttemptEnder
	Notify   persistence.Notifier
	Events   *events.Recorder
	Clock    clock.Clock
	Tick     time.Duration // 2 s
	// LeaseTTL and Heartbeat are FF_WORKER_LEASE_TTL and FF_WORKER_HEARTBEAT: the fallback's
	// staleness bound and the LOST mark's "unseen since".
	LeaseTTL, Heartbeat time.Duration
	Errors              *prometheus.CounterVec // flowforge_loop_errors_total; optional
}

var _ scheduler.Loop = (*Loop)(nil)

// Name implements scheduler.Loop.
func (l *Loop) Name() string { return "recovery" }

// Run implements scheduler.Loop (see scheduler.RunTicking).
func (l *Loop) Run(ctx context.Context, gate scheduler.Gate) error {
	return scheduler.RunTicking(ctx, l.Name(), l.Clock, l.Tick, nil, l.Errors, func(ctx context.Context) error { return l.Once(ctx, gate) })
}

// Once makes one recovery pass. A provider or listing failure (the database is down) and a stale
// epoch are returned; a worker or attempt that fails is logged, counted and retried next pass.
func (l *Loop) Once(ctx context.Context, gate scheduler.Gate) error {
	expired, err := l.Leases.Expired(ctx)
	if err != nil {
		return fmt.Errorf("list expired worker leases: %w", err)
	}
	for _, w := range expired {
		if !leader(ctx, gate) {
			return nil
		}
		if err := l.abandonWorker(ctx, gate, w); err != nil {
			if stop := l.failed(ctx, err, "worker_id", w); stop != nil {
				return stop
			}
		}
	}
	alive, err := l.Leases.Alive(ctx)
	if err != nil {
		return fmt.Errorf("list live workers: %w", err)
	}
	ids := make([]string, len(alive))
	for i, w := range alive {
		ids[i] = w.WorkerID
	}
	before := l.Clock.Now().Add(-(3*l.Heartbeat + l.LeaseTTL))
	orphans, err := l.Recovery.OrphanedAttempts(ctx, l.DB, ids, before, batch)
	if err != nil {
		return fmt.Errorf("list orphaned attempts: %w", err)
	}
	for _, a := range orphans {
		if !leader(ctx, gate) {
			return nil
		}
		if err := l.abandon(ctx, gate, a); err != nil {
			if stop := l.failed(ctx, err, "attempt_id", a.ID); stop != nil {
				return stop
			}
		}
	}
	return nil
}

// leader reports whether decisions may still be made: ctx alive and the term not over.
func leader(ctx context.Context, gate scheduler.Gate) bool {
	select {
	case <-ctx.Done():
		return false
	case <-gate.Done():
		return false
	default:
		return true
	}
}
