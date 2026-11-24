package workerapi

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/daishakpatel/flowforge/internal/apperrors"
	"github.com/daishakpatel/flowforge/internal/coordination"
	"github.com/daishakpatel/flowforge/internal/logging"
	"github.com/daishakpatel/flowforge/internal/persistence"
)

// defaultRaftTimeout bounds the Raft calls on the heartbeat path, so a Raft outage never stalls a
// worker's Register, Heartbeat or Deregister.
const defaultRaftTimeout = 2 * time.Second

// RaftLeaseProvider implements LeaseProvider with Raft leases (task 04.07, FR-RAFT-09, ADR-003).
// Raft is the liveness authority: each worker holds a /workers/{id} key bound to a TTL lease
// (coordination.WorkerRegistry). Postgres still records every registration and heartbeat
// (worker_observations, through PG): the worker protocol reads it, and it guards recovery.
//
//   - Register and Renew go to both. A Raft failure other than a conflict is degraded: logged,
//     counted in Metrics (flowforge_worker_lease_renewal_failures_total) and the call succeeds,
//     so heartbeats are accepted through a Raft outage (00_MASTER §6.2). The first heartbeat
//     after the outage restores the Raft lease, re-registering it when it expired or is gone.
//   - Expired reports a worker only when Raft holds no live lease for it AND Postgres has not
//     seen it within the TTL. Raft expiry is necessary; the Postgres check stops a worker that
//     kept heartbeating through an outage, whose Raft lease expired meanwhile, from being
//     abandoned before its next heartbeat restores the lease. When Raft can't be read, Expired
//     and Alive fail, and recovery abandons nothing (fail safe).
//
// Give the Registry no metrics of its own: Metrics here counts every Raft failure once.
type RaftLeaseProvider struct {
	PG       *PGLeaseProvider
	Registry *coordination.WorkerRegistry
	Metrics  *coordination.RegistryMetrics // optional
	Timeout  time.Duration                 // per Raft call in Register, Renew and Release; default 2s
}

var _ LeaseProvider = (*RaftLeaseProvider)(nil)

// Register implements LeaseProvider. A worker id live in Raft under another API key is CONFLICT.
func (p *RaftLeaseProvider) Register(ctx context.Context, w persistence.WorkerRegistration) error {
	rctx, cancel := p.bounded(ctx)
	_, err := p.Registry.Register(rctx, coordination.WorkerInfo{
		ID: w.WorkerID, KeyID: w.KeyID, Capabilities: w.TaskTypes, Queues: w.Queues, Capacity: w.Capacity, Version: w.Version,
	})
	cancel()
	switch {
	case errors.Is(err, coordination.ErrWorkerConflict):
		return apperrors.Conflict(fmt.Sprintf("worker %s is registered and live under another API key", w.WorkerID))
	case err != nil:
		p.degraded(ctx, "register", w.WorkerID, err)
	}
	return p.PG.Register(ctx, w)
}

// Renew implements LeaseProvider: Postgres first (INVALID_STATE for an unregistered worker),
// then the Raft lease, degrading on failure.
func (p *RaftLeaseProvider) Renew(ctx context.Context, workerID, keyID string, activeTasks int) error {
	if err := p.PG.Renew(ctx, workerID, keyID, activeTasks); err != nil {
		return err
	}
	rctx, cancel := p.bounded(ctx)
	defer cancel()
	err := p.renewRaft(rctx, workerID, keyID)
	switch {
	case errors.Is(err, coordination.ErrWorkerConflict):
		return apperrors.Conflict(fmt.Sprintf("worker %s is live in Raft under another API key", workerID))
	case errors.Is(err, coordination.ErrNotRegistered):
		// Deregistered between the Postgres check and now: there is nothing to renew.
	case err != nil:
		p.degraded(ctx, "renew", workerID, err)
	}
	return nil
}

// renewRaft extends keyID's Raft lease of the worker, registering it again when it has none and
// Postgres still holds an ACTIVE registration of it by keyID: then Raft lost the lease or never
// got it. It never extends another key's lease (ErrWorkerConflict), and never revives a worker
// deregistered meanwhile (ErrNotRegistered).
func (p *RaftLeaseProvider) renewRaft(ctx context.Context, workerID, keyID string) error {
	h, err := p.Registry.Handle(ctx, workerID)
	switch {
	case err == nil && h.Info.KeyID != keyID:
		return fmt.Errorf("%w: %s", coordination.ErrWorkerConflict, workerID)
	case err == nil:
		if _, err = p.Registry.Renew(ctx, h); !errors.Is(err, coordination.ErrNotRegistered) {
			return err
		}
	case !errors.Is(err, coordination.ErrNotRegistered):
		return err
	}
	w, err := p.PG.Workers.Get(ctx, p.PG.DB, workerID)
	if err != nil {
		return err
	}
	if w.Status != "ACTIVE" || w.KeyID != keyID {
		return fmt.Errorf("%w: %s is no longer an active registration of this key", coordination.ErrNotRegistered, workerID)
	}
	_, err = p.Registry.Register(ctx, coordination.WorkerInfo{
		ID: workerID, KeyID: w.KeyID, Capabilities: w.TaskTypes, Queues: w.Queues, Capacity: w.Capacity, Version: w.Version,
	})
	return err
}

// Release implements LeaseProvider. A Raft failure is only logged: the lease expires anyway.
func (p *RaftLeaseProvider) Release(ctx context.Context, workerID, keyID string) error {
	if err := p.PG.Release(ctx, workerID, keyID); err != nil {
		return err
	}
	rctx, cancel := p.bounded(ctx)
	defer cancel()
	h, err := p.Registry.Handle(rctx, workerID)
	if err == nil && h.Info.KeyID == keyID {
		err = p.Registry.Deregister(rctx, h)
	}
	if err != nil && !errors.Is(err, coordination.ErrNotRegistered) {
		logging.FromContext(ctx).WarnContext(ctx, "raft worker lease release failed; it will expire",
			"worker_id", workerID, logging.Err(err))
	}
	return nil
}

// Expired implements LeaseProvider: the workers Postgres has not seen within the TTL that also
// hold no live Raft lease (a linearizable read). An error means nothing may be abandoned.
func (p *RaftLeaseProvider) Expired(ctx context.Context) ([]string, error) {
	stale, err := p.PG.Expired(ctx)
	if err != nil || len(stale) == 0 {
		return nil, err
	}
	return p.Registry.Expired(ctx, stale)
}

// Alive implements LeaseProvider: the ACTIVE workers holding a live Raft lease.
func (p *RaftLeaseProvider) Alive(ctx context.Context) ([]persistence.WorkerView, error) {
	live, err := p.Registry.List(ctx)
	if err != nil {
		return nil, err
	}
	ids := make(map[string]bool, len(live))
	for _, w := range live {
		ids[w.ID] = true
	}
	all, err := p.PG.Workers.List(ctx, p.PG.DB)
	if err != nil {
		return nil, err
	}
	var alive []persistence.WorkerView
	for _, w := range all {
		if w.Status == "ACTIVE" && ids[w.WorkerID] {
			alive = append(alive, w)
		}
	}
	return alive, nil
}

func (p *RaftLeaseProvider) bounded(ctx context.Context) (context.Context, context.CancelFunc) {
	t := p.Timeout
	if t <= 0 {
		t = defaultRaftTimeout
	}
	return context.WithTimeout(ctx, t)
}

// degraded logs and counts a Raft lease failure the caller carries on from (00_MASTER §6.2).
func (p *RaftLeaseProvider) degraded(ctx context.Context, op, workerID string, err error) {
	logging.FromContext(ctx).WarnContext(ctx, "raft worker lease "+op+" failed; continuing on Postgres",
		"worker_id", workerID, logging.Err(err))
	if p.Metrics != nil {
		p.Metrics.RenewalFailures.Inc()
	}
}
