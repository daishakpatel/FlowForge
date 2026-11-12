package workerapi

import (
	"context"
	"fmt"
	"time"

	"github.com/daishakpatel/flowforge/internal/apperrors"
	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/persistence"
)

// PGLeaseProvider implements LeaseProvider with worker_observations: a worker is alive while it
// is ACTIVE and was seen within TTL, measured on Clock (ADR-021).
type PGLeaseProvider struct {
	DB      persistence.Querier
	Workers persistence.WorkerRepo
	Clock   clock.Clock
	TTL     time.Duration // FF_WORKER_LEASE_TTL
}

var _ LeaseProvider = (*PGLeaseProvider)(nil)

// Register implements LeaseProvider.
func (p *PGLeaseProvider) Register(ctx context.Context, w persistence.WorkerRegistration) error {
	now := p.Clock.Now()
	ok, err := p.Workers.Upsert(ctx, p.DB, w, now, now.Add(-p.TTL))
	if err == nil && !ok {
		return apperrors.Conflict(fmt.Sprintf("worker %s is registered and live under another API key", w.WorkerID))
	}
	return err
}

// Renew implements LeaseProvider.
func (p *PGLeaseProvider) Renew(ctx context.Context, workerID, keyID string, activeTasks int) error {
	ok, err := p.Workers.Touch(ctx, p.DB, workerID, keyID, activeTasks, p.Clock.Now())
	if err == nil && !ok {
		return apperrors.InvalidState(fmt.Sprintf("worker %s is not registered: call Register", workerID))
	}
	return err
}

// Release implements LeaseProvider.
func (p *PGLeaseProvider) Release(ctx context.Context, workerID, keyID string) error {
	return p.Workers.Release(ctx, p.DB, workerID, keyID, p.Clock.Now())
}

// Expired implements LeaseProvider.
func (p *PGLeaseProvider) Expired(ctx context.Context) ([]string, error) {
	return p.Workers.Expired(ctx, p.DB, p.Clock.Now().Add(-p.TTL))
}

// Alive implements LeaseProvider.
func (p *PGLeaseProvider) Alive(ctx context.Context) ([]persistence.WorkerView, error) {
	all, err := p.Workers.List(ctx, p.DB)
	if err != nil {
		return nil, err
	}
	liveAfter := p.Clock.Now().Add(-p.TTL)
	var alive []persistence.WorkerView
	for _, w := range all {
		if w.Alive(liveAfter) {
			alive = append(alive, w)
		}
	}
	return alive, nil
}
