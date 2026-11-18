package pg

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/persistence/sqlcgen"
)

// OutboxRepo implements persistence.OutboxRepo.
type OutboxRepo struct{}

var _ persistence.OutboxRepo = OutboxRepo{}

// LockUnpublished implements persistence.OutboxRepo.
func (OutboxRepo) LockUnpublished(ctx context.Context, tx pgx.Tx, limit int) ([]persistence.OutboxView, error) {
	rows, err := sqlcgen.New(tx).LockUnpublishedOutbox(ctx, int32(min(limit, math.MaxInt32))) //nolint:gosec // G115: clamped
	if err != nil {
		return nil, fmt.Errorf("lock unpublished outbox rows: %w", err)
	}
	out := make([]persistence.OutboxView, len(rows))
	for i, r := range rows {
		out[i] = persistence.OutboxView{ID: r.ID, EventID: r.EventID, Topic: r.Topic, Key: r.Key, Envelope: r.Envelope, CreatedAt: r.CreatedAt.Time}
	}
	return out, nil
}

// MarkPublished implements persistence.OutboxRepo.
func (OutboxRepo) MarkPublished(ctx context.Context, tx pgx.Tx, ids []int64, at time.Time) error {
	if err := sqlcgen.New(tx).MarkOutboxPublished(ctx, sqlcgen.MarkOutboxPublishedParams{PublishedAt: timestamptz(at), Ids: ids}); err != nil {
		return fmt.Errorf("mark %d outbox rows published: %w", len(ids), err)
	}
	return nil
}

// Backlog implements persistence.OutboxRepo.
func (OutboxRepo) Backlog(ctx context.Context, q persistence.Querier) (int64, *time.Time, error) {
	row, err := sqlcgen.New(q).OutboxBacklog(ctx)
	if err != nil {
		return 0, nil, fmt.Errorf("outbox backlog: %w", err)
	}
	return row.Backlog, timePtr(row.Oldest), nil
}
