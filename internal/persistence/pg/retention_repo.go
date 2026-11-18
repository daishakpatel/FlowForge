package pg

import (
	"context"
	"fmt"
	"time"

	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/persistence/sqlcgen"
)

// RetentionRepo implements persistence.RetentionRepo.
type RetentionRepo struct{}

var _ persistence.RetentionRepo = RetentionRepo{}

// Purge implements persistence.RetentionRepo.
func (RetentionRepo) Purge(ctx context.Context, q persistence.Querier, target persistence.RetentionTarget, before time.Time, limit int) (int64, error) {
	db, at, lim := sqlcgen.New(q), timestamptz(before), limitOf(limit)
	var n int64
	var err error
	switch target {
	case persistence.RetainOutbox:
		n, err = db.DeletePublishedOutbox(ctx, sqlcgen.DeletePublishedOutboxParams{Before: at, Lim: lim})
	case persistence.RetainEvents:
		n, err = db.DeleteOldEvents(ctx, sqlcgen.DeleteOldEventsParams{Before: at, Lim: lim})
	case persistence.RetainTaskLogs:
		n, err = db.DeleteOldTaskLogs(ctx, sqlcgen.DeleteOldTaskLogsParams{Before: at, Lim: lim})
	case persistence.RetainIdempotencyKeys:
		n, err = db.ClearOldIdempotencyKeys(ctx, sqlcgen.ClearOldIdempotencyKeysParams{Before: at, Lim: lim})
	default:
		return 0, fmt.Errorf("unknown retention target %q", target)
	}
	if err != nil {
		return 0, fmt.Errorf("purge %s: %w", target, err)
	}
	return n, nil
}
