package pg

import (
	"context"
	"fmt"

	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/persistence/sqlcgen"
)

// EventRepo implements persistence.EventRepo.
type EventRepo struct{}

var _ persistence.EventRepo = EventRepo{}

// ListByRun implements persistence.EventRepo. limit is clamped like persistence.Page.
func (EventRepo) ListByRun(ctx context.Context, q persistence.Querier, runID string, afterID int64, limit int) ([]persistence.EventView, error) {
	rows, err := sqlcgen.New(q).ListEventsByRun(ctx, sqlcgen.ListEventsByRunParams{
		RunID: runID, AfterID: afterID, Lim: limitOf(persistence.Page{Limit: limit}.EffectiveLimit()),
	})
	if err != nil {
		return nil, fmt.Errorf("list events of run %s: %w", runID, err)
	}
	out := make([]persistence.EventView, len(rows))
	for i, row := range rows {
		out[i] = eventView(row)
	}
	return out, nil
}
