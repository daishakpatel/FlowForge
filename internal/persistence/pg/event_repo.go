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

var _ persistence.EventStreamRepo = EventRepo{}

// Stream implements persistence.EventStreamRepo. A run filter uses ix_events_run; without one the
// query walks the primary key from afterID.
func (EventRepo) Stream(ctx context.Context, q persistence.Querier, f persistence.EventStreamFilter, afterID, upTo int64, limit int) ([]persistence.EventView, error) {
	lim := limitOf(limit)
	var rows []sqlcgen.StreamEventsRow
	var err error
	if f.RunID == "" {
		rows, err = sqlcgen.New(q).StreamEvents(ctx, sqlcgen.StreamEventsParams{AfterID: afterID, UpTo: upTo, Types: f.Types, Since: timestamptz(f.Since), Lim: lim})
	} else {
		var runRows []sqlcgen.StreamRunEventsRow
		runRows, err = sqlcgen.New(q).StreamRunEvents(ctx, sqlcgen.StreamRunEventsParams{RunID: f.RunID, AfterID: afterID, UpTo: upTo, Types: f.Types, Since: timestamptz(f.Since), Lim: lim})
		for _, r := range runRows {
			rows = append(rows, sqlcgen.StreamEventsRow(r))
		}
	}
	if err != nil {
		return nil, fmt.Errorf("stream events after %d: %w", afterID, err)
	}
	out := make([]persistence.EventView, len(rows))
	for i, r := range rows {
		out[i] = eventView(sqlcgen.WorkflowEvent{
			ID: r.ID, EventID: r.EventID, RunID: r.RunID, TaskID: r.TaskID, EventType: r.EventType, EventVersion: r.EventVersion,
			Payload: r.Payload, CorrelationID: r.CorrelationID, CreatedAt: r.CreatedAt,
		})
		out[i].Envelope = r.Envelope
	}
	return out, nil
}

// LastID implements persistence.EventStreamRepo.
func (EventRepo) LastID(ctx context.Context, q persistence.Querier) (int64, error) {
	id, err := sqlcgen.New(q).LastEventID(ctx)
	if err != nil {
		return 0, fmt.Errorf("last event id: %w", err)
	}
	return id, nil
}

// Marks implements persistence.EventStreamRepo.
func (EventRepo) Marks(ctx context.Context, q persistence.Querier, afterID int64, limit int) ([]persistence.EventMark, error) {
	rows, err := sqlcgen.New(q).EventMarksAfter(ctx, sqlcgen.EventMarksAfterParams{AfterID: afterID, Lim: limitOf(limit)})
	if err != nil {
		return nil, fmt.Errorf("event ids after %d: %w", afterID, err)
	}
	out := make([]persistence.EventMark, len(rows))
	for i, r := range rows {
		out[i] = persistence.EventMark{ID: r.ID, CreatedAt: r.CreatedAt.Time}
	}
	return out, nil
}
