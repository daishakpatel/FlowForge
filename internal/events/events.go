// Package events defines the 00E §7 workflow event types and records them: one workflow_events
// row (the run's history, read by SSE) and one outbox row (published to Kafka by the relay) per
// event, both inside the caller's transaction (ADR-009, task 02.03).
package events

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/ids"
	"github.com/daishakpatel/flowforge/internal/logging"
	"github.com/daishakpatel/flowforge/internal/persistence/sqlcgen"
)

// Recorder writes events. It is safe for concurrent use when its clock and ID generator are.
type Recorder struct {
	clock    clock.Clock
	ids      ids.Generator
	producer string
}

// NewRecorder returns a Recorder that stamps occurred_at from c, takes event IDs from g and
// names producer (for example "flowforge-api") in every envelope.
func NewRecorder(c clock.Clock, g ids.Generator, producer string) *Recorder {
	return &Recorder{clock: c, ids: g, producer: producer}
}

// Record inserts e into workflow_events (when it belongs to a run) and outbox, in tx. It takes a
// transaction, not a pool, because the event must commit or roll back with the state change it
// describes (ADR-009). The outbox key is the run id, so a run's events stay ordered in Kafka;
// cluster events use their aggregate id. correlation_id is the request id from ctx
// (logging.WithFields) when present, else the run id.
func (r *Recorder) Record(ctx context.Context, tx pgx.Tx, e Event) error {
	if err := e.validate(); err != nil {
		return err
	}
	payload, err := json.Marshal(e.payload)
	if err != nil {
		return fmt.Errorf("marshal %s payload: %w", e.Type, err)
	}
	env := Envelope{
		EventID:       r.ids.New(ids.PrefixEvent),
		EventType:     e.Type,
		EventVersion:  e.Version,
		OccurredAt:    r.clock.Now().UTC(),
		Producer:      r.producer,
		CorrelationID: correlationID(ctx, e),
		AggregateID:   e.AggregateID,
		Payload:       payload,
	}
	envelope, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal %s envelope: %w", e.Type, err)
	}

	q := sqlcgen.New(tx)
	key := e.RunID
	if key == "" {
		key = e.AggregateID
	} else {
		err := q.InsertWorkflowEvent(ctx, sqlcgen.InsertWorkflowEventParams{
			EventID: env.EventID, RunID: e.RunID, TaskID: text(e.TaskID), EventType: string(e.Type),
			EventVersion: int32(e.Version), //nolint:gosec // G115: event versions are small constants
			Payload:      payload, CorrelationID: env.CorrelationID,
		})
		if err != nil {
			return fmt.Errorf("insert workflow event %s: %w", e.Type, err)
		}
	}
	err = q.InsertOutbox(ctx, sqlcgen.InsertOutboxParams{
		EventID: env.EventID, Topic: Topic(e.Type), Key: key, Envelope: envelope,
	})
	if err != nil {
		return fmt.Errorf("insert outbox %s: %w", e.Type, err)
	}
	return nil
}

func correlationID(ctx context.Context, e Event) string {
	if v, ok := logging.Field(ctx, logging.KeyRequestID); ok && v.String() != "" {
		return v.String()
	}
	if e.RunID != "" {
		return e.RunID
	}
	return e.AggregateID
}

func text(s string) pgtype.Text { return pgtype.Text{String: s, Valid: s != ""} }
