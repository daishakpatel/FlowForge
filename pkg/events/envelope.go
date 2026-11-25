// Package events is the FlowForge event contract (00E §7, task 05.01): the versioned envelope
// every Kafka record value carries, and a validator over the JSON Schemas in schemas/events.
//
// Compatibility rules (docs/architecture/event-schemas.md):
//  1. Consumers ignore unknown fields (Unmarshal does; the schemas allow them).
//  2. Within a schema version, fields are only added, and only as optional ones; removing,
//     renaming or retyping a field, or making it required, is a new version (…v2) published
//     alongside the old one during a deprecation window. pkg/events' compatibility test enforces it
//     against schemas/baseline.
//  3. event_id is globally unique: consumers dedupe on it.
//  4. The Kafka record key is the envelope's partition key (00E §7: run_id, symbol, …).
package events

import (
	"encoding/json"
	"fmt"
	"time"
)

// Envelope is the 00E §7 event envelope with a payload of type T. internal/events records
// workflow events as Envelope[json.RawMessage]; market data uses typed payloads.
type Envelope[T any] struct {
	EventID       string    `json:"event_id"`        // globally unique; the consumers' dedupe key
	EventType     string    `json:"event_type"`      // e.g. task.queued, market.tick
	EventVersion  int       `json:"event_version"`   // the payload schema version
	OccurredAt    time.Time `json:"occurred_at"`     // RFC 3339, UTC
	Producer      string    `json:"producer"`        // the service that wrote it, e.g. flowforge-scheduler
	CorrelationID string    `json:"correlation_id"`  // request id, else run id, else the aggregate id
	AggregateID   string    `json:"aggregate_id"`    // the entity it is about: task, run, symbol, …
	Trace         string    `json:"trace,omitempty"` // W3C traceparent, once tracing lands (08.01)
	Payload       T         `json:"payload"`
}

// Marshal encodes e with occurred_at in UTC. It does not validate; producers call
// Registry.Validate on the result before publishing.
func Marshal[T any](e Envelope[T]) ([]byte, error) {
	e.OccurredAt = e.OccurredAt.UTC()
	b, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("marshal %s envelope: %w", e.EventType, err)
	}
	return b, nil
}

// Unmarshal decodes an envelope, ignoring unknown fields (rule 1). It checks only that known
// fields have their Go types; validate against the schema first to check the rest.
func Unmarshal[T any](data []byte) (Envelope[T], error) {
	var e Envelope[T]
	if err := json.Unmarshal(data, &e); err != nil {
		return Envelope[T]{}, fmt.Errorf("unmarshal event envelope: %w", err)
	}
	return e, nil
}
