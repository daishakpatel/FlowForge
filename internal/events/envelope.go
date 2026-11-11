package events

import (
	"encoding/json"
	"time"

	"github.com/daishakpatel/flowforge/internal/workflow/statemachine"
)

// Envelope is the 00E §7 event envelope, stored in outbox.envelope and published as the Kafka
// record value. Consumers ignore unknown fields; fields are only ever added (00C §9).
type Envelope struct {
	EventID       string                 `json:"event_id"`
	EventType     statemachine.EventType `json:"event_type"`
	EventVersion  int                    `json:"event_version"`
	OccurredAt    time.Time              `json:"occurred_at"` // UTC
	Producer      string                 `json:"producer"`
	CorrelationID string                 `json:"correlation_id"`
	AggregateID   string                 `json:"aggregate_id"`
	Trace         string                 `json:"trace,omitempty"` // W3C traceparent once tracing lands (08.01)
	Payload       json.RawMessage        `json:"payload"`
}
