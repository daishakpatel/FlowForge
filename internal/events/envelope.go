package events

import (
	"encoding/json"

	pkgevents "github.com/daishakpatel/flowforge/pkg/events"
)

// Envelope is the 00E §7 event envelope, stored in outbox.envelope and published as the Kafka
// record value: pkg/events.Envelope with the payload kept as raw JSON (task 05.01 moved the type
// there, next to its JSON Schema, flowforge_event.v1). Consumers ignore unknown fields; fields are
// only ever added (00C §9).
type Envelope = pkgevents.Envelope[json.RawMessage]
