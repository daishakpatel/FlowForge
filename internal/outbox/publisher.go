// Package outbox publishes the transactional outbox (task 03.07, ADR-009, FR-WF-15). State changes
// insert an outbox row in their own transaction (events.Recorder); RelayLoop, on the leader,
// hands the rows to a Publisher in id order and marks the delivered prefix. Delivery is at least
// once and ordered per key (the run id); consumers dedupe on event_id.
//
// The Publisher is chosen by FF_EVENT_PUBLISHER: log (LogPublisher), none (DiscardPublisher) or
// kafka (05.01). RelayLoop does not change when the publisher does.
package outbox

import (
	"context"
	"errors"
	"fmt"
)

// Message is one outbox row to publish.
type Message struct {
	Topic   string            // flowforge.workflow-events or flowforge.task-events (00E §7)
	Key     string            // the run id: a run's messages share a partition and stay ordered
	Value   []byte            // the 00E §7 envelope JSON
	Headers map[string]string // HeaderEventID
}

// HeaderEventID carries the envelope's event_id, the consumers' dedupe key.
const HeaderEventID = "event_id"

// Publisher delivers messages, for example to Kafka (05.01).
//
// Contract, which RelayLoop's ordering and at-least-once guarantees rely on:
//   - Messages with the same Key are delivered in slice order, and none is delivered after one of
//     the same Key failed.
//   - Publish returns nil when every message was delivered. When only the first n were, it
//     returns a *PartialError with Published = n. Any other error means no message can be
//     assumed delivered: all are retried, so some may arrive twice.
//   - Publish honors ctx; RelayLoop bounds it with its PublishTimeout.
type Publisher interface {
	Publish(ctx context.Context, msgs []Message) error
}

// PartialError reports that only the first Published messages of a batch were delivered.
type PartialError struct {
	Published int
	Err       error
}

func (e *PartialError) Error() string {
	return fmt.Sprintf("published %d messages, then: %v", e.Published, e.Err)
}

func (e *PartialError) Unwrap() error { return e.Err }

// Delivered returns how many leading messages of a batch of n the result of Publish says were
// delivered: n for nil, PartialError.Published (clamped to 0..n) for a partial failure, else 0.
func Delivered(err error, n int) int {
	if err == nil {
		return n
	}
	var pe *PartialError
	if errors.As(err, &pe) {
		return max(0, min(pe.Published, n))
	}
	return 0
}
