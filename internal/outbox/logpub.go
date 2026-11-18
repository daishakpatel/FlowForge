package outbox

import (
	"context"
	"log/slog"
)

// LogPublisher "publishes" by logging each message's topic, key and event id at debug level
// (FF_EVENT_PUBLISHER=log, the default until Kafka arrives in 05.01). The envelope itself is
// never logged (00C §10.2). It never fails.
type LogPublisher struct {
	Logger *slog.Logger
}

var _ Publisher = LogPublisher{}

// Publish implements Publisher.
func (p LogPublisher) Publish(ctx context.Context, msgs []Message) error {
	for _, m := range msgs {
		p.Logger.DebugContext(ctx, "event published", "topic", m.Topic, "key", m.Key, "event_id", m.Headers[HeaderEventID])
	}
	return nil
}

// DiscardPublisher drops every message (FF_EVENT_PUBLISHER=none). The relay still marks the rows
// published, so the backlog stays empty and retention can delete them.
type DiscardPublisher struct{}

var _ Publisher = DiscardPublisher{}

// Publish implements Publisher.
func (DiscardPublisher) Publish(context.Context, []Message) error { return nil }
