// Package producer publishes market-data ticks to Kafka (task 05.03, FR-KAFKA-05): it validates
// each tick with the domain rules and the market_tick.v1 schema, wraps it in the event envelope
// (00E §7), keys it by symbol so a symbol's ticks stay in order on one partition, and sends ticks
// that fail validation to the market.invalid DLQ with a reason, unless told to let them through.
package producer

import (
	"cmp"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/kafka"
	"github.com/daishakpatel/flowforge/internal/stream/domain"
	"github.com/daishakpatel/flowforge/pkg/events"
)

// Topics, event types and headers (00E §7).
const (
	TopicTicks   = "market.ticks"
	TopicInvalid = "market.invalid"
	// HeaderEventID carries the envelope's event_id, the consumers' dedupe key (as the outbox's).
	HeaderEventID = "event_id"
	// HeaderReason carries a DLQ record's reason.
	HeaderReason = "reason"
	// ReasonSchemaInvalid rejects a tick that passes the domain rules but not market_tick.v1.
	ReasonSchemaInvalid domain.Reason = "schema_invalid"

	eventTypeTick    = "market.tick"
	eventTypeInvalid = "market.invalid"
	maxDetail        = 1024 // market_invalid.v1 detail maxLength
	maxID            = 128  // envelope.v1 event_id, correlation_id and aggregate_id maxLength
)

// Publisher sends records and waits for their acknowledgement; *kafka.Producer is one.
type Publisher interface {
	Produce(ctx context.Context, recs ...*kgo.Record) error
}

// Options configures a Producer.
type Options struct {
	// AllowInvalid sends ticks that fail validation to market.ticks anyway, so the processor's
	// DLQ path can be tested (--allow-invalid). Off: they go to market.invalid.
	AllowInvalid bool
	// Name is the envelope's producer; default "flowforge-producer".
	Name string
	// TicksTopic and InvalidTopic default to market.ticks and market.invalid (tests use their own).
	TicksTopic, InvalidTopic string
}

// Producer turns ticks into records and publishes them. It is safe for concurrent use when its
// Publisher is.
type Producer struct {
	pub   Publisher
	reg   *events.Registry
	clock clock.Clock
	m     *Metrics
	opts  Options
}

// New returns a Producer that publishes through pub, validates with reg, and judges event times
// against c (domain.Validate's future and too-old bounds).
func New(pub Publisher, reg *events.Registry, c clock.Clock, m *Metrics, opts Options) *Producer {
	opts.Name = cmp.Or(opts.Name, "flowforge-producer")
	opts.TicksTopic = cmp.Or(opts.TicksTopic, TopicTicks)
	opts.InvalidTopic = cmp.Or(opts.InvalidTopic, TopicInvalid)
	return &Producer{pub: pub, reg: reg, clock: c, m: m, opts: opts}
}

// Result counts what one Publish call sent.
type Result struct {
	Published     int // valid ticks on market.ticks
	Rejected      int // invalid ticks on market.invalid
	ForcedInvalid int // invalid ticks on market.ticks (AllowInvalid)
}

// tickPayload is the market_tick.v1 payload. Prices and sizes are JSON numbers written from the
// exact decimals.
type tickPayload struct {
	EventID   string      `json:"event_id"`
	Symbol    string      `json:"symbol"`
	EventTime time.Time   `json:"event_time"`
	Price     json.Number `json:"price"`
	Size      json.Number `json:"size"`
	Source    string      `json:"source"`
	Sequence  int64       `json:"sequence"`
}

// invalidPayload is the market_invalid.v1 payload of a tick rejected before publishing: it was on
// no topic, so the source fields are absent (00A amendment 05.03).
type invalidPayload struct {
	Reason   domain.Reason `json:"reason"`
	Detail   string        `json:"detail,omitempty"`
	Original string        `json:"original"` // the tick payload, base64
}

// Publish validates ticks and publishes them in one batch, waiting until every record is
// acknowledged. On error the batch may be partly written; retrying it is safe because consumers
// dedupe on event_id.
func (p *Producer) Publish(ctx context.Context, ticks []domain.Tick) (Result, error) {
	if len(ticks) == 0 {
		return Result{}, nil
	}
	now := p.clock.Now()
	recs := make([]*kgo.Record, 0, len(ticks))
	var res Result
	results := make([]string, 0, len(ticks))
	for _, t := range ticks {
		rec, kind, err := p.record(t, now)
		if err != nil {
			return Result{}, err
		}
		recs = append(recs, rec)
		results = append(results, kind)
		switch kind {
		case resultPublished:
			res.Published++
		case resultInvalid:
			res.Rejected++
		default:
			res.ForcedInvalid++
		}
	}
	start := p.clock.Now()
	err := p.pub.Produce(ctx, recs...)
	p.m.Latency.Observe(p.clock.Now().Sub(start).Seconds())
	if err != nil {
		p.m.Events.WithLabelValues(resultError).Add(float64(len(recs)))
		return Result{}, fmt.Errorf("publish %d ticks: %w", len(recs), err)
	}
	for _, r := range results {
		p.m.Events.WithLabelValues(r).Inc()
	}
	return res, nil
}

// record builds t's record and says where it goes: published, invalid (DLQ) or forced_invalid.
func (p *Producer) record(t domain.Tick, now time.Time) (*kgo.Record, string, error) {
	payload := tickPayload{
		EventID: t.EventID, Symbol: t.Symbol, EventTime: t.EventTime.UTC(), Price: json.Number(t.Price.String()),
		Size: json.Number(t.Size.String()), Source: t.Source, Sequence: t.Seq,
	}
	value, err := events.Marshal(events.Envelope[tickPayload]{
		EventID: t.EventID, EventType: eventTypeTick, EventVersion: 1, OccurredAt: t.EventTime, Producer: p.opts.Name,
		CorrelationID: t.Symbol, AggregateID: t.Symbol, Payload: payload,
	})
	if err != nil {
		return nil, "", err
	}
	reason, detail := domain.Validate(t, now), ""
	if reason == domain.ReasonNone {
		var ve *events.ValidationError
		switch err := p.reg.Validate("envelope.v1", value); {
		case errors.As(err, &ve):
			reason, detail = ReasonSchemaInvalid, ve.Reason
		case err != nil:
			return nil, "", fmt.Errorf("validate tick %s: %w", t.EventID, err)
		}
	}
	switch {
	case reason == domain.ReasonNone:
		return p.tickRecord(t, value), resultPublished, nil
	case p.opts.AllowInvalid:
		return p.tickRecord(t, value), resultForcedInvalid, nil
	}
	rec, err := p.invalidRecord(t, payload, reason, detail)
	return rec, resultInvalid, err
}

// clip cuts s to the envelope's 128-character id limit.
func clip(s string) string {
	if r := []rune(s); len(r) > maxID {
		return string(r[:maxID])
	}
	return s
}

func (p *Producer) tickRecord(t domain.Tick, value []byte) *kgo.Record {
	r := &kgo.Record{Topic: p.opts.TicksTopic, Key: []byte(t.Symbol), Value: value}
	kafka.SetHeader(r, HeaderEventID, t.EventID)
	return r
}

// invalidRecord wraps a rejected tick for market.invalid (key = event id, 00E §7).
func (p *Producer) invalidRecord(t domain.Tick, payload tickPayload, reason domain.Reason, detail string) (*kgo.Record, error) {
	original, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal rejected tick: %w", err)
	}
	if len(detail) > maxDetail {
		detail = detail[:maxDetail-3] + "..."
	}
	// The envelope caps ids at 128 characters; a longer one is cut here and kept whole in original,
	// so one malformed row reaches the DLQ instead of stopping the run.
	id := clip(cmp.Or(t.EventID, "unknown"))
	agg := clip(cmp.Or(t.Symbol, id))
	value, err := events.Marshal(events.Envelope[invalidPayload]{
		EventID: id, EventType: eventTypeInvalid, EventVersion: 1, OccurredAt: p.clock.Now(), Producer: p.opts.Name,
		CorrelationID: agg, AggregateID: agg,
		Payload: invalidPayload{Reason: reason, Detail: detail, Original: base64.StdEncoding.EncodeToString(original)},
	})
	if err != nil {
		return nil, err
	}
	if err := p.reg.Validate("envelope.v1", value); err != nil {
		return nil, fmt.Errorf("DLQ record for %s does not match market_invalid.v1: %w", id, err) // a bug here, not bad input
	}
	r := &kgo.Record{Topic: p.opts.InvalidTopic, Key: []byte(id), Value: value}
	kafka.SetHeader(r, HeaderEventID, id)
	kafka.SetHeader(r, HeaderReason, string(reason))
	return r, nil
}
