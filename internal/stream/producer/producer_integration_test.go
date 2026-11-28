//go:build integration

package producer_test

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/kafka"
	"github.com/daishakpatel/flowforge/internal/kafka/kafkatest"
	"github.com/daishakpatel/flowforge/internal/obs"
	"github.com/daishakpatel/flowforge/internal/stream/domain"
	"github.com/daishakpatel/flowforge/internal/stream/producer"
	"github.com/daishakpatel/flowforge/internal/stream/source"
	"github.com/daishakpatel/flowforge/pkg/events"
)

// kafkaRun publishes count synthetic ticks (ending at now on a fake clock) to fresh topics and
// returns the summary and the topics.
func kafkaRun(t *testing.T, k *kafkatest.Broker, count int, in source.Inject, allowInvalid bool) (producer.Summary, string, string) {
	t.Helper()
	ticksTopic, invalidTopic := k.Topic(t, 6), k.Topic(t, 3)
	kp, err := kafka.NewProducer(k.Config())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { kp.Close(context.Background()) })
	reg, err := events.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	m, err := producer.NewMetrics(obs.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	fake := clock.NewFake(now)
	const rate = 1000
	src, err := source.NewSynthetic(source.Config{
		Seed: 11, Symbols: []string{"ACME", "BOLT", "CRUX"}, Rate: rate, Mode: source.ModeSimulated,
		Start: now.Add(-time.Duration(count) * time.Second / rate), Inject: in,
	}, fake)
	if err != nil {
		t.Fatal(err)
	}
	p := producer.New(kp, reg, fake, m, producer.Options{AllowInvalid: allowInvalid, TicksTopic: ticksTopic, InvalidTopic: invalidTopic})
	sum, err := producer.Run(t.Context(), src, p, producer.RunOptions{Count: count, Batch: 250})
	if err != nil {
		t.Fatal(err)
	}
	return sum, ticksTopic, invalidTopic
}

// consume reads exactly n records of topic, failing after 60 s.
func consume(t *testing.T, k *kafkatest.Broker, topic string, n int) []*kgo.Record {
	t.Helper()
	c, err := kafka.NewConsumer(k.Config(), kafkatest.Name("g"), topic)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	var out []*kgo.Record
	for len(out) < n && ctx.Err() == nil {
		recs, err := c.Poll(ctx, n-len(out))
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, recs...)
	}
	if len(out) != n {
		t.Fatalf("%s: got %d of %d records", topic, len(out), n)
	}
	return out
}

func decodeTick(t *testing.T, r *kgo.Record) struct {
	Symbol   string `json:"symbol"`
	Sequence int64  `json:"sequence"`
} {
	t.Helper()
	env, err := events.Unmarshal[struct {
		Symbol   string `json:"symbol"`
		Sequence int64  `json:"sequence"`
	}](r.Value)
	if err != nil {
		t.Fatal(err)
	}
	return env.Payload
}

// TestRun_PerSymbolOrderThroughKafka (task 05.03 §8): a consumer sees each symbol's ticks in the
// order they were produced, all on one partition.
func TestRun_PerSymbolOrderThroughKafka(t *testing.T) {
	t.Parallel()
	k := kafkatest.Start(t)
	sum, ticksTopic, _ := kafkaRun(t, k, 3000, source.Inject{}, false)
	if sum.Published != 3000 {
		t.Fatalf("summary %+v", sum)
	}
	lastSeq, partition := map[string]int64{}, map[string]int32{}
	for i, r := range consume(t, k, ticksTopic, 3000) {
		p := decodeTick(t, r)
		if string(r.Key) != p.Symbol {
			t.Fatalf("record %d: key %q, symbol %q", i, r.Key, p.Symbol)
		}
		if part, ok := partition[p.Symbol]; ok && part != r.Partition {
			t.Fatalf("%s on partitions %d and %d", p.Symbol, part, r.Partition)
		}
		partition[p.Symbol] = r.Partition
		if p.Sequence != lastSeq[p.Symbol]+1 {
			t.Fatalf("record %d: %s sequence %d after %d: out of order", i, p.Symbol, p.Sequence, lastSeq[p.Symbol])
		}
		lastSeq[p.Symbol] = p.Sequence
	}
}

// TestRun_InvalidTicksNeverReachTicksUnlessAllowed (task 05.03 §8).
func TestRun_InvalidTicksNeverReachTicksUnlessAllowed(t *testing.T) {
	t.Parallel()
	k := kafkatest.Start(t)
	reg, err := events.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	in := source.Inject{Invalid: 0.1}

	sum, ticksTopic, invalidTopic := kafkaRun(t, k, 1000, in, false)
	if sum.Rejected == 0 || sum.Published+sum.Rejected != 1000 || sum.ForcedInvalid != 0 {
		t.Fatalf("summary %+v", sum)
	}
	for i, r := range consume(t, k, ticksTopic, sum.Published) {
		if err := reg.Validate("envelope.v1", r.Value); err != nil {
			t.Fatalf("ticks record %d does not match its schema: %v", i, err)
		}
		env, err := events.Unmarshal[map[string]any](r.Value)
		if err != nil || env.AggregateID != string(r.Key) {
			t.Fatalf("ticks record %d: %v", i, err)
		}
	}
	for i, r := range consume(t, k, invalidTopic, sum.Rejected) {
		reason, _ := kafka.Header(r, producer.HeaderReason)
		if reason == "" || reason == string(domain.ReasonNone) {
			t.Fatalf("DLQ record %d has no reason", i)
		}
		if err := reg.Validate("envelope.v1", r.Value); err != nil {
			t.Fatalf("DLQ record %d: %v", i, err)
		}
	}

	sum, ticksTopic, _ = kafkaRun(t, k, 1000, in, true)
	if sum.ForcedInvalid == 0 || sum.Rejected != 0 || sum.Published+sum.ForcedInvalid != 1000 {
		t.Fatalf("allow-invalid summary %+v", sum)
	}
	invalid := 0
	for _, r := range consume(t, k, ticksTopic, 1000) {
		if reg.Validate("envelope.v1", r.Value) != nil {
			invalid++
		} else if env, _ := events.Unmarshal[struct{ Symbol string }](r.Value); env.Payload.Symbol != env.AggregateID {
			invalid++ // a lower-cased symbol still matches the envelope pattern
		}
	}
	if invalid == 0 {
		t.Error("--allow-invalid: no invalid tick reached the ticks topic")
	}
}
