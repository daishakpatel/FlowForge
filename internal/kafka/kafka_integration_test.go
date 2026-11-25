//go:build integration

package kafka_test

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/daishakpatel/flowforge/internal/kafka"
	"github.com/daishakpatel/flowforge/internal/kafka/kafkatest"
	"github.com/daishakpatel/flowforge/internal/testutil"
	"github.com/daishakpatel/flowforge/pkg/events"
)

func producer(t *testing.T, k *kafkatest.Broker) *kafka.Producer {
	t.Helper()
	p, err := kafka.NewProducer(k.Config())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close(context.Background()) })
	return p
}

func consumer(t *testing.T, k *kafkatest.Broker, group, topic string) *kafka.Consumer {
	t.Helper()
	c, err := kafka.NewConsumer(k.Config(), group, topic)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

// pollN polls c until it has n records or 60 s pass.
func pollN(t *testing.T, c *kafka.Consumer, n int) []*kgo.Record {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	var got []*kgo.Record
	for len(got) < n {
		recs, err := c.Poll(ctx, n-len(got))
		if err != nil {
			t.Fatalf("poll: %v", err)
		}
		if ctx.Err() != nil {
			t.Fatalf("got %d of %d records before the deadline", len(got), n)
		}
		got = append(got, recs...)
	}
	return got
}

type tickPayload struct {
	EventID   string    `json:"event_id"`
	Symbol    string    `json:"symbol"`
	EventTime time.Time `json:"event_time"`
	Price     float64   `json:"price"`
	Size      float64   `json:"size"`
	Source    string    `json:"source"`
}

func TestProduceConsume_RoundTripKeepsEnvelopeAndTraceparent(t *testing.T) {
	t.Parallel()
	k := kafkatest.Start(t)
	topic := k.Topic(t, 3)
	reg, err := events.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 11, 2, 14, 30, 0, 250_000_000, time.UTC)
	env := events.Envelope[tickPayload]{
		EventID: "3f786850e387550fdab836ed7e6dc881de23001b", EventType: "market.tick", EventVersion: 1, OccurredAt: at,
		Producer: "marketdata-producer", CorrelationID: "ACME", AggregateID: "ACME",
		Trace:   "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		Payload: tickPayload{EventID: "3f786850e387550fdab836ed7e6dc881de23001b", Symbol: "ACME", EventTime: at, Price: 101.25, Size: 300, Source: "synthetic"},
	}
	value, err := events.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	if err := reg.Validate("envelope.v1", value); err != nil {
		t.Fatal(err)
	}
	rec := &kgo.Record{Topic: topic, Key: []byte("ACME"), Value: value}
	kafka.SetHeader(rec, "event_id", env.EventID)
	ctx := kafka.WithTraceparent(t.Context(), env.Trace)
	if err := producer(t, k).Produce(ctx, rec); err != nil {
		t.Fatal(err)
	}

	got := pollN(t, consumer(t, k, kafkatest.Name("g"), topic), 1)[0]
	if string(got.Key) != "ACME" || !bytes.Equal(got.Value, value) {
		t.Errorf("record key %q value %s; want ACME, %s", got.Key, got.Value, value)
	}
	if tp := kafka.TraceparentFrom(kafka.ExtractTrace(t.Context(), got)); tp != env.Trace {
		t.Errorf("traceparent header %q, want %q", tp, env.Trace)
	}
	if id, _ := kafka.Header(got, "event_id"); id != env.EventID {
		t.Errorf("event_id header %q", id)
	}
	if err := reg.Validate("envelope.v1", got.Value); err != nil {
		t.Errorf("consumed value invalid: %v", err)
	}
	back, err := events.Unmarshal[tickPayload](got.Value)
	if err != nil {
		t.Fatal(err)
	}
	if back.EventID != env.EventID || !back.OccurredAt.Equal(at) || back.Payload != env.Payload {
		t.Errorf("decoded %+v, want %+v", back, env)
	}
}

func TestProduceConsume_PerKeyOrder(t *testing.T) {
	t.Parallel()
	const n = 1000
	k := kafkatest.Start(t)
	topic := k.Topic(t, 6)
	recs := make([]*kgo.Record, n)
	for i := range recs {
		recs[i] = &kgo.Record{Topic: topic, Key: []byte("ACME"), Value: []byte(strconv.Itoa(i))}
	}
	if err := producer(t, k).Produce(t.Context(), recs...); err != nil {
		t.Fatal(err)
	}
	got := pollN(t, consumer(t, k, kafkatest.Name("g"), topic), n)
	for i, r := range got {
		if r.Partition != got[0].Partition {
			t.Fatalf("record %d on partition %d, the first on %d: one key must map to one partition", i, r.Partition, got[0].Partition)
		}
		if string(r.Value) != strconv.Itoa(i) {
			t.Fatalf("record %d is %s: out of order", i, r.Value)
		}
	}
}

// TestConsumer_RedeliversUncommitted: offsets move only on Commit (commit-after-write, ADR-011),
// so a new member of the group resumes after the last committed record.
func TestConsumer_RedeliversUncommitted(t *testing.T) {
	t.Parallel()
	k := kafkatest.Start(t)
	topic := k.Topic(t, 1)
	recs := make([]*kgo.Record, 10)
	for i := range recs {
		recs[i] = &kgo.Record{Topic: topic, Value: []byte(strconv.Itoa(i))}
	}
	if err := producer(t, k).Produce(t.Context(), recs...); err != nil {
		t.Fatal(err)
	}
	group := kafkatest.Name("g")
	first, err := kafka.NewConsumer(k.Config(), group, topic)
	if err != nil {
		t.Fatal(err)
	}
	got := pollN(t, first, 10)
	if err := first.Commit(t.Context(), got[:4]...); err != nil {
		t.Fatal(err)
	}
	first.Close()

	again := pollN(t, consumer(t, k, group, topic), 1)
	if string(again[0].Value) != "4" {
		t.Errorf("after committing records 0-3 the group resumed at %s, want 4", again[0].Value)
	}
}

func TestEnsureTopics_IdempotentRecreatesAndWarns(t *testing.T) {
	t.Parallel()
	k := kafkatest.Start(t)
	adm, err := kafka.NewAdmin(k.Config())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(adm.Close)
	a, b := kafkatest.Name("ensure"), kafkatest.Name("ensure")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = adm.DeleteTopics(ctx, a, b) // cleanup only; a leftover topic has a unique name
	})
	want := []kafka.Topic{
		{Name: a, Partitions: 3, ReplicationFactor: 1, Configs: map[string]string{"retention.ms": "604800000", "cleanup.policy": "delete"}},
		{Name: b, Partitions: 2, ReplicationFactor: 1, Configs: map[string]string{"cleanup.policy": "compact,delete"}},
	}
	ctx := t.Context()
	// The broker lists a topic only a moment after creating it; EnsureTopics then warns that it
	// could not compare it, so wait before each run that must compare.
	listed := func(names ...string) {
		testutil.WaitFor(t, func() bool {
			d, err := adm.ListTopics(ctx, names...)
			return err == nil && d.Error() == nil
		}, 30*time.Second)
	}

	res, err := kafka.EnsureTopics(ctx, adm, want)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Created) != 2 || len(res.Existing) != 0 || len(res.Warnings) != 0 {
		t.Fatalf("first run = %+v, want both created", res)
	}
	listed(a, b)
	res, err = kafka.EnsureTopics(ctx, adm, want)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Created) != 0 || len(res.Existing) != 2 || len(res.Warnings) != 0 {
		t.Fatalf("second run = %+v, want no changes", res)
	}

	if _, err := adm.DeleteTopics(ctx, a); err != nil {
		t.Fatal(err)
	}
	testutil.WaitFor(t, func() bool { // deletion is asynchronous
		d, err := adm.ListTopics(ctx, a)
		return err == nil && errors.Is(d[a].Err, kerr.UnknownTopicOrPartition)
	}, 30*time.Second)
	res, err = kafka.EnsureTopics(ctx, adm, want)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Created) != 1 || res.Created[0] != a {
		t.Fatalf("after deleting %s: %+v, want it recreated", a, res)
	}
	listed(a, b)

	want[1].Partitions = 5
	res, err = kafka.EnsureTopics(ctx, adm, want)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Created) != 0 || len(res.Warnings) != 1 || !strings.Contains(res.Warnings[0], "has 2 partitions, want 5") {
		t.Fatalf("partition drift: %+v, want one partition warning and no change", res)
	}
	d, err := adm.ListTopics(ctx, b)
	if err != nil {
		t.Fatal(err)
	}
	if n := len(d[b].Partitions); n != 2 {
		t.Errorf("%s has %d partitions after ensure, want 2 (never changed)", b, n)
	}
}

// TestConsumer_HoldsPartitionsUntilNextPoll: a batch is processed and committed before its
// partitions can move to another member, so a late Commit can never move the group's offset
// backwards over a newer owner's commits, and the batch is never processed by two members at once.
func TestConsumer_HoldsPartitionsUntilNextPoll(t *testing.T) {
	t.Parallel()
	k := kafkatest.Start(t)
	topic := k.Topic(t, 2) // two partitions, so the (cooperative-sticky) rebalance moves one to b
	recs := make([]*kgo.Record, 40)
	for i := range recs {
		recs[i] = &kgo.Record{Topic: topic, Key: []byte("k" + strconv.Itoa(i)), Value: []byte(strconv.Itoa(i))}
	}
	if err := producer(t, k).Produce(t.Context(), recs...); err != nil {
		t.Fatal(err)
	}
	if recs[0].Partition == recs[1].Partition && slices.IndexFunc(recs, func(r *kgo.Record) bool { return r.Partition != recs[0].Partition }) < 0 {
		t.Fatal("all records landed on one partition; the test needs both")
	}
	group := kafkatest.Name("g")
	a := consumer(t, k, group, topic)
	batch := pollN(t, a, len(recs)) // a now holds both partitions

	b := consumer(t, k, group, topic)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	got, err := b.Poll(ctx, 0) // b joins; the rebalance must wait for a
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("a second member got %d records of a batch the first still holds", len(got))
	}

	if err := a.Commit(t.Context(), batch...); err != nil {
		t.Fatal(err)
	}
	ctx, cancel = context.WithTimeout(t.Context(), 2*time.Second)
	_, _ = a.Poll(ctx, 0) // releases the batch; the rebalance can finish
	cancel()
	ctx, cancel = context.WithTimeout(t.Context(), 5*time.Second)
	got, err = b.Poll(ctx, 0)
	cancel()
	if err != nil || len(got) != 0 {
		t.Fatalf("after a committed everything, b got %d records (err %v), want 0", len(got), err)
	}
}
