package producer_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/shopspring/decimal"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/kafka"
	"github.com/daishakpatel/flowforge/internal/obs"
	"github.com/daishakpatel/flowforge/internal/stream/domain"
	"github.com/daishakpatel/flowforge/internal/stream/producer"
	"github.com/daishakpatel/flowforge/internal/stream/source"
	"github.com/daishakpatel/flowforge/pkg/events"
)

var now = time.Date(2026, 11, 2, 14, 30, 0, 0, time.UTC)

// fakePublisher records what it is given; failWith makes Produce fail.
type fakePublisher struct {
	mu       sync.Mutex
	recs     []*kgo.Record
	failWith error
}

func (f *fakePublisher) Produce(_ context.Context, recs ...*kgo.Record) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failWith != nil {
		return f.failWith
	}
	f.recs = append(f.recs, recs...)
	return nil
}

func (f *fakePublisher) byTopic(topic string) []*kgo.Record {
	var out []*kgo.Record
	for _, r := range f.recs {
		if r.Topic == topic {
			out = append(out, r)
		}
	}
	return out
}

type harness struct {
	pub *fakePublisher
	p   *producer.Producer
	m   *producer.Metrics
	reg *events.Registry
}

func newHarness(t *testing.T, opts producer.Options) harness {
	t.Helper()
	reg, err := events.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	m, err := producer.NewMetrics(obs.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	pub := &fakePublisher{}
	return harness{pub: pub, p: producer.New(pub, reg, clock.NewFake(now), m, opts), m: m, reg: reg}
}

func tick(symbol, price, size string, seq int64) domain.Tick {
	at := now.Add(-time.Duration(100-seq) * time.Millisecond)
	t := domain.Tick{Symbol: symbol, Price: decimal.RequireFromString(price), Size: decimal.RequireFromString(size), EventTime: at, Source: "synthetic", Seq: seq}
	t.EventID = source.EventID(t.Source, t.Symbol, t.Seq, t.EventTime)
	return t
}

func TestPublish_ValidTicksGoToTicksKeyedBySymbol(t *testing.T) {
	t.Parallel()
	h := newHarness(t, producer.Options{})
	ticks := []domain.Tick{tick("ACME", "101.25", "300", 1), tick("BOLT", "48.10", "50", 1)}
	res, err := h.p.Publish(t.Context(), ticks)
	if err != nil {
		t.Fatal(err)
	}
	if res.Published != 2 || res.Rejected != 0 || len(h.pub.recs) != 2 {
		t.Fatalf("result %+v, %d records", res, len(h.pub.recs))
	}
	for i, r := range h.pub.recs {
		if r.Topic != producer.TopicTicks || string(r.Key) != ticks[i].Symbol {
			t.Errorf("record %d: topic %s key %s", i, r.Topic, r.Key)
		}
		if id, _ := kafka.Header(r, producer.HeaderEventID); id != ticks[i].EventID {
			t.Errorf("record %d: event_id header %q", i, id)
		}
		if err := h.reg.Validate("envelope.v1", r.Value); err != nil {
			t.Errorf("record %d: %v", i, err)
		}
		env, err := events.Unmarshal[map[string]any](r.Value)
		if err != nil {
			t.Fatal(err)
		}
		if env.EventType != "market.tick" || env.EventID != ticks[i].EventID || env.AggregateID != ticks[i].Symbol || env.Producer != "flowforge-producer" {
			t.Errorf("record %d envelope %+v", i, env)
		}
		if !strings.Contains(string(r.Value), `"price":101.25`) && i == 0 {
			t.Errorf("price is not a JSON number: %s", r.Value)
		}
	}
	if got := testutil.ToFloat64(h.m.Events.WithLabelValues("published")); got != 2 {
		t.Errorf("published metric %v", got)
	}
}

func TestPublish_InvalidTicksGoToTheDLQWithAReason(t *testing.T) {
	t.Parallel()
	h := newHarness(t, producer.Options{})
	bad := tick("ACME", "-1", "300", 2)   // domain: nonpositive_price
	zero := tick("BOLT", "48.10", "0", 3) // domain-valid, but market_tick.v1 requires size > 0
	res, err := h.p.Publish(t.Context(), []domain.Tick{tick("ACME", "101", "1", 1), bad, zero})
	if err != nil {
		t.Fatal(err)
	}
	if res.Published != 1 || res.Rejected != 2 {
		t.Fatalf("result %+v", res)
	}
	if n := len(h.pub.byTopic(producer.TopicTicks)); n != 1 {
		t.Errorf("%d records on the ticks topic, want only the valid one", n)
	}
	dlq := h.pub.byTopic(producer.TopicInvalid)
	if len(dlq) != 2 {
		t.Fatalf("%d DLQ records, want 2", len(dlq))
	}
	for i, want := range []struct {
		tick   domain.Tick
		reason string
	}{{bad, "nonpositive_price"}, {zero, "schema_invalid"}} {
		r := dlq[i]
		if string(r.Key) != want.tick.EventID {
			t.Errorf("DLQ %d key %q, want the event id", i, r.Key)
		}
		if got, _ := kafka.Header(r, producer.HeaderReason); got != want.reason {
			t.Errorf("DLQ %d reason header %q, want %s", i, got, want.reason)
		}
		if err := h.reg.Validate("envelope.v1", r.Value); err != nil {
			t.Fatalf("DLQ %d does not match market_invalid.v1: %v", i, err)
		}
		env, err := events.Unmarshal[struct {
			Reason, Detail, Original string
			SourceTopic              *string `json:"source_topic"`
		}](r.Value)
		if err != nil {
			t.Fatal(err)
		}
		if env.EventType != "market.invalid" || env.Payload.Reason != want.reason || env.Payload.SourceTopic != nil {
			t.Errorf("DLQ %d envelope %+v (a producer reject has no source topic)", i, env)
		}
		orig, err := base64.StdEncoding.DecodeString(env.Payload.Original)
		var payload map[string]any
		if err != nil || json.Unmarshal(orig, &payload) != nil || payload["event_id"] != want.tick.EventID {
			t.Errorf("DLQ %d original %q does not decode to the tick (%v)", i, orig, err)
		}
	}
	if !strings.Contains(string(dlq[1].Value), "size") {
		t.Errorf("schema_invalid detail does not name the field: %s", dlq[1].Value)
	}
	if got := testutil.ToFloat64(h.m.Events.WithLabelValues("invalid")); got != 2 {
		t.Errorf("invalid metric %v", got)
	}
}

// TestPublish_OverlongIDsStillReachTheDLQ: an id or symbol longer than the envelope allows is
// cut in the DLQ envelope (the full tick stays in original), so one bad row never stops a run.
func TestPublish_OverlongIDsStillReachTheDLQ(t *testing.T) {
	t.Parallel()
	h := newHarness(t, producer.Options{})
	long := tick(strings.Repeat("A", 200), "1", "1", 1)
	long.EventID = strings.Repeat("e", 200)
	res, err := h.p.Publish(t.Context(), []domain.Tick{long})
	if err != nil || res.Rejected != 1 {
		t.Fatalf("overlong tick: %+v, %v; want it rejected to the DLQ", res, err)
	}
	if err := h.reg.Validate("envelope.v1", h.pub.byTopic(producer.TopicInvalid)[0].Value); err != nil {
		t.Errorf("DLQ record invalid: %v", err)
	}
}

func TestPublish_AllowInvalidSendsThemToTicks(t *testing.T) {
	t.Parallel()
	h := newHarness(t, producer.Options{AllowInvalid: true})
	res, err := h.p.Publish(t.Context(), []domain.Tick{tick("acme", "101", "1", 1)})
	if err != nil {
		t.Fatal(err)
	}
	if res.ForcedInvalid != 1 || len(h.pub.byTopic(producer.TopicTicks)) != 1 || len(h.pub.byTopic(producer.TopicInvalid)) != 0 {
		t.Errorf("result %+v; want the invalid tick on the ticks topic", res)
	}
	if got := testutil.ToFloat64(h.m.Events.WithLabelValues("forced_invalid")); got != 1 {
		t.Errorf("forced_invalid metric %v", got)
	}
}

func TestPublish_ErrorIsReturnedAndCounted(t *testing.T) {
	t.Parallel()
	h := newHarness(t, producer.Options{})
	h.pub.failWith = errors.New("broker down")
	if _, err := h.p.Publish(t.Context(), []domain.Tick{tick("ACME", "1", "1", 1), tick("ACME", "1", "1", 2)}); err == nil {
		t.Fatal("a failed produce was not reported")
	}
	if got := testutil.ToFloat64(h.m.Events.WithLabelValues("error")); got != 2 {
		t.Errorf("error metric %v, want 2", got)
	}
	if res, err := h.p.Publish(t.Context(), nil); err != nil || res != (producer.Result{}) {
		t.Errorf("empty batch: %+v, %v", res, err)
	}
}

// sliceSource returns its items, then io.EOF; an error item is returned as an error.
type sliceSource struct{ items []any }

func (s *sliceSource) Next(ctx context.Context) (domain.Tick, error) {
	if err := ctx.Err(); err != nil {
		return domain.Tick{}, err
	}
	if len(s.items) == 0 {
		return domain.Tick{}, io.EOF
	}
	it := s.items[0]
	s.items = s.items[1:]
	if err, ok := it.(error); ok {
		return domain.Tick{}, err
	}
	return it.(domain.Tick), nil
}

func TestRun_BatchesCountsAndBadRows(t *testing.T) {
	t.Parallel()
	h := newHarness(t, producer.Options{})
	var items []any
	for i := range 25 {
		items = append(items, tick("ACME", "10", "1", int64(i+1)))
	}
	items = append(items[:3:3], append([]any{&source.ParseError{Line: 4, Err: errors.New("bad price")}}, items[3:]...)...)
	items = append(items, tick("ACME", "-1", "1", 99))

	sum, err := producer.Run(t.Context(), &sliceSource{items: items}, h.p, producer.RunOptions{Batch: 10})
	if err != nil {
		t.Fatal(err)
	}
	if sum.Read != 26 || sum.Published != 25 || sum.Rejected != 1 || sum.BadRows != 1 {
		t.Errorf("summary %+v; want 26 read, 25 published, 1 rejected, 1 bad row", sum)
	}

	h2 := newHarness(t, producer.Options{})
	var many []any
	for i := range 50 {
		many = append(many, tick("BOLT", "10", "1", int64(i+1)))
	}
	sum, err = producer.Run(t.Context(), &sliceSource{items: many}, h2.p, producer.RunOptions{Count: 12, Batch: 5})
	if err != nil || sum.Read != 12 || len(h2.pub.recs) != 12 {
		t.Errorf("count 12: %+v, %d records, %v", sum, len(h2.pub.recs), err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := producer.Run(ctx, &sliceSource{items: many}, h2.p, producer.RunOptions{}); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled run: %v", err)
	}

	h3 := newHarness(t, producer.Options{})
	h3.pub.failWith = errors.New("broker down")
	if _, err := producer.Run(t.Context(), &sliceSource{items: many[:3]}, h3.p, producer.RunOptions{}); err == nil {
		t.Error("a failed publish did not stop the run")
	}
}
