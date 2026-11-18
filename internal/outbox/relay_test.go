package outbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/daishakpatel/flowforge/internal/apperrors"
	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/obs"
	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/scheduler"
)

// memOutbox is an in-memory outbox table.
type memOutbox struct {
	mu        sync.Mutex
	rows      []persistence.OutboxView
	published map[int64]time.Time
}

func newMemOutbox(n, keys int) *memOutbox {
	s := &memOutbox{published: map[int64]time.Time{}}
	for i := range n {
		id := int64(i + 1)
		s.rows = append(s.rows, persistence.OutboxView{
			ID: id, EventID: fmt.Sprintf("evt_%04d", id), Topic: "flowforge.task-events",
			Key: fmt.Sprintf("run_%02d", i%keys), Envelope: []byte(`{}`),
		})
	}
	return s
}

func (s *memOutbox) LockUnpublished(_ context.Context, _ pgx.Tx, limit int) ([]persistence.OutboxView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []persistence.OutboxView
	for _, r := range s.rows {
		if _, done := s.published[r.ID]; !done && len(out) < limit {
			out = append(out, r)
		}
	}
	return out, nil
}

func (s *memOutbox) MarkPublished(_ context.Context, _ pgx.Tx, ids []int64, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range ids {
		if _, done := s.published[id]; !done {
			s.published[id] = at
		}
	}
	return nil
}

func (s *memOutbox) Backlog(context.Context, persistence.Querier) (int64, *time.Time, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int64(len(s.rows) - len(s.published)), nil, nil
}

func (s *memOutbox) marked() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []int64
	for id := range s.published {
		out = append(out, id)
	}
	slices.Sort(out)
	return out
}

type memTx struct{}

func (memTx) WithTx(_ context.Context, _ pgx.TxOptions, fn func(pgx.Tx) error) error { return fn(nil) }

// fence is an epoch fence at epoch 0 that can be made stale.
type fence struct {
	persistence.ClusterRepo
	stale bool
}

func (f *fence) AssertEpoch(context.Context, pgx.Tx, int64) error {
	if f.stale {
		return &apperrors.Error{Code: apperrors.CodeNotLeader, Message: "stale epoch", Cause: persistence.ErrStaleEpoch}
	}
	return nil
}

// notLeader is a Gate that never leads.
type notLeader struct{ scheduler.AlwaysLeader }

func (notLeader) IsLeader() bool { return false }

func newRelay(s *memOutbox, p Publisher, batch int) (*RelayLoop, *fence) {
	f := &fence{}
	return &RelayLoop{
		Tx: memTx{}, Cluster: f, Outbox: s, Publisher: p,
		Clock: clock.NewFake(time.Date(2026, 11, 17, 0, 0, 0, 0, time.UTC)), Tick: time.Second, Batch: batch,
	}, f
}

// prefixPublisher delivers the first n messages of its first call, then reports PartialError.
type prefixPublisher struct {
	MemoryPublisher
	n     int
	calls int
}

func (p *prefixPublisher) Publish(ctx context.Context, msgs []Message) error {
	p.calls++
	if p.calls > 1 {
		return p.MemoryPublisher.Publish(ctx, msgs)
	}
	_ = p.MemoryPublisher.Publish(ctx, msgs[:p.n]) // MemoryPublisher fails only on a done ctx
	return &PartialError{Published: p.n, Err: errors.New("broker went away")}
}

// TestRelay_MarksOnlyThePublishedPrefix: a batch that fails after 3 of 5 messages marks exactly
// those 3; the next pass starts from the 4th, so nothing is skipped or reordered.
func TestRelay_MarksOnlyThePublishedPrefix(t *testing.T) {
	t.Parallel()
	s := newMemOutbox(5, 1)
	p := &prefixPublisher{n: 3}
	r, _ := newRelay(s, p, 500)
	if err := r.Once(t.Context(), scheduler.AlwaysLeader{}); err == nil {
		t.Fatal("a failed publish must be reported so the loop backs off")
	}
	if got := s.marked(); !slices.Equal(got, []int64{1, 2, 3}) {
		t.Fatalf("marked %v after a partial failure; want the published prefix [1 2 3]", got)
	}
	if err := r.Once(t.Context(), scheduler.AlwaysLeader{}); err != nil {
		t.Fatal(err)
	}
	if got := eventIDs(p.Messages()); !slices.Equal(got, []string{"evt_0001", "evt_0002", "evt_0003", "evt_0004", "evt_0005"}) {
		t.Errorf("published %v; want each once, in order", got)
	}
}

// flakyPublisher fails every 7th call. Odd failures deliver a random prefix and report it
// (PartialError); even ones deliver a random prefix and then lose the acknowledgement (a plain
// error), so those messages are published again: at-least-once.
type flakyPublisher struct {
	MemoryPublisher
	rng            *rand.Rand
	calls, failure int
}

func (p *flakyPublisher) Publish(ctx context.Context, msgs []Message) error {
	p.calls++
	if p.calls%7 != 0 {
		return p.MemoryPublisher.Publish(ctx, msgs)
	}
	p.failure++
	n := p.rng.IntN(len(msgs))
	_ = p.MemoryPublisher.Publish(ctx, msgs[:n])
	if p.failure%2 == 1 {
		return &PartialError{Published: n, Err: errors.New("broker went away")}
	}
	return errors.New("acknowledgement lost")
}

const flakySeed = 20261117

// TestRelay_FlakyPublisherDeliversEveryEventInOrder is task 03.07 §8's relay test without a
// database: 1 000 events over 10 runs, a publisher failing every 7th batch. Every event is
// published at least once, deduping on event_id leaves each run's events in id order, and
// published_at is only ever set on what the publisher acknowledged.
func TestRelay_FlakyPublisherDeliversEveryEventInOrder(t *testing.T) {
	t.Parallel()
	s := newMemOutbox(1000, 10)
	p := &flakyPublisher{rng: rand.New(rand.NewPCG(flakySeed, 0))} //nolint:gosec // G404: seeded test randomness
	r, _ := newRelay(s, p, 50)
	for pass := 0; ; pass++ {
		if pass > 200 {
			t.Fatalf("seed %d: backlog never drained", flakySeed)
		}
		_ = r.Once(t.Context(), scheduler.AlwaysLeader{}) // failures are retried by the next pass
		if n, _, _ := s.Backlog(t.Context(), nil); n == 0 {
			break
		}
	}
	msgs := p.Messages()
	if p.failure == 0 || len(msgs) <= 1000 {
		t.Fatalf("seed %d: %d failures, %d messages; the test must inject failures and duplicates", flakySeed, p.failure, len(msgs))
	}
	checkOrderAndDedupe(t, msgs, 1000)
	if len(s.marked()) != 1000 {
		t.Errorf("%d rows marked; want 1000", len(s.marked()))
	}
	t.Logf("seed %d: %d failures, %d messages for 1000 events", flakySeed, p.failure, len(msgs))
}

// checkOrderAndDedupe asserts that the first delivery of each event_id covers every one of want
// events and that, per key, first deliveries come in event order.
func checkOrderAndDedupe(t *testing.T, msgs []Message, want int) {
	t.Helper()
	seen := map[string]bool{}
	last := map[string]string{}
	for _, m := range msgs {
		id := m.Headers[HeaderEventID]
		if seen[id] {
			continue
		}
		seen[id] = true
		if prev := last[m.Key]; prev != "" && id < prev {
			t.Errorf("key %s: %s delivered after %s", m.Key, id, prev)
		}
		last[m.Key] = id
	}
	if len(seen) != want {
		t.Errorf("%d distinct events delivered; want %d", len(seen), want)
	}
}

func TestRelay_DoesNothingWhenNotLeaderOrStale(t *testing.T) {
	t.Parallel()
	s := newMemOutbox(3, 1)
	p := &MemoryPublisher{}
	r, f := newRelay(s, p, 500)
	if err := r.Once(t.Context(), notLeader{}); err != nil || len(p.Messages()) != 0 || len(s.marked()) != 0 {
		t.Fatalf("not leader: err %v, published %d, marked %d; want nothing", err, len(p.Messages()), len(s.marked()))
	}
	f.stale = true
	if err := r.Once(t.Context(), scheduler.AlwaysLeader{}); !errors.Is(err, persistence.ErrStaleEpoch) || len(p.Messages()) != 0 {
		t.Fatalf("stale epoch: err %v, published %d; want ErrStaleEpoch and nothing published", err, len(p.Messages()))
	}
}

func eventIDs(msgs []Message) []string {
	out := make([]string, len(msgs))
	for i, m := range msgs {
		out[i] = m.Headers[HeaderEventID]
	}
	return out
}

// TestRelay_MetricsAndPublishers: the relay keeps its metrics, Delivered clamps what a publisher
// reports, and the log and discard publishers accept everything.
func TestRelay_MetricsAndPublishers(t *testing.T) {
	t.Parallel()
	m, err := NewMetrics(obs.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	s := newMemOutbox(5, 1)
	r, _ := newRelay(s, &prefixPublisher{n: 2}, 500)
	r.Metrics = m
	_ = r.Once(t.Context(), scheduler.AlwaysLeader{}) // fails after 2
	if got := promtest.ToFloat64(m.Failures); got != 1 {
		t.Errorf("failures %v; want 1", got)
	}
	if got := promtest.ToFloat64(m.Backlog); got != 3 {
		t.Errorf("backlog %v; want 3", got)
	}
	if err := r.Once(t.Context(), scheduler.AlwaysLeader{}); err != nil || promtest.ToFloat64(m.Backlog) != 0 {
		t.Errorf("drained: err %v, backlog %v", err, promtest.ToFloat64(m.Backlog))
	}
	for _, c := range []struct {
		err  error
		want int
	}{{nil, 5}, {&PartialError{Published: 9}, 5}, {&PartialError{Published: -1}, 0}, {errors.New("x"), 0}} {
		if got := Delivered(c.err, 5); got != c.want {
			t.Errorf("Delivered(%v, 5) = %d; want %d", c.err, got, c.want)
		}
	}
	msgs := []Message{{Topic: "t", Key: "k", Headers: map[string]string{HeaderEventID: "evt_1"}}}
	if err := (LogPublisher{Logger: slog.New(slog.DiscardHandler)}).Publish(t.Context(), msgs); err != nil {
		t.Error(err)
	}
	if err := (DiscardPublisher{}).Publish(t.Context(), msgs); err != nil {
		t.Error(err)
	}
	if !strings.Contains((&PartialError{Published: 2, Err: errors.New("down")}).Error(), "published 2") {
		t.Error("PartialError message")
	}
}
