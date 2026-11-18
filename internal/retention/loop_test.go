package retention

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	promtest "github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/obs"
	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/scheduler"
)

// memPurge has rows per target; each Purge call removes up to limit of them and logs the call.
type memPurge struct {
	mu     sync.Mutex
	rows   map[persistence.RetentionTarget]int64
	calls  []string
	before map[persistence.RetentionTarget]time.Time
	onCall func()
}

func (m *memPurge) Purge(_ context.Context, _ persistence.Querier, t persistence.RetentionTarget, before time.Time, limit int) (int64, error) {
	m.mu.Lock()
	n := min(m.rows[t], int64(limit))
	m.rows[t] -= n
	m.calls = append(m.calls, string(t))
	m.before[t] = before
	f := m.onCall
	m.mu.Unlock()
	if f != nil {
		f()
	}
	return n, nil
}

var now = time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)

func newLoop(m *memPurge) (*Loop, *clock.Fake) {
	c := clock.NewFake(now)
	return &Loop{Repo: m, Clock: c, Tick: time.Hour, Policy: DefaultPolicy(), Batch: 5000, Pause: 50 * time.Millisecond}, c
}

// TestLoop_DeletesInBoundedBatches: 12 000 old events go in three batches of at most 5 000, with a
// pause between batches; every target is purged with its own age.
func TestLoop_DeletesInBoundedBatches(t *testing.T) {
	t.Parallel()
	m := &memPurge{rows: map[persistence.RetentionTarget]int64{persistence.RetainEvents: 12000, persistence.RetainOutbox: 3}, before: map[persistence.RetentionTarget]time.Time{}}
	l, c := newLoop(m)
	deleted, err := NewDeletedRows(obs.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	l.Deleted = deleted
	done := make(chan error, 1)
	go func() { done <- l.Once(t.Context(), scheduler.AlwaysLeader{}) }()
	for range 2 { // the two pauses between the three event batches
		c.BlockUntilWaiters(1)
		c.Advance(l.Pause)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	want := []string{"outbox", "workflow_events", "workflow_events", "workflow_events", "task_logs", "idempotency_keys"}
	if !slices.Equal(m.calls, want) {
		t.Errorf("calls %v; want %v", m.calls, want)
	}
	p := DefaultPolicy()
	for target, age := range map[persistence.RetentionTarget]time.Duration{
		persistence.RetainOutbox: p.Outbox, persistence.RetainEvents: p.Events, persistence.RetainTaskLogs: p.TaskLogs,
		persistence.RetainIdempotencyKeys: p.IdempotencyKeys,
	} {
		if got := m.before[target]; !got.Equal(now.Add(-age)) {
			t.Errorf("%s purged before %s; want %s", target, got, now.Add(-age))
		}
	}
	if got := promtest.ToFloat64(deleted.WithLabelValues("workflow_events")); got != 12000 {
		t.Errorf("deleted rows metric = %v; want 12000", got)
	}
}

func TestLoop_LeaderGated(t *testing.T) {
	t.Parallel()
	m := &memPurge{rows: map[persistence.RetentionTarget]int64{persistence.RetainEvents: 12000}, before: map[persistence.RetentionTarget]time.Time{}}
	l, _ := newLoop(m)
	if err := l.Once(t.Context(), notLeader{}); err != nil || len(m.calls) != 0 {
		t.Fatalf("not leader: err %v, calls %v; want nothing", err, m.calls)
	}

	// Leadership ending mid-pass stops it after the current batch.
	g := &flipGate{leader: true}
	m.onCall = func() { g.set(false) }
	if err := l.Once(t.Context(), g); err != nil || len(m.calls) != 1 {
		t.Errorf("leadership lost after one batch: err %v, calls %v; want one call", err, m.calls)
	}
}

func TestDefaultPolicy(t *testing.T) {
	t.Parallel()
	p := DefaultPolicy()
	if p.Outbox != 72*time.Hour || p.Events != 30*24*time.Hour || p.TaskLogs != 14*24*time.Hour || p.IdempotencyKeys != 24*time.Hour {
		t.Errorf("DefaultPolicy = %+v; want 3 d, 30 d, 14 d, 24 h (task 03.07 §4)", p)
	}
}

type notLeader struct{ scheduler.AlwaysLeader }

func (notLeader) IsLeader() bool { return false }

type flipGate struct {
	scheduler.AlwaysLeader
	mu     sync.Mutex
	leader bool
}

func (g *flipGate) IsLeader() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.leader
}

func (g *flipGate) set(v bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.leader = v
}
