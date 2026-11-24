package workerapi

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/daishakpatel/flowforge/internal/apperrors"
	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/coordination"
	"github.com/daishakpatel/flowforge/internal/coordination/fake"
	"github.com/daishakpatel/flowforge/internal/obs"
	"github.com/daishakpatel/flowforge/internal/persistence"
)

const raftTTL = 15 * time.Second

// memWorkers is an in-memory persistence.WorkerRepo with worker_observations' semantics.
type memWorkers struct {
	mu sync.Mutex
	w  map[string]persistence.WorkerView
}

func (m *memWorkers) Upsert(_ context.Context, _ persistence.Querier, r persistence.WorkerRegistration, now, liveAfter time.Time) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if cur, ok := m.w[r.WorkerID]; ok && cur.KeyID != r.KeyID && cur.Alive(liveAfter) {
		return false, nil
	}
	m.w[r.WorkerID] = persistence.WorkerView{WorkerID: r.WorkerID, Status: "ACTIVE", KeyID: r.KeyID, Capacity: r.Capacity,
		TaskTypes: r.TaskTypes, Queues: r.Queues, Version: r.Version, FirstSeenAt: now, LastSeenAt: now}
	return true, nil
}

func (m *memWorkers) Touch(_ context.Context, _ persistence.Querier, id, keyID string, active int, now time.Time) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, ok := m.w[id]
	if !ok || cur.KeyID != keyID || cur.Status != "ACTIVE" {
		return false, nil
	}
	cur.LastSeenAt, cur.ActiveTasks = now, active
	m.w[id] = cur
	return true, nil
}

func (m *memWorkers) Release(_ context.Context, _ persistence.Querier, id, keyID string, _ time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if cur, ok := m.w[id]; ok && cur.KeyID == keyID {
		cur.Status = "DEREGISTERED"
		m.w[id] = cur
	}
	return nil
}

func (m *memWorkers) Get(_ context.Context, _ persistence.Querier, id string) (persistence.WorkerView, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, ok := m.w[id]
	if !ok {
		return cur, apperrors.NotFound("no worker " + id)
	}
	return cur, nil
}

func (m *memWorkers) Expired(_ context.Context, _ persistence.Querier, before time.Time) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for id, w := range m.w {
		if w.Status == "ACTIVE" && w.LastSeenAt.Before(before) {
			out = append(out, id)
		}
	}
	slices.Sort(out)
	return out, nil
}

func (m *memWorkers) List(_ context.Context, _ persistence.Querier) ([]persistence.WorkerView, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []persistence.WorkerView
	for _, w := range m.w {
		out = append(out, w)
	}
	slices.SortFunc(out, func(a, b persistence.WorkerView) int { return cmpString(a.WorkerID, b.WorkerID) })
	return out, nil
}

func cmpString(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// raftEnv is a RaftLeaseProvider over a fake Raft KV (client "api") and in-memory observations,
// on one fake clock.
type raftEnv struct {
	clk     *clock.Fake
	store   *fake.Store
	workers *memWorkers
	metrics *coordination.RegistryMetrics
	p       *RaftLeaseProvider
}

func newRaftEnv(t *testing.T) *raftEnv {
	t.Helper()
	clk := clock.NewFake(time.Date(2026, 11, 22, 0, 0, 0, 0, time.UTC))
	store := fake.NewStore(clk)
	m, err := coordination.NewRegistryMetrics(obs.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	// As in production, the provider counts every Raft failure; the registry gets no metrics.
	reg, err := coordination.NewWorkerRegistry(store.Client("api"), coordination.RegistryConfig{TTL: raftTTL, Clock: clk})
	if err != nil {
		t.Fatal(err)
	}
	ws := &memWorkers{w: map[string]persistence.WorkerView{}}
	return &raftEnv{clk: clk, store: store, workers: ws, metrics: m, p: &RaftLeaseProvider{
		PG:       &PGLeaseProvider{Workers: ws, Clock: clk, TTL: raftTTL},
		Registry: reg, Metrics: m, Timeout: time.Second,
	}}
}

func (e *raftEnv) register(t *testing.T, id, key string) {
	t.Helper()
	if err := e.p.Register(t.Context(), persistence.WorkerRegistration{
		WorkerID: id, KeyID: key, TaskTypes: []string{"noop"}, Queues: []string{"default"}, Capacity: 2,
	}); err != nil {
		t.Fatalf("register %s: %v", id, err)
	}
}

// raftAlive lists the workers holding a live Raft lease, as an observer client sees them.
func (e *raftEnv) raftAlive(t *testing.T) []string {
	t.Helper()
	reg, err := coordination.NewWorkerRegistry(e.store.Client("observer"), coordination.RegistryConfig{TTL: raftTTL, Clock: e.clk})
	if err != nil {
		t.Fatal(err)
	}
	ws, err := reg.List(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, w := range ws {
		ids = append(ids, w.ID)
	}
	return ids
}

func TestRaftLease_RegisterRenewReleaseKeepRaftAndPostgresInStep(t *testing.T) {
	e := newRaftEnv(t)
	e.register(t, "w1", "key-a")
	if got := e.raftAlive(t); !slices.Equal(got, []string{"w1"}) {
		t.Fatalf("raft workers after Register = %v", got)
	}
	// Another key may not take a live worker id: Raft is the authority on that (CONFLICT).
	err := e.p.Register(t.Context(), persistence.WorkerRegistration{WorkerID: "w1", KeyID: "key-b", TaskTypes: []string{"x"}, Queues: []string{"q"}, Capacity: 1})
	if apperrors.CodeOf(err) != apperrors.CodeConflict {
		t.Fatalf("register under another key: %v, want CONFLICT", err)
	}
	// Renewals keep the lease past its TTL.
	for range 4 {
		e.clk.Advance(raftTTL / 2)
		if err := e.p.Renew(t.Context(), "w1", "key-a", 1); err != nil {
			t.Fatalf("renew: %v", err)
		}
	}
	if got := e.raftAlive(t); !slices.Equal(got, []string{"w1"}) {
		t.Fatalf("raft workers after renewals = %v", got)
	}
	// An unregistered worker is still INVALID_STATE (from Postgres).
	if err := e.p.Renew(t.Context(), "nobody", "key-a", 0); apperrors.CodeOf(err) != apperrors.CodeInvalidState {
		t.Fatalf("renew unknown worker: %v", err)
	}
	if err := e.p.Release(t.Context(), "w1", "key-a"); err != nil {
		t.Fatal(err)
	}
	if got := e.raftAlive(t); len(got) != 0 {
		t.Fatalf("raft workers after Release = %v", got)
	}
	if w, _ := e.workers.Get(t.Context(), nil, "w1"); w.Status != "DEREGISTERED" {
		t.Fatalf("postgres status after Release = %s", w.Status)
	}
}

// A Raft outage must not fail heartbeats: they are recorded in Postgres, counted as renewal
// failures, and the Raft lease comes back on the first heartbeat after the outage, even when it
// expired meanwhile (00_MASTER §6.2).
func TestRaftLease_HeartbeatsDegradeThroughARaftOutage(t *testing.T) {
	e := newRaftEnv(t)
	e.register(t, "w1", "key-a")
	e.store.Partition("api", true)
	for range 4 { // 20 s: the Raft lease expires
		e.clk.Advance(5 * time.Second)
		if err := e.p.Renew(t.Context(), "w1", "key-a", 1); err != nil {
			t.Fatalf("renew during the outage: %v", err)
		}
	}
	if n := testutil.ToFloat64(e.metrics.RenewalFailures); n < 4 {
		t.Fatalf("renewal failures counted = %v, want ≥ 4", n)
	}
	if w, _ := e.workers.Get(t.Context(), nil, "w1"); !w.LastSeenAt.Equal(e.clk.Now()) {
		t.Fatalf("postgres last_seen %v, want %v", w.LastSeenAt, e.clk.Now())
	}
	if got := e.raftAlive(t); len(got) != 0 {
		t.Fatalf("raft lease should have expired during the outage: %v", got)
	}
	e.store.Partition("api", false)
	if err := e.p.Renew(t.Context(), "w1", "key-a", 1); err != nil {
		t.Fatalf("renew after the outage: %v", err)
	}
	if got := e.raftAlive(t); !slices.Equal(got, []string{"w1"}) {
		t.Fatalf("raft workers after the outage = %v", got)
	}
}

// A worker that registered while Raft was down gets its Raft lease on its next heartbeat.
func TestRaftLease_RegisterDuringAnOutageCompletesOnTheNextHeartbeat(t *testing.T) {
	e := newRaftEnv(t)
	e.store.Partition("api", true)
	e.register(t, "w1", "key-a")
	e.store.Partition("api", false)
	if got := e.raftAlive(t); len(got) != 0 {
		t.Fatalf("raft workers = %v, want none yet", got)
	}
	if err := e.p.Renew(t.Context(), "w1", "key-a", 0); err != nil {
		t.Fatal(err)
	}
	if got := e.raftAlive(t); !slices.Equal(got, []string{"w1"}) {
		t.Fatalf("raft workers after the heartbeat = %v", got)
	}
}

// Expired reports a worker only when its Raft lease is gone and Postgres has not seen it within
// the TTL: a worker that kept heartbeating through a Raft outage is never reported, even before
// its first heartbeat after the outage restores its lease.
func TestRaftLease_ExpiredNeedsRaftAndPostgresToAgree(t *testing.T) {
	e := newRaftEnv(t)
	e.register(t, "dead", "key-a")
	e.register(t, "healthy", "key-a")
	e.store.Partition("api", true)
	for range 4 { // 20 s outage: both Raft leases expire; only "healthy" heartbeats
		e.clk.Advance(5 * time.Second)
		if err := e.p.Renew(t.Context(), "healthy", "key-a", 0); err != nil {
			t.Fatal(err)
		}
	}
	e.store.Partition("api", false)
	got, err := e.p.Expired(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, []string{"dead"}) {
		t.Fatalf("Expired = %v, want [dead]", got)
	}
	alive, err := e.p.Alive(t.Context())
	if err != nil || len(alive) != 0 {
		t.Fatalf("Alive = %v, %v; want none before the next heartbeat", alive, err)
	}
	if err := e.p.Renew(t.Context(), "healthy", "key-a", 0); err != nil {
		t.Fatal(err)
	}
	if alive, err = e.p.Alive(t.Context()); err != nil || len(alive) != 1 || alive[0].WorkerID != "healthy" {
		t.Fatalf("Alive = %v, %v; want [healthy]", alive, err)
	}
}

// When Raft cannot be read, Expired fails and recovery abandons nothing (fail safe), even for a
// worker Postgres has not seen for a long time.
func TestRaftLease_ExpiredFailsWhenRaftIsUnreadable(t *testing.T) {
	e := newRaftEnv(t)
	e.register(t, "w1", "key-a")
	e.clk.Advance(time.Minute)
	e.store.Partition("api", true)
	if got, err := e.p.Expired(t.Context()); err == nil {
		t.Fatalf("Expired = %v with Raft unreadable; want an error", got)
	}
	if _, err := e.p.Alive(t.Context()); err == nil {
		t.Fatal("Alive with Raft unreadable: want an error")
	}
}

// A heartbeat that passed its Postgres check just before a Deregister must not bring the
// worker's Raft lease back, and a renewal never extends a lease another API key holds (code
// review, 04.07).
func TestRaftLease_RenewNeverRevivesOrTakesALease(t *testing.T) {
	e := newRaftEnv(t)
	e.register(t, "w1", "key-a")
	if err := e.p.Release(t.Context(), "w1", "key-a"); err != nil {
		t.Fatal(err)
	}
	if err := e.p.renewRaft(t.Context(), "w1", "key-a"); !errors.Is(err, coordination.ErrNotRegistered) {
		t.Fatalf("raft step after a Deregister = %v, want ErrNotRegistered", err)
	}
	if got := e.raftAlive(t); len(got) != 0 {
		t.Fatalf("a heartbeat racing Deregister revived %v", got)
	}

	// Postgres holds w2 for key-a, while Raft holds a live lease of key-b's.
	e.register(t, "w2", "key-a")
	h, err := e.p.Registry.Handle(t.Context(), "w2")
	if err == nil {
		err = e.p.Registry.Deregister(t.Context(), h)
	}
	if err != nil {
		t.Fatal(err)
	}
	other, err := coordination.NewWorkerRegistry(e.store.Client("api-2"), coordination.RegistryConfig{TTL: raftTTL, Clock: e.clk})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.Register(t.Context(), coordination.WorkerInfo{ID: "w2", KeyID: "key-b"}); err != nil {
		t.Fatal(err)
	}
	if err := e.p.renewRaft(t.Context(), "w2", "key-a"); !errors.Is(err, coordination.ErrWorkerConflict) {
		t.Fatalf("renewing key-b's lease for key-a = %v, want ErrWorkerConflict", err)
	}
}
