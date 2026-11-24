//go:build integration

package scheduler

import (
	"context"
	crand "crypto/rand"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/coordination"
	"github.com/daishakpatel/flowforge/internal/coordination/fake"
	"github.com/daishakpatel/flowforge/internal/events"
	"github.com/daishakpatel/flowforge/internal/ids"
	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/persistence/pg"
	"github.com/daishakpatel/flowforge/internal/testutil"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
)

// believing is the Gate of a leader that has not noticed it was deposed: a paused process that
// wakes up with the term it had.
type believing struct{ epoch int64 }

func (b believing) IsLeader() bool                 { return true }
func (b believing) Epoch() int64                   { return b.epoch }
func (b believing) Done() <-chan struct{}          { return nil }
func (b believing) Wait(ctx context.Context) error { return ctx.Err() }

// TestFencing_DeposedLeaderCannotCommit is the two-layer fence (task 04.07, FR-RAFT-08,
// ADR-003) with an injected pause: scheduler A leads through the Raft-backed Elector, then
// pauses (partitioned from the KV, its next decision held back); B takes over and PGFence
// activates B's epoch in Postgres. When A wakes up still believing it leads, its decision fails
// with STALE_EPOCH and changes nothing; B's commits.
func TestFencing_DeposedLeaderCannotCommit(t *testing.T) {
	e := newEnv(t)
	run, _ := e.createRun(t, "", tasksJSON("a"))
	rec := events.NewRecorder(clock.Real{}, ids.NewGenerator(clock.Real{}, crand.Reader), "flowforge-scheduler")
	fence := coordination.PGFence{Tx: e.tm, Cluster: pg.ClusterRepo{}, Events: rec}
	store := fake.NewStore(clock.Real{})
	ctx, cancel := context.WithCancel(t.Context())
	var stopped []chan struct{}
	t.Cleanup(func() {
		cancel()
		for _, c := range stopped {
			<-c
		}
	})
	elect := func(name string) *coordination.Elector {
		el, err := coordination.NewElector(coordination.ElectorConfig{NodeID: name, TTL: 900 * time.Millisecond, Renew: 150 * time.Millisecond},
			store.Client(name), fence)
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan struct{})
		stopped = append(stopped, done)
		go func() { defer close(done); _ = el.Run(ctx) }()
		testutil.WaitFor(t, el.Gate().IsLeader, 10*time.Second)
		return el
	}

	a := elect("scheduler-a")
	epochA := a.Gate().Epoch()
	store.Partition("scheduler-a", true) // A pauses: no renewals, its decision held back
	b := elect("scheduler-b")
	epochB := b.Gate().Epoch()
	if epochB <= epochA {
		t.Fatalf("epochs: A %d, B %d; want B's above", epochA, epochB)
	}

	err := e.loop.Once(t.Context(), believing{epoch: epochA})
	if !errors.Is(err, persistence.ErrStaleEpoch) {
		t.Fatalf("A's decision after the takeover: %v, want STALE_EPOCH", err)
	}
	if st := e.statuses(t, run)["a"]; st != model.TaskPending {
		t.Fatalf("A's rejected decision changed the task to %s", st)
	}
	if err := e.loop.Once(t.Context(), b.Gate()); err != nil {
		t.Fatalf("B's decision: %v", err)
	}
	if st := e.statuses(t, run)["a"]; st != model.TaskQueued {
		t.Fatalf("after B's decision the task is %s, want QUEUED", st)
	}

	stored, leader, err := pg.ClusterRepo{}.GetEpoch(t.Context(), e.pool)
	if err != nil || stored != epochB || leader != "scheduler-b" {
		t.Fatalf("cluster_state = %d %q, %v; want %d scheduler-b", stored, leader, err, epochB)
	}
	rows, err := e.pool.Query(t.Context(), `SELECT (envelope->'payload'->>'epoch')::bigint FROM outbox
		WHERE envelope->>'event_type' = 'scheduler.leader_changed' ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	var got []int64
	for rows.Next() {
		var ep int64
		if err := rows.Scan(&ep); err != nil {
			t.Fatal(err)
		}
		got = append(got, ep)
	}
	if rows.Err() != nil || !slices.Equal(got, []int64{epochA, epochB}) {
		t.Fatalf("scheduler.leader_changed epochs = %v (%v), want [%d %d]", got, rows.Err(), epochA, epochB)
	}
}

// TestFencing_ActivationWaitIsBounded: a deposed leader frozen inside a transaction keeps
// AssertEpoch's share lock; the new leader's activation fails after its lock timeout instead of
// hanging (the Elector then campaigns again), and succeeds once the lock is gone (code review,
// 04.07).
func TestFencing_ActivationWaitIsBounded(t *testing.T) {
	e := newEnv(t)
	frozen, err := e.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = frozen.Rollback(context.Background()) }() // already rolled back below on success
	if err := (pg.ClusterRepo{}).AssertEpoch(t.Context(), frozen, 0); err != nil {
		t.Fatal(err)
	}
	rec := events.NewRecorder(clock.Real{}, ids.NewGenerator(clock.Real{}, crand.Reader), "flowforge-scheduler")
	fence := coordination.PGFence{Tx: e.tm, Cluster: pg.ClusterRepo{}, Events: rec, LockTimeout: 300 * time.Millisecond}
	// Unbounded, the activation would wait for the lock until this context's deadline.
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err = fence.Activate(ctx, 1, "scheduler-b")
	if err == nil {
		t.Fatal("activation succeeded while a deposed leader's transaction held the epoch lock")
	}
	if errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
		t.Fatalf("activation waited for the lock until the 2 s deadline (%v), want its 300ms lock timeout", err)
	}
	if err := frozen.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := fence.Activate(t.Context(), 1, "scheduler-b"); err != nil {
		t.Fatalf("activation once the lock is free: %v", err)
	}
}
