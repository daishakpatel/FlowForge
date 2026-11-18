//go:build integration

package outbox

import (
	crand "crypto/rand"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/config"
	"github.com/daishakpatel/flowforge/internal/events"
	"github.com/daishakpatel/flowforge/internal/ids"
	"github.com/daishakpatel/flowforge/internal/persistence/pg"
	"github.com/daishakpatel/flowforge/internal/scheduler"
	"github.com/daishakpatel/flowforge/internal/testutil"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
	"github.com/daishakpatel/flowforge/internal/workflow/statemachine"
)

// pgEnv is a migrated database with n task events over runs runs, recorded the production way.
type pgEnv struct {
	pool *pgxpool.Pool
	tm   *pg.TxManager
}

func newPGEnv(t *testing.T, n, runs int) *pgEnv {
	t.Helper()
	db := testutil.NewFlowforgeDB(t)
	pool, err := pg.NewPool(t.Context(), config.Secret(db.DSN))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	e := &pgEnv{pool: pool, tm: pg.NewTxManager(pool, clock.Real{}, nil)}
	rec := events.NewRecorder(clock.Real{}, ids.NewGenerator(clock.Real{}, crand.Reader), "test")
	for start := 0; start < n; start += 100 {
		if err := e.tm.WithTx(t.Context(), pgx.TxOptions{}, func(tx pgx.Tx) error {
			for i := start; i < min(start+100, n); i++ {
				run := fmt.Sprintf("run_%02d", i%runs)
				ev := events.NewTaskEvent(statemachine.EventTaskQueued, events.TaskPayload{RunID: run, TaskID: fmt.Sprintf("task_%04d", i), TaskKey: "k", Status: model.TaskQueued})
				if err := rec.Record(t.Context(), tx, ev); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	return e
}

func (e *pgEnv) relay(p Publisher, batch int) *RelayLoop {
	return &RelayLoop{DB: e.pool, Tx: e.tm, Cluster: pg.ClusterRepo{}, Outbox: pg.OutboxRepo{}, Publisher: p, Clock: clock.Real{}, Batch: batch}
}

// published returns the ids of the outbox rows marked published, in id order.
func (e *pgEnv) published(t *testing.T) []int64 {
	t.Helper()
	rows, err := e.pool.Query(t.Context(), `SELECT id FROM outbox WHERE published_at IS NOT NULL ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// TestRelayPG_MarksOnlyThePublishedPrefix: against Postgres, a failure after 3 of 5 rows leaves
// exactly the first 3 rows with published_at.
func TestRelayPG_MarksOnlyThePublishedPrefix(t *testing.T) {
	t.Parallel()
	e := newPGEnv(t, 5, 1)
	if err := e.relay(&prefixPublisher{n: 3}, 500).Once(t.Context(), scheduler.AlwaysLeader{}); err == nil {
		t.Fatal("partial failure not reported")
	}
	got := e.published(t)
	if len(got) != 3 || got[2] != got[0]+2 {
		t.Fatalf("published rows %v; want the first 3 of 5", got)
	}
	if n, _, _ := (pg.OutboxRepo{}).Backlog(t.Context(), e.pool); n != 2 {
		t.Errorf("backlog %d; want 2", n)
	}
}

// TestRelayPG_FlakyPublisher1000Events is task 03.07 §8's relay test against Postgres: 1 000 events
// of 10 runs, a publisher failing every 7th batch. Every event is published at least once, and
// deduping on event_id leaves each run's events in recorded order.
func TestRelayPG_FlakyPublisher1000Events(t *testing.T) {
	t.Parallel()
	e := newPGEnv(t, 1000, 10)
	p := &flakyPublisher{rng: rand.New(rand.NewPCG(flakySeed, 0))} //nolint:gosec // G404: seeded test randomness
	r := e.relay(p, 50)
	for pass := 0; ; pass++ {
		if pass > 200 {
			t.Fatalf("seed %d: backlog never drained", flakySeed)
		}
		_ = r.Once(t.Context(), scheduler.AlwaysLeader{}) // failures are retried by the next pass
		if n, _, err := (pg.OutboxRepo{}).Backlog(t.Context(), e.pool); err != nil || n == 0 {
			break
		}
	}
	if p.failure == 0 || len(p.Messages()) <= 1000 {
		t.Fatalf("seed %d: %d failures, %d messages; failures and duplicates must be injected", flakySeed, p.failure, len(p.Messages()))
	}
	checkOrderAndDedupe(t, p.Messages(), 1000)
	if got := len(e.published(t)); got != 1000 {
		t.Errorf("%d rows marked published; want 1000", got)
	}
	t.Logf("seed %d: %d failures, %d messages for 1000 events", flakySeed, p.failure, len(p.Messages()))
}

func TestRelayPG_NotLeaderPublishesNothing(t *testing.T) {
	t.Parallel()
	e := newPGEnv(t, 3, 1)
	p := &MemoryPublisher{}
	if err := e.relay(p, 500).Once(t.Context(), notLeader{}); err != nil || len(p.Messages()) != 0 || len(e.published(t)) != 0 {
		t.Fatalf("not leader: err %v, published %d, marked %d", err, len(p.Messages()), len(e.published(t)))
	}
}
