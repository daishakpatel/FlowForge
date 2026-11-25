//go:build fault

package raft_test

// The fencing scenarios (task 04.08 scenario 12 and extra X3; FR-RAFT-08, ADR-003): real Raft
// (kvtest), coordination.Electors over their own Raft clients, and 04.07's PGFence plus
// ClusterRepo.AssertEpoch on Postgres (testcontainers). The invariant: the epochs of committed
// scheduler decisions never go backwards.

import (
	"context"
	crand "crypto/rand"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/config"
	"github.com/daishakpatel/flowforge/internal/coordination"
	"github.com/daishakpatel/flowforge/internal/events"
	pbc "github.com/daishakpatel/flowforge/internal/gen/proto/coordination/v1"
	"github.com/daishakpatel/flowforge/internal/ids"
	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/persistence/pg"
	"github.com/daishakpatel/flowforge/internal/raftkv/client"
	"github.com/daishakpatel/flowforge/internal/raftkv/kvtest"
	"github.com/daishakpatel/flowforge/internal/testutil"
)

// The leader lease the Compose demo uses (04.07): a takeover takes about TTL + renew.
const (
	leaderTTL     = 6 * time.Second
	leaderRenew   = 2 * time.Second
	schedFailover = 10 * time.Second // 00_MASTER §6.1: scheduler leader failover
)

// cutKV is a coordination.KV that a test can cut off from Raft, as a paused or partitioned
// scheduler process is.
type cutKV struct {
	coordination.KV
	cut atomic.Bool
}

var errCut = fmt.Errorf("%w: scheduler cut off (fault injection)", client.ErrUnavailable)

func (k *cutKV) Get(ctx context.Context, key string) (*pbc.KeyValue, error) {
	if k.cut.Load() {
		return nil, errCut
	}
	return k.KV.Get(ctx, key)
}

func (k *cutKV) List(ctx context.Context, prefix, start string) ([]*pbc.KeyValue, string, error) {
	if k.cut.Load() {
		return nil, "", errCut
	}
	return k.KV.List(ctx, prefix, start)
}

func (k *cutKV) Put(ctx context.Context, key string, value []byte, lease uint64) (*pbc.KeyValue, error) {
	if k.cut.Load() {
		return nil, errCut
	}
	return k.KV.Put(ctx, key, value, lease)
}

func (k *cutKV) CAS(ctx context.Context, key string, expected uint64, value []byte, lease uint64) (bool, *pbc.KeyValue, error) {
	if k.cut.Load() {
		return false, nil, errCut
	}
	return k.KV.CAS(ctx, key, expected, value, lease)
}

func (k *cutKV) LeaseGrant(ctx context.Context, ttl time.Duration, owner string) (uint64, uint64, error) {
	if k.cut.Load() {
		return 0, 0, errCut
	}
	return k.KV.LeaseGrant(ctx, ttl, owner)
}

func (k *cutKV) LeaseKeepAlive(ctx context.Context, id, token uint64) error {
	if k.cut.Load() {
		return errCut
	}
	return k.KV.LeaseKeepAlive(ctx, id, token)
}

func (k *cutKV) LeaseRevoke(ctx context.Context, id uint64) error {
	if k.cut.Load() {
		return errCut
	}
	return k.KV.LeaseRevoke(ctx, id)
}

func (k *cutKV) IncrCounter(ctx context.Context, name string) (uint64, error) {
	if k.cut.Load() {
		return 0, errCut
	}
	return k.KV.IncrCounter(ctx, name)
}

// fenced is a Raft cluster, a Postgres database with the epoch fence, and a decisions table that
// stands in for scheduler writes.
type fenced struct {
	s     *scenario
	c     *kvtest.Cluster
	pool  *pgxpool.Pool
	tm    *pg.TxManager
	fence coordination.PGFence
	wg    sync.WaitGroup
	stop  context.CancelFunc
	ctx   context.Context //nolint:containedctx // the candidates' shared lifetime, cancelled in Cleanup
}

func newFenced(s *scenario, opts kvtest.Options) *fenced {
	t := s.t
	t.Helper()
	db := testutil.NewFlowforgeDB(t)
	pool, err := pg.NewPool(t.Context(), config.Secret(db.DSN))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(t.Context(), `CREATE TABLE fault_decisions (id bigserial PRIMARY KEY, node text NOT NULL, epoch bigint NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	tm := pg.NewTxManager(pool, clock.Real{}, nil)
	rec := events.NewRecorder(clock.Real{}, ids.NewGenerator(clock.Real{}, crand.Reader), "flowforge-scheduler")
	ctx, cancel := context.WithCancel(context.Background())
	f := &fenced{s: s, c: prodCluster(t, opts), pool: pool, tm: tm, ctx: ctx, stop: cancel,
		fence: coordination.PGFence{Tx: tm, Cluster: pg.ClusterRepo{}, Events: rec}}
	t.Cleanup(func() { cancel(); f.wg.Wait() })
	return f
}

// candidate is one scheduler: an Elector over its own Raft client, cuttable.
type candidate struct {
	name string
	kv   *cutKV
	el   *coordination.Elector
}

func (f *fenced) candidate(name string, stream uint64, ttl, renew time.Duration) *candidate {
	t := f.s.t
	t.Helper()
	cl := f.s.client(f.c.Endpoints(), stream)
	t.Cleanup(func() { closeClient(cl) })
	kv := &cutKV{KV: coordination.NewKV(cl)}
	el, err := coordination.NewElector(coordination.ElectorConfig{NodeID: name, TTL: ttl, Renew: renew}, kv, f.fence)
	if err != nil {
		t.Fatal(err)
	}
	f.wg.Go(func() { _ = el.Run(f.ctx) })
	return &candidate{name: name, kv: kv, el: el}
}

// decide commits one decision at epoch, fenced like every scheduler transaction (ADR-003).
func (f *fenced) decide(ctx context.Context, node string, epoch int64) error {
	return f.tm.WithTx(ctx, pgx.TxOptions{}, func(tx pgx.Tx) error {
		if err := (pg.ClusterRepo{}).AssertEpoch(ctx, tx, epoch); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO fault_decisions (node, epoch) VALUES ($1, $2)`, node, epoch)
		return err
	})
}

// epochsNeverDecrease checks the fence's invariant over every committed decision, in commit order.
func (f *fenced) epochsNeverDecrease() (decisions, terms int) {
	t := f.s.t
	t.Helper()
	rows, err := f.pool.Query(t.Context(), `SELECT node, epoch FROM fault_decisions ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var last int64 = -1
	for rows.Next() {
		var node string
		var epoch int64
		if err := rows.Scan(&node, &epoch); err != nil {
			t.Fatal(err)
		}
		if epoch < last {
			t.Fatalf("decision %d by %s committed at epoch %d after one at epoch %d: the fence failed", decisions+1, node, epoch, last)
		}
		if epoch != last {
			terms++
		}
		last = epoch
		decisions++
	}
	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}
	return decisions, terms
}

func waitLeads(t *testing.T, c *candidate, d time.Duration) {
	t.Helper()
	if !testutil.Eventually(c.el.Gate().IsLeader, d, 20*time.Millisecond) {
		t.Fatalf("%s did not become leader within %s", c.name, d)
	}
}

func TestFault_12_StaleSchedulerCannotAct(t *testing.T) {
	s := newScenario(t, "12", "Stale scheduler cannot act after a new epoch", inProcess,
		"the leading scheduler is cut off from Raft (a paused process) while another campaigns; then it wakes and makes the decision it had pending, with its old epoch",
		"the other scheduler takes over within the 10 s target and its epoch is fenced in Postgres; the old leader's decision fails with STALE_EPOCH and commits nothing; committed epochs never decrease; the KV history is linearizable")
	f := newFenced(s, kvtest.Options{})
	s.target("scheduler failover", schedFailover)
	l := s.startLoad(f.c.Endpoints(), 2)
	a := f.candidate("scheduler-a", 60, leaderTTL, leaderRenew)
	waitLeads(t, a, 20*time.Second)
	epochA := a.el.Gate().Epoch()
	if err := f.decide(t.Context(), a.name, epochA); err != nil {
		t.Fatalf("leader's decision: %v", err)
	}
	b := f.candidate("scheduler-b", 61, leaderTTL, leaderRenew)
	s.inject(func() { a.kv.cut.Store(true) })
	waitLeads(t, b, 30*time.Second)
	took := s.clk.Now().Sub(s.faultAt)
	secs := took.Seconds()
	s.row.TTRSeconds = &secs
	epochB := b.el.Gate().Epoch()
	err := f.decide(t.Context(), a.name, epochA)
	if !errors.Is(err, persistence.ErrStaleEpoch) {
		t.Fatalf("the deposed leader's decision at epoch %d: %v, want STALE_EPOCH", epochA, err)
	}
	if err := f.decide(t.Context(), b.name, epochB); err != nil {
		t.Fatalf("new leader's decision: %v", err)
	}
	decisions, terms := f.epochsNeverDecrease()
	s.observe("scheduler-a (epoch %d) cut off; scheduler-b took over at epoch %d after %.2f s (TTL %s, renew %s); a's pending decision got STALE_EPOCH; %d decisions over %d terms, epochs never decreased",
		epochA, epochB, secs, leaderTTL, leaderRenew, decisions, terms)
	l.stop()
	s.check()
	s.converged(f.c, f.c.IDs())
}

// skewed is a clock running ahead by offset; durations are unaffected.
type skewed struct {
	clock.Clock
	offset time.Duration
}

func (c skewed) Now() time.Time { return c.Clock.Now().Add(c.offset) }

func TestFault_X3_ClockSkew(t *testing.T) {
	s := newScenario(t, "X3", "Clock skew", inProcess,
		"Raft node 1's clock runs 2 s ahead (it stamps commands and judges lease expiry while it leads); Raft leadership moves among the nodes every 1.5 s while two schedulers (3 s lease, renewed every 1 s) keep deciding",
		"lease timing shifts (earlier or later expiry judgments) and scheduler leadership changes at least once, but safety holds through fencing: committed decision epochs never decrease; the KV history is linearizable")
	f := newFenced(s, kvtest.Options{MemberClock: func(id uint64) clock.Clock {
		if id == 1 {
			return skewed{Clock: clock.Real{}, offset: 2 * time.Second}
		}
		return nil
	}})
	l := s.startLoad(f.c.Endpoints(), 2)
	// A 3 s scheduler lease renewed every 1 s: a lease stamped by a normal node is judged expired
	// about 1 s early while node 1 leads, so the skew can depose a scheduler that still believes it leads.
	cands := []*candidate{f.candidate("scheduler-a", 60, 3*time.Second, time.Second), f.candidate("scheduler-b", 61, 3*time.Second, time.Second)}
	var stale, committed atomic.Int64
	for _, cand := range cands {
		f.wg.Go(func() {
			for f.ctx.Err() == nil {
				if epoch, leading := cand.el.Gate().Term(); leading {
					switch err := f.decide(f.ctx, cand.name, epoch); {
					case errors.Is(err, persistence.ErrStaleEpoch):
						stale.Add(1)
					case err == nil:
						committed.Add(1)
					}
				}
				_ = clock.Real{}.Sleep(f.ctx, 50*time.Millisecond)
			}
		})
	}
	rng := rand.New(rand.NewPCG(seed, 3)) //nolint:gosec // G404: seeded transfer schedule
	s.inject(func() {})
	moves := 0
	for end := s.clk.Now().Add(12 * time.Second); s.clk.Now().Before(end); {
		s.run(1500 * time.Millisecond)
		lead := f.c.Leader()
		if lead == 0 {
			continue
		}
		others := f.c.Others(lead)
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		if f.c.Member(lead).Node.TransferLeadership(ctx, others[rng.IntN(len(others))]) == nil {
			moves++
		}
		cancel()
	}
	f.stop()
	l.stop()
	decisions, terms := f.epochsNeverDecrease()
	if decisions == 0 || terms < 2 {
		t.Fatalf("%d decisions over %d scheduler term(s): the skew never moved scheduler leadership, so fencing was not exercised", decisions, terms)
	}
	s.observe("node 1 at +2 s; %d Raft leadership moves; %d decisions committed over %d scheduler terms, %d rejected as STALE_EPOCH; committed epochs never decreased",
		moves, decisions, terms, stale.Load())
	s.check()
	s.converged(f.c, f.c.IDs())
}
