package coordination_test

import (
	"context"
	"io"
	"log/slog"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/coordination"
	"github.com/daishakpatel/flowforge/internal/raftkv/client"
	"github.com/daishakpatel/flowforge/internal/raftkv/kvtest"
	"github.com/daishakpatel/flowforge/internal/testutil"
)

// Real-time settings over a kvtest cluster (10 ms raft ticks: elections take 100–200 ms).
const (
	raftTTL    = 2 * time.Second
	raftRenew  = 300 * time.Millisecond
	raftMargin = 700 * time.Millisecond
)

// renewalLog wraps a KV and records when each keep-alive succeeded.
type renewalLog struct {
	coordination.KV
	clk clock.Clock
	mu  sync.Mutex
	ok  []time.Time
}

func (r *renewalLog) LeaseKeepAlive(ctx context.Context, id, token uint64) error {
	start := r.clk.Now()
	err := r.KV.LeaseKeepAlive(ctx, id, token)
	if err == nil {
		r.mu.Lock()
		r.ok = append(r.ok, start)
		r.mu.Unlock()
	}
	return err
}

// maxGap is the longest time between successful keep-alives after from.
func (r *renewalLog) maxGap(from time.Time) time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	var gap time.Duration
	prev := from
	for _, t := range r.ok {
		if t.After(from) {
			gap = max(gap, t.Sub(prev))
			prev = t
		}
	}
	return gap
}

func raftKV(t *testing.T, c *kvtest.Cluster) coordination.KV {
	t.Helper()
	cl, err := client.New(client.Config{Endpoints: c.Endpoints(), BaseBackoff: 60 * time.Millisecond, MaxBackoff: 600 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cl.Close() })
	return coordination.NewKV(cl)
}

func runElector(t *testing.T, name string, kv coordination.KV) *coordination.Elector {
	t.Helper()
	e, err := coordination.NewElector(coordination.ElectorConfig{
		NodeID: name, TTL: raftTTL, Renew: raftRenew, SafetyMargin: raftMargin,
		Rand: rand.New(rand.NewPCG(uint64(len(name)), 7)), Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), //nolint:gosec // G404: seeded jitter
	}, kv, &recordingFence{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = e.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return e
}

func waitLeading(t *testing.T, e *coordination.Elector, within time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), within)
	defer cancel()
	if err := e.Gate().Wait(ctx); err != nil {
		t.Fatalf("not leading within %s: %v", within, err)
	}
}

// TestRaft_LeaderNodeKillKeepsSchedulerLeadership: killing the Raft leader node while a scheduler
// leads only delays its renewals by a client failover, well inside TTL − margin, so the
// scheduler keeps leading the same term.
func TestRaft_LeaderNodeKillKeepsSchedulerLeadership(t *testing.T) {
	c := kvtest.New(t, kvtest.Options{})
	c.WaitLeader(5 * time.Second)
	log := &renewalLog{KV: raftKV(t, c), clk: clock.Real{}}
	e := runElector(t, "s1", log)
	waitLeading(t, e, 10*time.Second)
	done, epoch := e.Gate().Done(), e.Gate().Epoch()
	for range 3 {
		killedAt := clock.Real{}.Now()
		lead := c.WaitLeader(5 * time.Second)
		c.Kill(lead)
		testutil.WaitFor(t, func() bool { return log.maxGap(killedAt) > 0 && c.Leader() != 0 }, 10*time.Second)
		_ = (clock.Real{}).Sleep(context.Background(), raftTTL) // observe a full TTL after the failover
		c.Start(lead)
		gap := log.maxGap(killedAt)
		t.Logf("raft leader %d killed: longest gap between successful renewals %s (local deadline after %s)", lead, gap, raftTTL-raftMargin)
		if gap >= raftTTL-raftMargin {
			t.Fatalf("renewals stalled %s, beyond the local deadline", gap)
		}
	}
	if !e.Gate().IsLeader() || closed(done) || e.Gate().Epoch() != epoch {
		t.Fatalf("leadership lost across raft failovers: leader %v, done %v, epoch %d → %d", e.Gate().IsLeader(), closed(done), epoch, e.Gate().Epoch())
	}
}

// TestRaft_QuorumLossStepsDownUntilItReturns: without a Raft quorum the leader can't renew, so it
// steps down at its local deadline; no scheduler leads while quorum is lost (00_MASTER §6.2);
// when quorum returns a scheduler leads again at a higher epoch.
func TestRaft_QuorumLossStepsDownUntilItReturns(t *testing.T) {
	c := kvtest.New(t, kvtest.Options{})
	c.WaitLeader(5 * time.Second)
	s1 := runElector(t, "s1", raftKV(t, c))
	s2 := runElector(t, "s2-standby", raftKV(t, c))
	testutil.WaitFor(t, func() bool { return s1.Gate().IsLeader() || s2.Gate().IsLeader() }, 10*time.Second)
	lead, other := s1, s2
	if s2.Gate().IsLeader() {
		lead, other = s2, s1
	}
	epoch := lead.Gate().Epoch()
	down := c.Others(c.WaitLeader(5 * time.Second))
	start := clock.Real{}.Now()
	for _, id := range down {
		c.Kill(id)
	}
	select {
	case <-lead.Gate().Done():
	case <-clock.Real{}.After(raftTTL + raftRenew + time.Second):
		t.Fatal("the leader did not step down without quorum")
	}
	t.Logf("stepped down %s after quorum loss (local deadline ≤ %s)", clock.Real{}.Now().Sub(start), raftTTL-raftMargin+raftRenew)
	for range 2 * raftTTL / raftRenew { // two TTLs without quorum: nobody may lead
		if s1.Gate().IsLeader() || s2.Gate().IsLeader() {
			t.Fatal("a scheduler leads without a Raft quorum")
		}
		_ = (clock.Real{}).Sleep(context.Background(), raftRenew)
	}
	for _, id := range down {
		c.Start(id)
	}
	testutil.WaitFor(t, func() bool { return s1.Gate().IsLeader() || s2.Gate().IsLeader() }, 15*time.Second)
	next := slices.MaxFunc([]*coordination.Elector{lead, other}, func(a, b *coordination.Elector) int {
		return int(a.Gate().Epoch() - b.Gate().Epoch())
	})
	if !next.Gate().IsLeader() || next.Gate().Epoch() <= epoch {
		t.Fatalf("after quorum returned: epoch %d (was %d), leading %v", next.Gate().Epoch(), epoch, next.Gate().IsLeader())
	}
}

// TestRaft_WorkerRegistryOverRaft runs the registry lifecycle against the real KV.
func TestRaft_WorkerRegistryOverRaft(t *testing.T) {
	c := kvtest.New(t, kvtest.Options{})
	c.WaitLeader(5 * time.Second)
	reg, err := coordination.NewWorkerRegistry(raftKV(t, c), coordination.RegistryConfig{TTL: raftTTL})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	h, err := reg.Register(ctx, worker("w1", "k1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reg.Register(ctx, worker("w2", "k1")); err != nil {
		t.Fatal(err)
	}
	if alive, err := reg.List(ctx); err != nil || len(alive) != 2 {
		t.Fatalf("List = %v, %v", alive, err)
	}
	// w2 is never renewed: it expires; w1 is renewed and stays.
	testutil.WaitFor(t, func() bool {
		if h, err = reg.Renew(ctx, h); err != nil {
			t.Fatal(err)
		}
		exp, err := reg.Expired(ctx, []string{"w1", "w2"})
		return err == nil && slices.Equal(exp, []string{"w2"})
	}, 3*raftTTL)
	if err := reg.Deregister(ctx, h); err != nil {
		t.Fatal(err)
	}
	if alive, _ := reg.List(ctx); len(alive) != 0 {
		t.Fatalf("workers left after deregistering: %v", alive)
	}
}
