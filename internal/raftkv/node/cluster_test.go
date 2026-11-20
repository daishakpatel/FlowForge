package node_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/daishakpatel/flowforge/internal/raftkv/node"
	"github.com/daishakpatel/flowforge/internal/raftkv/nodetest"
	"github.com/daishakpatel/flowforge/internal/raftkv/storage"
	"github.com/daishakpatel/flowforge/internal/raftkv/transport"
)

// electionTimeouts is the task's bound for a new leader: 5 election timeouts of 10 ticks.
const electionTimeouts = 5 * 10

// forSeeds runs f once per seed: 20 seeds (task §9), 3 with -short. The seed drives the Fault
// and the proposal nonces; the raft library's election jitter is its own randomness, so every
// seed also runs a different election interleaving.
func forSeeds(t *testing.T, f func(t *testing.T, seed int64)) {
	n := 20
	if testing.Short() {
		n = 3
	}
	for seed := int64(1); seed <= int64(n); seed++ {
		t.Run(fmt.Sprint("seed=", seed), func(t *testing.T) { f(t, seed) })
	}
}

func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return c
}

func mustPropose(t *testing.T, c *nodetest.Cluster, id uint64, data string) node.Result {
	t.Helper()
	res, err := c.Node(id).Propose(ctx(t), []byte(data))
	if err != nil {
		t.Fatalf("Propose(%q) on %d: %v", data, id, err)
	}
	return res
}

// converge ticks until every running node applied the same sequence.
func converge(t *testing.T, c *nodetest.Cluster, maxRounds int) {
	t.Helper()
	if !c.TickUntil(c.Converged, maxRounds) {
		t.Fatalf("not converged after %d rounds", maxRounds)
	}
	if err := c.CheckPrefixes(); err != nil {
		t.Fatal(err)
	}
}

func appliedData(c *nodetest.Cluster, id uint64) []string {
	var ds []string
	for _, e := range c.Applier(id).Entries() {
		ds = append(ds, e.Data)
	}
	return ds
}

func TestCluster_ElectsOneStableLeader(t *testing.T) {
	forSeeds(t, func(t *testing.T, seed int64) {
		c := nodetest.New(t, nodetest.Options{Seed: seed})
		lead := c.WaitLeader(electionTimeouts)
		term := c.Node(lead).Status().Term
		for range 100 {
			c.Tick()
			leaders := 0
			for _, s := range c.Statuses() {
				if s.IsLeader {
					leaders++
				}
				if s.Leader != lead || s.Term != term {
					t.Fatalf("leadership moved without a fault: %+v (was %d in term %d)", s, lead, term)
				}
			}
			if leaders != 1 {
				t.Fatalf("%d leaders", leaders)
			}
		}
	})
}

func TestCluster_ReplicatesInOrder(t *testing.T) {
	forSeeds(t, func(t *testing.T, seed int64) {
		c := nodetest.New(t, nodetest.Options{Seed: seed})
		lead := c.WaitLeader(electionTimeouts)
		const total, workers = 1000, 8
		var wg sync.WaitGroup
		errs := make(chan error, total)
		for w := range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := w; i < total; i += workers {
					res, err := c.Node(lead).Propose(ctx(t), []byte(fmt.Sprint("p", i)))
					if err == nil && string(res.Data) != fmt.Sprint("ok:p", i) {
						err = fmt.Errorf("result %q for p%d", res.Data, i)
					}
					if err != nil {
						errs <- err
					}
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatal(err)
		}
		converge(t, c, 100)
		if got := len(c.Applier(lead).Entries()); got != total {
			t.Fatalf("applied %d entries, want %d", got, total)
		}
	})
}

func TestCluster_LeaderKillFailoverAndCatchUp(t *testing.T) {
	forSeeds(t, func(t *testing.T, seed int64) {
		c := nodetest.New(t, nodetest.Options{Seed: seed, SnapshotEntries: 20, SnapshotTrailing: 5})
		old := c.WaitLeader(electionTimeouts)
		for i := range 10 {
			mustPropose(t, c, old, fmt.Sprint("before", i))
		}
		c.Stop(old)
		lead := c.WaitLeader(electionTimeouts)
		if lead == old {
			t.Fatalf("stopped node %d still leads", old)
		}
		for i := range 100 {
			mustPropose(t, c, lead, fmt.Sprint("after", i))
		}
		c.Start(old) // far behind the compacted log: it catches up through a snapshot
		converge(t, c, 200)
		if got := len(c.Applier(old).Entries()); got != 110 {
			t.Fatalf("restarted node has %d entries, want 110", got)
		}
		if calls := c.Applier(old).ApplyCalls(); calls >= 110 {
			t.Errorf("restarted node applied %d entries one by one: expected a snapshot install", calls)
		}
	})
}

func TestCluster_FollowerRejectsWithLeaderHint(t *testing.T) {
	forSeeds(t, func(t *testing.T, seed int64) {
		c := nodetest.New(t, nodetest.Options{Seed: seed})
		lead := c.WaitLeader(electionTimeouts)
		follower := c.Others(lead)[0]
		_, err := c.Node(follower).Propose(ctx(t), []byte("x"))
		var nle *node.NotLeaderError
		if !errors.As(err, &nle) || !errors.Is(err, node.ErrNotLeader) || nle.Leader != lead || nle.Addr != fmt.Sprintf("n%d:7001", lead) {
			t.Fatalf("follower Propose = %v, want NotLeader(%d)", err, lead)
		}

		// During leader loss every proposal fails with a definite error, and none of them is ever
		// applied.
		c.Stop(lead)
		var failed []string
		c.TickUntil(func() bool {
			for _, id := range c.Others(lead) {
				data := fmt.Sprint("gap", len(failed))
				_, err := c.Node(id).Propose(ctx(t), []byte(data))
				if err == nil {
					return true // a new leader took it
				}
				if !errors.Is(err, node.ErrNotLeader) && !errors.Is(err, node.ErrNoQuorum) {
					t.Fatalf("Propose during leader loss = %v, want NotLeader or NoQuorum", err)
				}
				failed = append(failed, data)
			}
			return false
		}, electionTimeouts)
		c.Start(lead)
		converge(t, c, 100)
		for _, id := range c.IDs() {
			for _, d := range failed {
				if slices.Contains(appliedData(c, id), d) {
					t.Fatalf("%q failed definitely but node %d applied it", d, id)
				}
			}
		}
	})
}

func TestCluster_NoQuorumAppliesNothing(t *testing.T) {
	forSeeds(t, func(t *testing.T, seed int64) {
		c := nodetest.New(t, nodetest.Options{Seed: seed})
		lead := c.WaitLeader(electionTimeouts)
		mustPropose(t, c, lead, "committed")
		converge(t, c, 20)
		for _, id := range c.Others(lead) {
			c.Stop(id)
		}
		pctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, err := c.Node(lead).Propose(pctx, []byte("stranded"))
			done <- err
		}()
		// CheckQuorum: without a quorum the leader steps down within an election timeout.
		if !c.TickUntil(func() bool { return !c.Node(lead).Status().IsLeader }, electionTimeouts) {
			t.Fatal("leader without quorum did not step down")
		}
		if _, err := c.Node(lead).Propose(ctx(t), []byte("late")); !errors.Is(err, node.ErrNoQuorum) {
			t.Errorf("Propose without quorum = %v, want ErrNoQuorum", err)
		}
		c.TickUntil(func() bool { return false }, 20)
		cancel()
		if err := <-done; !errors.Is(err, node.ErrProposalTimeout) {
			t.Errorf("stranded proposal = %v, want ErrProposalTimeout", err)
		}
		if got := appliedData(c, lead); !slices.Equal(got, []string{"committed"}) {
			t.Fatalf("survivor applied %v without a quorum", got)
		}
	})
}

func TestCluster_PartitionedLeaderStepsDownAndRejoins(t *testing.T) {
	forSeeds(t, func(t *testing.T, seed int64) {
		c := nodetest.New(t, nodetest.Options{Seed: seed})
		old := c.WaitLeader(electionTimeouts)
		majority := c.Others(old)
		c.Fault.Partition([]uint64{old}, majority)
		stranded := make(chan error, 1)
		go func() { // appended on the isolated leader, never committed
			_, err := c.Node(old).Propose(ctx(t), []byte("stranded"))
			stranded <- err
		}()
		var lead uint64
		ok := c.TickUntil(func() bool {
			s := c.Node(majority[0]).Status()
			lead = s.Leader
			return lead != 0 && lead != old && c.Node(lead).Status().IsLeader && !c.Node(old).Status().IsLeader
		}, electionTimeouts)
		if !ok {
			t.Fatalf("no new majority leader, or the isolated leader kept leading: %+v", c.Statuses())
		}
		_, err := c.Node(old).Propose(ctx(t), []byte("rejected"))
		if !errors.Is(err, node.ErrNoQuorum) && !errors.Is(err, node.ErrNotLeader) {
			t.Fatalf("Propose on the deposed leader = %v", err)
		}
		for i := range 5 {
			mustPropose(t, c, lead, fmt.Sprint("majority", i))
		}
		term := c.Node(lead).Status().Term

		c.Fault.Heal()
		converge(t, c, 100)
		// PreVote: the rejoining node does not disrupt the cluster with a higher term.
		if s := c.Node(lead).Status(); !s.IsLeader || s.Term != term {
			t.Errorf("leader after heal: %+v, want %d still leading in term %d", s, lead, term)
		}
		// Its uncommitted entry was discarded: a definite failure, applied nowhere.
		if err := <-stranded; !errors.Is(err, node.ErrProposalDropped) {
			t.Errorf("stranded proposal = %v, want ErrProposalDropped", err)
		}
		for _, id := range c.IDs() {
			if d := appliedData(c, id); slices.Contains(d, "stranded") || slices.Contains(d, "rejected") || len(d) != 5 {
				t.Errorf("node %d applied %v", id, d)
			}
		}
	})
}

func TestCluster_RestartAllPreservesState(t *testing.T) {
	forSeeds(t, func(t *testing.T, seed int64) {
		opts := nodetest.Options{Seed: seed, SnapshotEntries: 25, SnapshotTrailing: 5}
		// The first seeds restart from real bbolt files; the rest from Mem, which holds exactly what
		// Save persisted (same conformance suite) and keeps the 20-seed run fast: Bolt fsyncs every
		// Ready, about 3 s per seed here.
		if seed <= 3 {
			dir := t.TempDir()
			opts.Storage = func(id uint64) (storage.Storage, error) {
				return storage.OpenBolt(filepath.Join(dir, fmt.Sprint(id, ".db")), storage.Identity{ClusterID: 1, NodeID: id})
			}
		}
		c := nodetest.New(t, opts)
		lead := c.WaitLeader(electionTimeouts)
		var wg sync.WaitGroup // concurrent proposals share fsyncs (Bolt syncs every Ready)
		for w := range 6 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range 10 {
					if _, err := c.Node(lead).Propose(ctx(t), []byte(fmt.Sprint("e", w, "-", i))); err != nil {
						t.Errorf("Propose: %v", err)
					}
				}
			}()
		}
		wg.Wait()
		converge(t, c, 50)
		before := c.Applier(lead).Entries()

		c.StopAll()
		for _, id := range c.IDs() {
			c.Start(id)
		}
		for _, id := range c.IDs() {
			r := c.Applier(id)
			// Restored from the snapshot, then only the entries after it re-applied (rule 5).
			if got := r.Entries(); len(got) > len(before) || !slices.Equal(got, before[:len(got)]) {
				t.Fatalf("node %d restarted with %d entries that differ from before", id, len(got))
			}
			if r.ApplyCalls() >= len(before) {
				t.Errorf("node %d re-applied %d entries: expected to resume from its snapshot", id, r.ApplyCalls())
			}
		}
		lead = c.WaitLeader(electionTimeouts)
		mustPropose(t, c, lead, "after-restart")
		converge(t, c, 50)
		if got := c.Applier(lead).Entries(); len(got) != len(before)+1 || !slices.Equal(got[:len(before)], before) {
			t.Fatalf("state after full restart: %d entries", len(got))
		}
	})
}

// TestCluster_ChaosKeepsAppliedSequencesConsistent runs continuous proposals under random drops,
// delays, duplicates, reordering and partitions. Invariants: after every round the applied
// sequences are prefixes of one another; after healing they are equal; a proposal that failed
// definitely is applied nowhere; a successful one is applied everywhere at its index.
func TestCluster_ChaosKeepsAppliedSequencesConsistent(t *testing.T) {
	forSeeds(t, func(t *testing.T, seed int64) {
		c := nodetest.New(t, nodetest.Options{Seed: seed, SnapshotEntries: 40, SnapshotTrailing: 10})
		rng := rand.New(rand.NewSource(seed)) //nolint:gosec // test randomness
		c.Fault.SetDefault(transport.Faults{Drop: 0.05, Duplicate: 0.05, Delay: 0.1, MaxDelayTicks: 3, Reorder: 0.2})
		pctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		type outcome struct {
			data string
			res  node.Result
			err  error
		}
		var wg sync.WaitGroup
		outcomes := make(chan outcome, 1000)
		for round := range 300 {
			if round%40 == 20 {
				ids := c.IDs()
				rng.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
				cut := 1 + rng.Intn(len(ids)-1)
				c.Fault.Partition(ids[:cut], ids[cut:])
			}
			if round%40 == 35 {
				c.Fault.Partition(c.IDs()) // one group: partition healed, link faults kept
			}
			for _, id := range c.IDs() {
				if s := c.Node(id).Status(); s.IsLeader {
					data := fmt.Sprintf("r%d-n%d", round, id)
					wg.Add(1)
					go func() {
						defer wg.Done()
						res, err := c.Node(id).Propose(pctx, []byte(data))
						outcomes <- outcome{data, res, err}
					}()
				}
			}
			c.Tick()
			if err := c.CheckPrefixes(); err != nil {
				t.Fatalf("round %d: %v", round, err)
			}
		}
		c.Fault.Heal()
		c.WaitLeader(200)
		mustPropose(t, c, c.Leader(), "final") // commits everything a current leader holds
		converge(t, c, 200)
		cancel()
		wg.Wait()
		close(outcomes)
		final := c.Applier(c.IDs()[0]).Entries()
		at := map[string]uint64{}
		for _, e := range final {
			at[e.Data] = e.Index
		}
		for o := range outcomes {
			definite := errors.Is(o.err, node.ErrNotLeader) || errors.Is(o.err, node.ErrNoQuorum) || errors.Is(o.err, node.ErrProposalDropped)
			switch idx, applied := at[o.data]; {
			case o.err == nil && (!applied || idx != o.res.Index):
				t.Errorf("%q succeeded at %d but the cluster applied it at %d (%v)", o.data, o.res.Index, idx, applied)
			case definite && applied:
				t.Errorf("%q failed definitely (%v) but was applied at %d", o.data, o.err, idx)
			case o.err != nil && !definite && !errors.Is(o.err, node.ErrProposalTimeout):
				t.Errorf("%q: unexpected error %v", o.data, o.err)
			}
		}
	})
}

// savedState is what one node's storage holds, as the instrumented storage last saw it.
type savedState struct {
	term, vote, last uint64
}

type instrumented struct {
	storage.Storage
	mu    *sync.Mutex
	state *savedState
}

func (s instrumented) Save(hs *pb.HardState, ents []*pb.Entry, snap *pb.Snapshot) error {
	if err := s.Storage.Save(hs, ents, snap); err != nil {
		return err
	}
	h, _, _ := s.InitialState()
	last, _ := s.LastIndex()
	s.mu.Lock()
	*s.state = savedState{term: h.GetTerm(), vote: h.GetVote(), last: last}
	s.mu.Unlock()
	return nil
}

// TestCluster_PersistsBeforeSending checks rule 1 on every message of a run with elections,
// replication and a leader restart: when a node sends a message, its storage already holds the
// term it carries, a granted vote, and the entries it appends or acknowledges.
func TestCluster_PersistsBeforeSending(t *testing.T) {
	forSeeds(t, func(t *testing.T, seed int64) {
		var mu sync.Mutex
		states := map[uint64]*savedState{}
		mems := map[uint64]storage.Storage{}
		var violations []string
		c := nodetest.New(t, nodetest.Options{Seed: seed,
			Storage: func(id uint64) (storage.Storage, error) {
				mu.Lock()
				defer mu.Unlock()
				if mems[id] == nil {
					mems[id], states[id] = storage.NewMem(), &savedState{}
				}
				return instrumented{Storage: mems[id], mu: &mu, state: states[id]}, nil
			},
			OnSend: func(m *pb.Message) {
				mu.Lock()
				defer mu.Unlock()
				if v := checkPersisted(m, *states[m.GetFrom()]); v != "" {
					violations = append(violations, v)
				}
			}})
		lead := c.WaitLeader(electionTimeouts)
		for i := range 20 {
			mustPropose(t, c, lead, fmt.Sprint("e", i))
		}
		c.Restart(lead)
		lead = c.WaitLeader(electionTimeouts)
		mustPropose(t, c, lead, "after")
		converge(t, c, 50)
		mu.Lock()
		defer mu.Unlock()
		if len(violations) > 0 {
			t.Fatalf("%d messages sent before their state was persisted, e.g. %s", len(violations), violations[0])
		}
	})
}

func checkPersisted(m *pb.Message, s savedState) string {
	t := m.GetType()
	switch {
	case t == pb.MsgPreVote || t == pb.MsgPreVoteResp:
		return "" // pre-votes carry a future term that is never persisted, by design
	case m.GetTerm() > s.term:
		return fmt.Sprintf("%v term %d > persisted %d", t, m.GetTerm(), s.term)
	case t == pb.MsgVoteResp && !m.GetReject() && s.vote != m.GetTo():
		return fmt.Sprintf("vote for %d granted, persisted vote %d", m.GetTo(), s.vote)
	case t == pb.MsgApp && len(m.GetEntries()) > 0 && m.GetEntries()[len(m.GetEntries())-1].GetIndex() > s.last:
		return fmt.Sprintf("MsgApp up to %d, persisted %d", m.GetEntries()[len(m.GetEntries())-1].GetIndex(), s.last)
	case t == pb.MsgAppResp && !m.GetReject() && m.GetIndex() > s.last:
		return fmt.Sprintf("MsgAppResp acks %d, persisted %d", m.GetIndex(), s.last)
	}
	return ""
}

// TestCluster_ReadIndexSurvivesALostRequest: raft never retransmits MsgReadIndex, so the node
// re-issues pending reads; a read whose request was dropped still completes after the link heals.
func TestCluster_ReadIndexSurvivesALostRequest(t *testing.T) {
	forSeeds(t, func(t *testing.T, seed int64) {
		c := nodetest.New(t, nodetest.Options{Seed: seed})
		lead := c.WaitLeader(electionTimeouts)
		mustPropose(t, c, lead, "x")
		follower := c.Others(lead)[0]
		c.Fault.SetLink(transport.Link{From: follower, To: lead}, transport.Faults{Drop: 1})
		done := make(chan error, 1)
		go func() {
			_, err := c.Node(follower).ReadIndex(ctx(t))
			done <- err
		}()
		c.TickUntil(func() bool { return false }, 3) // the request is sent and dropped
		c.Fault.Heal()
		var err error
		if !c.TickUntil(func() bool {
			select {
			case err = <-done:
				return true
			default:
				return false
			}
		}, electionTimeouts) {
			t.Fatal("ReadIndex never completed after its request was lost")
		}
		if err != nil {
			t.Fatalf("ReadIndex: %v", err)
		}
	})
}
