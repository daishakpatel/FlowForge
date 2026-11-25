//go:build fault

package raft_test

// The in-process Raft fault scenarios (task 04.08, FR-RAFT-10, v1.0 §8.7): every member is the
// full flowforge-raft stack over loopback gRPC (kvtest), at the production timing of 100 ms ticks
// and a 1 s election timeout (00E §5.3), so measured recovery times compare with 00_MASTER §6.1.

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	pc "github.com/anishathalye/porcupine"
	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/daishakpatel/flowforge/internal/raftkv/client"
	"github.com/daishakpatel/flowforge/internal/raftkv/kvtest"
	"github.com/daishakpatel/flowforge/internal/raftkv/storage"
	"github.com/daishakpatel/flowforge/internal/raftkv/transport"
	"github.com/daishakpatel/flowforge/internal/testutil"
	"github.com/daishakpatel/flowforge/internal/testutil/porcupine"
)

const (
	inProcess    = "in-process"
	raftFailover = 3 * time.Second // 00_MASTER §6.1: Raft leader failover (kill → client writes succeed)
	clientsPer   = 4
)

// prodCluster starts a cluster at the production Raft timing and waits for a leader.
func prodCluster(t *testing.T, opts kvtest.Options) *kvtest.Cluster {
	t.Helper()
	opts.TickInterval = 100 * time.Millisecond
	c := kvtest.New(t, opts)
	c.WaitLeader(15 * time.Second)
	return c
}

// leaderAmong returns the member of ids that reports itself leader (0: none), for a partition
// where the minority still believes it leads.
func leaderAmong(c *kvtest.Cluster, ids []uint64) uint64 {
	for _, id := range ids {
		if m := c.Member(id); m != nil && m.Node.Status().IsLeader {
			return id
		}
	}
	return 0
}

// caughtUp waits until member id has applied as far as member ref had when the wait began; under
// a running load the leader keeps moving, so the target is fixed up front.
func caughtUp(c *kvtest.Cluster, id, ref uint64, d time.Duration) bool {
	want := c.Member(ref).Node.Status().Applied
	return testutil.Eventually(func() bool { return c.Member(id).Node.Status().Applied >= want }, d, 20*time.Millisecond)
}

func closeClient(cl *client.Client) { _ = cl.Close() } // tests only: nothing to report

// attemptPut tries one write with a short deadline, records it, and returns its error.
func (s *scenario) attemptPut(cl *client.Client, value string, d time.Duration) error {
	ctx, cancel := context.WithTimeout(s.t.Context(), d)
	defer cancel()
	key := s.prefix + "probe"
	call := s.rec.Now()
	kv, err := cl.Put(ctx, key, []byte(value))
	s.rec.Record(probeClient, porcupine.Input{Op: porcupine.OpPut, Key: key, Value: value}, call,
		writeOutput(err, &s.unknown, porcupine.Output{Version: kv.GetVersion()}))
	return err
}

func TestFault_01_KillLeader(t *testing.T) {
	s := newScenario(t, "1", "Kill leader", inProcess, "the leader's node stops abruptly (kvtest Kill: listener and loop gone, as SIGKILL)",
		"the other two elect a leader in a higher term; writes succeed again within 3 s; linearizable; all converge after the old leader restarts")
	c := prodCluster(t, kvtest.Options{})
	s.target("Raft failover", raftFailover)
	l := s.startLoad(c.Endpoints(), clientsPer)
	probe := s.client(c.Endpoints(), 50)
	defer closeClient(probe)
	s.run(time.Second)
	lead := c.WaitLeader(10 * time.Second)
	term := c.Member(lead).Node.Status().Term
	s.inject(func() { c.Kill(lead) })
	ttr := s.recover(probe, 15*time.Second)
	next := c.WaitLeader(10 * time.Second)
	if next == lead || c.Member(next).Node.Status().Term <= term {
		t.Fatalf("after killing leader %d (term %d): leader %d in term %d", lead, term, next, c.Member(next).Node.Status().Term)
	}
	s.observe("leader %d killed; %d leads in term %d; first write succeeded %.2f s after the kill", lead, next, c.Member(next).Node.Status().Term, ttr.Seconds())
	s.run(time.Second)
	c.Start(lead)
	l.stop()
	s.check()
	s.converged(c, c.IDs())
}

func TestFault_02_KillFollower(t *testing.T) {
	s := newScenario(t, "2", "Kill follower", inProcess, "one follower's node stops abruptly",
		"no leader change; writes keep succeeding (majority intact); linearizable; the follower converges after restart")
	c := prodCluster(t, kvtest.Options{})
	l := s.startLoad(c.Endpoints(), clientsPer)
	probe := s.client(c.Endpoints(), 50)
	defer closeClient(probe)
	s.run(time.Second)
	lead := c.WaitLeader(10 * time.Second)
	f := c.Others(lead)[0]
	s.inject(func() { c.Kill(f) })
	ttr := s.recover(probe, 5*time.Second)
	s.run(time.Second)
	if now := c.Leader(); now != lead {
		t.Fatalf("leader changed from %d to %d when follower %d stopped", lead, now, f)
	}
	s.observe("follower %d killed; leader %d unchanged; writes continued (first after the kill in %.3f s)", f, lead, ttr.Seconds())
	c.Start(f)
	l.stop()
	s.check()
	s.converged(c, c.IDs())
}

func TestFault_03_RestartFollower(t *testing.T) {
	s := newScenario(t, "3", "Restart follower", inProcess, "a follower is down while writes continue, then restarts over its own data",
		"writes continue meanwhile; the follower catches up from the leader's log; linearizable; converged")
	c := prodCluster(t, kvtest.Options{})
	l := s.startLoad(c.Endpoints(), clientsPer)
	probe := s.client(c.Endpoints(), 50)
	defer closeClient(probe)
	lead := c.WaitLeader(10 * time.Second)
	f := c.Others(lead)[0]
	behind := c.Member(f).Node.Status().Applied // before the kill: a stopped member has no Member
	c.Kill(f)
	s.run(2 * time.Second)
	s.inject(func() { c.Start(f) })
	s.recover(probe, 5*time.Second)
	start := s.clk.Now()
	if !caughtUp(c, f, lead, 15*time.Second) {
		t.Fatalf("follower %d did not catch up after its restart", f)
	}
	s.observe("follower %d restarted %d entries behind; caught up in %.2f s", f, c.Member(lead).Node.Status().Applied-behind, s.clk.Now().Sub(start).Seconds())
	l.stop()
	s.check()
	s.converged(c, c.IDs())
}

func TestFault_04_KillTwoNodes(t *testing.T) {
	s := newScenario(t, "4", "Kill two nodes", inProcess, "two of three nodes stop (quorum lost), then one restarts",
		"no write and no linearizable read succeeds without quorum; a serializable read is still served locally; once a second node is back, writes succeed again; linearizable; converged")
	c := prodCluster(t, kvtest.Options{})
	l := s.startLoad(c.Endpoints(), clientsPer)
	probe := s.client(c.Endpoints(), 50)
	defer closeClient(probe)
	s.run(time.Second)
	lead := c.WaitLeader(10 * time.Second)
	down := c.Others(lead)
	c.Kill(down[0])
	c.Kill(down[1])
	s.run(500 * time.Millisecond)
	if err := s.attemptPut(probe, "no-quorum", 2*time.Second); err == nil {
		t.Fatal("a write succeeded with two of three nodes down")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	_, err := probe.Get(ctx, s.prefix+"k0")
	cancel()
	if err == nil {
		t.Fatal("a linearizable read succeeded without quorum")
	}
	local := s.client([]string{c.Member(lead).Addr}, 51)
	defer closeClient(local)
	ctx, cancel = context.WithTimeout(t.Context(), 2*time.Second)
	_, serr := local.Get(ctx, s.prefix+"k0", client.Serializable())
	cancel()
	if serr != nil && !errors.Is(serr, client.ErrNotFound) {
		t.Fatalf("serializable read on the surviving node: %v", serr)
	}
	s.observe("with nodes %v down: write rejected, linearizable read rejected (%v), serializable read served locally", down, err)
	s.inject(func() { c.Start(down[0]) })
	ttr := s.recover(probe, 15*time.Second)
	s.observe("node %d restarted: writes succeeded %.2f s later", down[0], ttr.Seconds())
	c.Start(down[1])
	l.stop()
	s.check()
	s.converged(c, c.IDs())
}

func TestFault_05_StaleDataRestore(t *testing.T) {
	s := newScenario(t, "5", "Restore stale data dir", inProcess,
		"a follower's data is replaced by an older copy of its own (taken earlier while stopped) and it restarts; the other two hold every committed entry",
		"safety holds: no divergence, linearizable; the node rejoins behind and catches up (under the same leader, or after a leadership transfer: raft never lowers a peer's acknowledged progress)")
	c := prodCluster(t, kvtest.Options{})
	l := s.startLoad(c.Endpoints(), clientsPer)
	probe := s.client(c.Endpoints(), 50)
	defer closeClient(probe)
	lead := c.WaitLeader(10 * time.Second)
	f := c.Others(lead)[0]
	c.Kill(f)
	old := c.Backup(f)
	oldLast, _ := old.LastIndex() // Mem never fails
	c.Start(f)
	s.run(2 * time.Second)
	c.Kill(f)
	s.inject(func() {
		c.Restore(f, old)
		c.Start(f)
	})
	s.recover(probe, 5*time.Second)
	lead = c.WaitLeader(10 * time.Second)
	how := "caught up under the same leader"
	if !caughtUp(c, f, lead, 3*time.Second) {
		other := c.Others(lead, f)[0]
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		err := c.Member(lead).Node.TransferLeadership(ctx, other)
		cancel()
		if err != nil {
			t.Fatalf("transfer leadership: %v", err)
		}
		lead = c.WaitLeader(10 * time.Second)
		how = fmt.Sprintf("stayed behind under the old leader (its acknowledged progress was kept); caught up after leadership moved to %d", lead)
	}
	if !caughtUp(c, f, lead, 20*time.Second) {
		t.Fatalf("node %d restored from an old copy (last index %d) never caught up", f, oldLast)
	}
	s.observe("node %d restarted from a copy ending at index %d: %s", f, oldLast, how)
	l.stop()
	s.check()
	s.converged(c, c.IDs())
}

func TestFault_06_NetworkDelay(t *testing.T) {
	s := newScenario(t, "6", "Network delay", inProcess,
		"60% of peer messages held 1–3 ticks (up to 300 ms latency spikes, below the 1 s election timeout), via transport.Fault",
		"writes keep succeeding, slower; elections stay stable; linearizable; converged once the delay ends")
	c := prodCluster(t, kvtest.Options{Faults: true, FaultSeed: seed})
	l := s.startLoad(c.Endpoints(), clientsPer)
	probe := s.client(c.Endpoints(), 50)
	defer closeClient(probe)
	s.run(time.Second)
	lead := c.WaitLeader(10 * time.Second)
	term := c.Member(lead).Node.Status().Term
	s.inject(func() { c.SetFaults(transport.Faults{Delay: 0.6, MaxDelayTicks: 3}) })
	ttr := s.recover(probe, 10*time.Second)
	var lat []time.Duration
	for i := range 20 {
		start := s.clk.Now()
		if err := s.attemptPut(probe, fmt.Sprintf("delayed-%d", i), 5*time.Second); err != nil {
			t.Fatalf("write %d under delay: %v", i, err)
		}
		lat = append(lat, s.clk.Now().Sub(start))
	}
	slices.Sort(lat)
	s.run(3 * time.Second)
	now := c.WaitLeader(10 * time.Second)
	changes := c.Member(now).Node.Status().Term - term
	c.SetFaults(transport.Faults{})
	if changes != 0 || now != lead {
		t.Fatalf("delays below the election timeout changed the leader: %d → %d, %d term change(s)", lead, now, changes)
	}
	s.observe("under delay: first write %.2f s after it began, sequential write latency p50 %s / max %s; leader %d → %d, %d term change(s)",
		ttr.Seconds(), lat[len(lat)/2].Round(time.Millisecond), lat[len(lat)-1].Round(time.Millisecond), lead, now, changes)
	l.stop()
	s.check()
	s.converged(c, c.IDs())
}

func TestFault_07_Partition(t *testing.T) {
	s := newScenario(t, "7", "Network partition 1|2", inProcess, "the leader is cut off from both followers (transport.Fault partition)",
		"the majority elects a leader and keeps accepting writes within 3 s; writes sent to the minority leader fail; after healing, the old leader follows and all converge; linearizable")
	c := prodCluster(t, kvtest.Options{Faults: true, FaultSeed: seed})
	s.target("Raft failover", raftFailover)
	l := s.startLoad(c.Endpoints(), clientsPer)
	probe := s.client(c.Endpoints(), 50)
	defer closeClient(probe)
	s.run(time.Second)
	lead := c.WaitLeader(10 * time.Second)
	majority := c.Others(lead)
	minority := s.client([]string{c.Member(lead).Addr}, 52)
	defer closeClient(minority)
	s.inject(func() { c.Partition([]uint64{lead}, majority) })
	// Right after the cut the isolated node still believes it leads (CheckQuorum steps it down
	// after about one election timeout, 1 s): a write through it must not be acknowledged.
	if leaderAmong(c, []uint64{lead}) != lead {
		t.Fatalf("node %d stopped leading before the minority write could be tried", lead)
	}
	// Concurrently with the recovery probes, so the measurement doesn't include this attempt;
	// attemptPut never calls t.Fatal, so it may run off the test goroutine.
	minorityErr := make(chan error, 1)
	go func() { minorityErr <- s.attemptPut(minority, "minority", 800*time.Millisecond) }()
	ttr := s.recover(probe, 15*time.Second)
	if err := <-minorityErr; err == nil {
		t.Fatal("a write through the isolated leader was acknowledged")
	}
	var next uint64
	if !testutil.Eventually(func() bool { next = leaderAmong(c, majority); return next != 0 }, 10*time.Second, 20*time.Millisecond) {
		t.Fatal("the majority elected no leader")
	}
	s.observe("leader %d isolated: a write through it while it still led was not acknowledged; majority elected %d; writes succeeded %.2f s after the cut", lead, next, ttr.Seconds())
	c.Heal()
	if !testutil.Eventually(func() bool { return c.Leader() != 0 }, 10*time.Second, 20*time.Millisecond) {
		t.Fatal("no agreed leader after healing")
	}
	s.run(time.Second)
	l.stop()
	s.check()
	s.converged(c, c.IDs())
}

func TestFault_08_RestartAllNodes(t *testing.T) {
	s := newScenario(t, "8", "Restart all nodes", inProcess, "every node stops abruptly, then all restart over their own data",
		"state is preserved: a value written before is read back; writes succeed again; linearizable; converged")
	c := prodCluster(t, kvtest.Options{})
	l := s.startLoad(c.Endpoints(), clientsPer)
	probe := s.client(c.Endpoints(), 50)
	defer closeClient(probe)
	s.run(time.Second)
	marker := s.prefix + "marker"
	if err := s.putMarker(probe, marker, "before-restart"); err != nil {
		t.Fatal(err)
	}
	s.inject(func() {
		for _, id := range c.IDs() {
			c.Kill(id)
		}
		for _, id := range c.IDs() {
			c.Start(id)
		}
	})
	ttr := s.recover(probe, 15*time.Second)
	got := s.readBack(probe, marker)
	if got != "before-restart" {
		t.Fatalf("marker after restarting every node = %q", got)
	}
	s.observe("all three restarted; writes succeeded %.2f s after the stop; the marker written before reads back", ttr.Seconds())
	l.stop()
	s.check()
	s.converged(c, c.IDs())
}

// putMarker writes key=value linearizably and records it.
func (s *scenario) putMarker(cl *client.Client, key, value string) error {
	ctx, cancel := context.WithTimeout(s.t.Context(), 5*time.Second)
	defer cancel()
	call := s.rec.Now()
	kv, err := cl.Put(ctx, key, []byte(value))
	s.rec.Record(probeClient+1, porcupine.Input{Op: porcupine.OpPut, Key: key, Value: value}, call,
		writeOutput(err, &s.unknown, porcupine.Output{Version: kv.GetVersion()}))
	return err
}

// readBack reads key linearizably, records the read, and returns its value.
func (s *scenario) readBack(cl *client.Client, key string) string {
	s.t.Helper()
	ctx, cancel := context.WithTimeout(s.t.Context(), 5*time.Second)
	defer cancel()
	call := s.rec.Now()
	res, err := cl.Get(ctx, key)
	if err != nil {
		s.t.Fatalf("read %s: %v", key, err)
	}
	kv := res.GetKv()
	s.rec.Record(probeClient+1, porcupine.Input{Op: porcupine.OpGet, Key: key}, call,
		porcupine.Output{Found: true, Value: string(kv.GetValue()), Version: kv.GetVersion()})
	return string(kv.GetValue())
}

func TestFault_09_WritesWithoutQuorum(t *testing.T) {
	s := newScenario(t, "9", "Writes without quorum", inProcess, "every node is cut off from the others (three-way partition) while clients keep writing; then the partition heals",
		"no write is acknowledged while no majority can talk; the leader steps down (CheckQuorum); after healing, writes succeed; writes that timed out are recorded as unknown (they may apply later); linearizable")
	c := prodCluster(t, kvtest.Options{Faults: true, FaultSeed: seed})
	l := s.startLoad(c.Endpoints(), clientsPer)
	probe := s.client(c.Endpoints(), 50)
	defer closeClient(probe)
	s.run(time.Second)
	ids := c.IDs()
	c.Partition([]uint64{ids[0]}, []uint64{ids[1]}, []uint64{ids[2]})
	for i := range 3 {
		if err := s.attemptPut(probe, fmt.Sprintf("isolated-%d", i), time.Second); err == nil {
			t.Fatalf("write %d succeeded with every node isolated", i)
		}
	}
	if !testutil.Eventually(func() bool { return leaderAmong(c, ids) == 0 }, 5*time.Second, 20*time.Millisecond) {
		t.Fatal("a node still claims leadership with no quorum (CheckQuorum)")
	}
	s.observe("three-way partition: 3 writes rejected; no node claims leadership")
	s.inject(c.Heal)
	ttr := s.recover(probe, 15*time.Second)
	s.observe("healed: writes succeeded %.2f s later", ttr.Seconds())
	l.stop()
	s.check()
	s.converged(c, c.IDs())
}

func TestFault_10_ConsistencyAfterRecovery(t *testing.T) {
	s := newScenario(t, "10", "State-machine consistency after recovery", inProcess,
		"6 s of seeded chaos (random node kills and restarts, partitions and heals) under load, then everything heals",
		"every node reaches the same state hash at the same applied index; linearizable")
	c := prodCluster(t, kvtest.Options{Faults: true, FaultSeed: seed})
	l := s.startLoad(c.Endpoints(), clientsPer)
	probe := s.client(c.Endpoints(), 50)
	defer closeClient(probe)
	rng := rand.New(rand.NewPCG(seed, 10)) //nolint:gosec // G404: seeded chaos schedule
	ids := c.IDs()
	var events []string
	for end := s.clk.Now().Add(6 * time.Second); s.clk.Now().Before(end); {
		switch id := ids[rng.IntN(len(ids))]; rng.IntN(3) {
		case 0:
			c.Kill(id)
			events = append(events, fmt.Sprintf("kill %d", id))
			s.run(400 * time.Millisecond)
			c.Start(id)
		case 1:
			c.Partition([]uint64{id}, c.Others(id))
			events = append(events, fmt.Sprintf("isolate %d", id))
			s.run(700 * time.Millisecond)
			c.Heal()
		default:
			s.run(300 * time.Millisecond)
		}
	}
	s.inject(func() {
		c.Heal()
		for _, id := range ids {
			c.Start(id)
		}
	})
	ttr := s.recover(probe, 15*time.Second)
	s.observe("chaos: %v; writes succeeded %.2f s after everything healed", events, ttr.Seconds())
	l.stop()
	s.check()
	s.converged(c, ids)
}

func TestFault_11_SnapshotRestoreOfWipedNode(t *testing.T) {
	s := newScenario(t, "11", "Snapshot restore of a wiped node", inProcess,
		"a follower's data is deleted (snapshot every 50 entries, 10 kept) and it restarts empty; leadership moves (the wipe procedure)",
		"the node installs the leader's snapshot and catches up; state hashes equal; linearizable")
	c := prodCluster(t, kvtest.Options{SnapshotEntries: 50, SnapshotTrailing: 10})
	l := s.startLoad(c.Endpoints(), clientsPer)
	probe := s.client(c.Endpoints(), 50)
	defer closeClient(probe)
	lead := c.WaitLeader(10 * time.Second)
	if !testutil.Eventually(func() bool { return c.Member(lead).Node.Status().FirstIndex > 1 }, 20*time.Second, 50*time.Millisecond) {
		t.Fatal("the leader's log was never compacted")
	}
	f := c.Others(lead)[0]
	c.Kill(f)
	c.Wipe(f)
	s.inject(func() { c.Start(f) })
	s.recover(probe, 5*time.Second)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	err := c.Member(lead).Node.TransferLeadership(ctx, c.Others(lead, f)[0])
	cancel()
	if err != nil {
		t.Fatalf("transfer leadership: %v", err)
	}
	next := c.WaitLeader(10 * time.Second)
	if !caughtUp(c, f, next, 30*time.Second) || c.Member(f).Node.Status().SnapshotIndex == 0 {
		t.Fatalf("wiped node %d did not catch up through a snapshot: %+v", f, c.Member(f).Node.Status())
	}
	s.observe("node %d wiped; after the transfer to %d it installed a snapshot at index %d and caught up %.2f s after its restart",
		f, next, c.Member(f).Node.Status().SnapshotIndex, s.clk.Now().Sub(s.faultAt).Seconds())
	l.stop()
	s.check()
	s.converged(c, c.IDs())
}

func TestFault_X1_RollingRestartUnderLoad(t *testing.T) {
	s := newScenario(t, "X1", "Rolling restart under load", inProcess, "each node in turn is killed and restarted while clients write",
		"writes resume within the Raft failover target after each restart; no data loss; linearizable; converged")
	c := prodCluster(t, kvtest.Options{})
	s.target("Raft failover", raftFailover)
	l := s.startLoad(c.Endpoints(), clientsPer)
	probe := s.client(c.Endpoints(), 50)
	defer closeClient(probe)
	var worst time.Duration
	for _, id := range c.IDs() {
		s.inject(func() { c.Kill(id) })
		worst = max(worst, s.recover(probe, 15*time.Second))
		c.Start(id)
		ref := c.WaitLeader(10 * time.Second)
		if !caughtUp(c, id, ref, 15*time.Second) {
			t.Fatalf("node %d did not catch up after its restart", id)
		}
	}
	secs := worst.Seconds()
	s.row.TTRSeconds = &secs
	s.observe("restarted nodes %v one by one; slowest recovery %.2f s", c.IDs(), secs)
	l.stop()
	s.check()
	s.converged(c, c.IDs())
}

func TestFault_X2_LeaderTransferUnderLoad(t *testing.T) {
	s := newScenario(t, "X2", "Leader transfer under load", inProcess, "leadership is transferred 5 times while clients write",
		"each transfer completes; writes resume within the Raft failover target; linearizable; converged")
	c := prodCluster(t, kvtest.Options{})
	s.target("Raft failover", raftFailover)
	l := s.startLoad(c.Endpoints(), clientsPer)
	probe := s.client(c.Endpoints(), 50)
	defer closeClient(probe)
	var worst time.Duration
	for i := range 5 {
		lead := c.WaitLeader(10 * time.Second)
		to := c.Others(lead)[i%2]
		s.inject(func() {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			if err := c.Member(lead).Node.TransferLeadership(ctx, to); err != nil {
				t.Fatalf("transfer %d → %d: %v", lead, to, err)
			}
		})
		worst = max(worst, s.recover(probe, 10*time.Second))
		s.run(300 * time.Millisecond)
	}
	secs := worst.Seconds()
	s.row.TTRSeconds = &secs
	s.observe("5 transfers; slowest write recovery %.2f s", secs)
	l.stop()
	s.check()
	s.converged(c, c.IDs())
}

// slowStorage delays every Save (a persisted Ready: the node's fsync) by delay nanoseconds.
type slowStorage struct {
	storage.Storage
	delay *atomic.Int64
}

func (s slowStorage) Save(hs *pb.HardState, ents []*pb.Entry, snap *pb.Snapshot) error {
	if d := time.Duration(s.delay.Load()); d > 0 {
		<-time.After(d) //nolint:forbidigo // the injected fault is wall-clock disk latency
	}
	return s.Storage.Save(hs, ents, snap)
}

func TestFault_X4_SlowDisk(t *testing.T) {
	s := newScenario(t, "X4", "Slow disk", inProcess, "every node's fsync (storage Save) delayed by 40 ms",
		"elections stay stable or recover; writes keep succeeding, slower; linearizable; converged")
	var delay atomic.Int64
	c := prodCluster(t, kvtest.Options{WrapStorage: func(_ uint64, st storage.Storage) storage.Storage {
		return slowStorage{Storage: st, delay: &delay}
	}})
	l := s.startLoad(c.Endpoints(), clientsPer)
	probe := s.client(c.Endpoints(), 50)
	defer closeClient(probe)
	s.run(time.Second)
	lead := c.WaitLeader(10 * time.Second)
	term := c.Member(lead).Node.Status().Term
	s.inject(func() { delay.Store(int64(40 * time.Millisecond)) })
	ttr := s.recover(probe, 10*time.Second)
	s.run(4 * time.Second)
	now := c.WaitLeader(10 * time.Second)
	changes := c.Member(now).Node.Status().Term - term
	delay.Store(0)
	s.observe("40 ms fsync: writes succeeded %.2f s after it began; leader %d → %d, %d term change(s)", ttr.Seconds(), lead, now, changes)
	l.stop()
	s.check()
	s.converged(c, c.IDs())
}

func TestFault_X5_LargeSnapshotInstallUnderLoad(t *testing.T) {
	s := newScenario(t, "X5", "Large snapshot install under load", inProcess,
		"about 12 MiB of state (1,500 × 8 KiB values), then a follower is wiped and catches up through a multi-chunk snapshot while clients write",
		"the snapshot installs (1 MiB chunks) without stalling writes; state hashes equal; linearizable")
	c := prodCluster(t, kvtest.Options{SnapshotEntries: 200, SnapshotTrailing: 20})
	probe := s.client(c.Endpoints(), 50)
	defer closeClient(probe)
	big := make([]byte, 8<<10)
	for i := range big {
		big[i] = byte('a' + i%26)
	}
	for i := range 1500 {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		_, err := probe.Put(ctx, fmt.Sprintf("%sbulk/%04d", s.prefix, i), big)
		cancel()
		if err != nil {
			t.Fatalf("bulk write %d: %v", i, err)
		}
	}
	l := s.startLoad(c.Endpoints(), clientsPer)
	lead := c.WaitLeader(10 * time.Second)
	f := c.Others(lead)[0]
	c.Kill(f)
	c.Wipe(f)
	s.inject(func() { c.Start(f) })
	s.recover(probe, 5*time.Second)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	err := c.Member(lead).Node.TransferLeadership(ctx, c.Others(lead, f)[0])
	cancel()
	if err != nil {
		t.Fatalf("transfer leadership: %v", err)
	}
	next := c.WaitLeader(10 * time.Second)
	if !caughtUp(c, f, next, 60*time.Second) {
		t.Fatalf("node %d did not catch up through the large snapshot", f)
	}
	if first, snap := c.Member(next).Node.Status().FirstIndex, c.Member(f).Node.Status().SnapshotIndex; first <= 1 || snap == 0 {
		t.Fatalf("no snapshot install: leader's first index %d, follower's snapshot index %d", first, snap)
	}
	s.observe("about 12 MiB of state; wiped node %d installed snapshot at index %d and caught up %.2f s after its restart, while writes continued",
		f, c.Member(f).Node.Status().SnapshotIndex, s.clk.Now().Sub(s.faultAt).Seconds())
	l.stop()
	s.check()
	s.converged(c, c.IDs())
}

// TestFault_NegativeControl proves the checker can fail (task 04.08): a broken client that takes
// a serializable read from a partitioned, stale follower and reports it as linearizable produces
// a history Porcupine must flag.
func TestFault_NegativeControl(t *testing.T) {
	s := newScenario(t, "N", "Negative control: stale read reported as linearizable", inProcess,
		"a deliberately broken client: after v2 is acknowledged, it reads the key from a partitioned follower with SERIALIZABLE consistency and records it as a linearizable read",
		"Porcupine flags the history as not linearizable (the checker works)")
	c := prodCluster(t, kvtest.Options{Faults: true, FaultSeed: seed})
	cl := s.client(c.Endpoints(), 50)
	defer closeClient(cl)
	lead := c.WaitLeader(10 * time.Second)
	f := c.Others(lead)[0]
	key := s.prefix + "stale"
	rec := porcupine.NewRecorder()
	put := func(value string) {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		call := rec.Now()
		kv, err := cl.Put(ctx, key, []byte(value))
		if err != nil {
			t.Fatalf("put %s: %v", value, err)
		}
		rec.Record(0, porcupine.Input{Op: porcupine.OpPut, Key: key, Value: value}, call, porcupine.Output{Version: kv.GetVersion()})
	}
	put("v1")
	if !caughtUp(c, f, lead, 10*time.Second) {
		t.Fatal("the follower never applied v1")
	}
	c.Partition([]uint64{lead, c.Others(lead, f)[0]}, []uint64{f})
	put("v2")
	stale := s.client([]string{c.Member(f).Addr}, 53)
	defer closeClient(stale)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	call := rec.Now()
	res, err := stale.Get(ctx, key, client.Serializable())
	cancel()
	if err != nil || string(res.GetKv().GetValue()) != "v1" {
		t.Fatalf("serializable read on the partitioned follower = %v, %v; the control needs the stale v1", res, err)
	}
	kv := res.GetKv()
	rec.Record(1, porcupine.Input{Op: porcupine.OpGet, Key: key}, call, porcupine.Output{Found: true, Value: string(kv.GetValue()), Version: kv.GetVersion()})
	c.Heal()
	out, _ := porcupine.Check(rec.Operations(), t.TempDir())
	s.checked = true
	s.row.Ops = len(rec.Operations())
	if out != pc.Illegal {
		t.Fatalf("Porcupine judged the stale-read history %s; the checker does not detect violations", out)
	}
	s.observe("stale read of v1 after v2 was acknowledged: Porcupine judged the history %s", out)
}
