package node_test

import (
	"fmt"
	"testing"

	"github.com/daishakpatel/flowforge/internal/raftkv/nodetest"
	"github.com/daishakpatel/flowforge/internal/raftkv/storage"
)

// TestNode_SnapshotNowAndCompactNow: an operator snapshot covers the applied index; compacting then
// drops the whole log up to it (task 04.06's snapshot-now and compact).
func TestNode_SnapshotNowAndCompactNow(t *testing.T) {
	c := nodetest.New(t, nodetest.Options{Seed: 1})
	lead := c.WaitLeader(electionTimeouts)
	for i := range 20 {
		mustPropose(t, c, lead, fmt.Sprint("e", i))
	}
	converge(t, c, electionTimeouts)
	n := c.Node(lead)
	applied := n.Status().Applied
	idx, err := tickWhileValue(c, func() (uint64, error) { return n.SnapshotNow(ctx(t)) })
	if err != nil || idx != applied {
		t.Fatalf("SnapshotNow = %d, %v; want %d", idx, err, applied)
	}
	c.Tick()
	if st := n.Status(); st.SnapshotIndex != idx || st.FirstIndex > idx {
		t.Fatalf("after SnapshotNow: snapshot %d, first %d (the default trailing keeps the log)", st.SnapshotIndex, st.FirstIndex)
	}
	first, err := tickWhileValue(c, func() (uint64, error) { return n.CompactNow(ctx(t)) })
	if err != nil || first != idx+1 {
		t.Fatalf("CompactNow = %d, %v; want first index %d", first, err, idx+1)
	}
	c.Tick()
	if st := n.Status(); st.FirstIndex != idx+1 {
		t.Fatalf("Status.FirstIndex = %d, want %d", st.FirstIndex, idx+1)
	}
	if again, err := tickWhileValue(c, func() (uint64, error) { return n.CompactNow(ctx(t)) }); err != nil || again != first {
		t.Fatalf("second CompactNow = %d, %v", again, err)
	}
	// The cluster keeps working after its leader dropped its whole log.
	mustPropose(t, c, lead, "after")
	converge(t, c, electionTimeouts)
}

// TestNode_AutomaticSnapshotCompactsTheLog: with SnapshotEntries 50 and 10 trailing entries, 500
// writes leave a snapshot near the head and a log compacted behind it.
func TestNode_AutomaticSnapshotCompactsTheLog(t *testing.T) {
	c := nodetest.New(t, nodetest.Options{Seed: 1, SnapshotEntries: 50, SnapshotTrailing: 10})
	lead := c.WaitLeader(electionTimeouts)
	for i := range 500 {
		mustPropose(t, c, lead, fmt.Sprint("e", i))
	}
	converge(t, c, electionTimeouts)
	for _, s := range c.Statuses() {
		if s.SnapshotIndex < s.Applied-50 || s.FirstIndex <= 1 || s.FirstIndex > s.SnapshotIndex-10+1 {
			t.Fatalf("node %d: applied %d, snapshot %d, first %d", s.ID, s.Applied, s.SnapshotIndex, s.FirstIndex)
		}
	}
}

func TestNode_StatusReportsStateAndPeerProgress(t *testing.T) {
	c := nodetest.New(t, nodetest.Options{Seed: 1})
	lead := c.WaitLeader(electionTimeouts)
	mustPropose(t, c, lead, "x")
	converge(t, c, electionTimeouts)
	c.TickUntil(func() bool {
		for _, p := range c.Node(lead).Status().Peers {
			if p.Match != c.Node(lead).Status().Commit || !p.Reachable {
				return false
			}
		}
		return true
	}, electionTimeouts)
	st := c.Node(lead).Status()
	if st.State != "LEADER" || len(st.Peers) != 3 {
		t.Fatalf("leader status = %+v", st)
	}
	for _, p := range st.Peers {
		if p.Match != st.Commit || !p.Reachable || p.Addr == "" {
			t.Fatalf("peer %+v, commit %d", p, st.Commit)
		}
	}
	if f := c.Node(c.Others(lead)[0]).Status(); f.State != "FOLLOWER" || len(f.Peers) != 0 {
		t.Fatalf("follower status = %+v", f)
	}
}

// TestNode_WipedFollowerSurvivesAndCatchesUp: a follower restarted with empty storage gets the
// leader's heartbeat with a commit index past its log (the leader still has its old match
// index). raft panics on that unless the node caps it; the follower must stay up and recover
// through the leader's snapshot (04.06).
func TestNode_WipedFollowerSurvivesAndCatchesUp(t *testing.T) {
	stores := map[uint64]storage.Storage{}
	wiped := map[uint64]bool{}
	c := nodetest.New(t, nodetest.Options{Seed: 1, SnapshotEntries: 50, SnapshotTrailing: 10, Storage: func(id uint64) (storage.Storage, error) {
		if stores[id] == nil || wiped[id] {
			stores[id], wiped[id] = storage.NewMem(), false
		}
		return stores[id], nil
	}})
	lead := c.WaitLeader(electionTimeouts)
	for i := range 200 {
		mustPropose(t, c, lead, fmt.Sprint("e", i))
	}
	converge(t, c, electionTimeouts)
	f := c.Others(lead)[0]
	c.Stop(f)
	wiped[f] = true
	c.Start(f)
	// The wipe procedure (docs/runbooks/raft-quorum-loss.md): move leadership. raft never lowers a
	// match index, so only a new leader, whose progress starts afresh, finds the gap and sends a
	// snapshot.
	other := c.Others(lead, f)[0]
	if err := transfer(t, c, lead, other); err != nil {
		t.Fatalf("transfer: %v", err)
	}
	mustPropose(t, c, c.WaitLeader(electionTimeouts), "after-wipe")
	converge(t, c, 5*electionTimeouts)
	if err := c.Node(f).Err(); err != nil {
		t.Fatalf("wiped follower stopped: %v", err)
	}
	if st := c.Node(f).Status(); st.SnapshotIndex < 150 {
		t.Fatalf("wiped follower did not install the leader's snapshot: %+v", st)
	}
}

// tickWhileValue ticks the cluster until f returns.
func tickWhileValue(c *nodetest.Cluster, f func() (uint64, error)) (uint64, error) {
	var v uint64
	err := tickWhile(c, func() error {
		var err error
		v, err = f()
		return err
	})
	return v, err
}
