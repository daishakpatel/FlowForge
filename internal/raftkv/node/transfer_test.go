package node_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/daishakpatel/flowforge/internal/raftkv/node"
	"github.com/daishakpatel/flowforge/internal/raftkv/nodetest"
)

// transfer runs TransferLeadership on id while the test goroutine keeps ticking the cluster
// (the transfer needs heartbeats and a MsgTimeoutNow round), and returns its error. The call
// polls the node's status on the real clock, so ticking continues until it returns; ctx(t)
// bounds it.
func transfer(t *testing.T, c *nodetest.Cluster, id, to uint64) error {
	t.Helper()
	return tickWhile(c, func() error { return c.Node(id).TransferLeadership(ctx(t), to) })
}

// tickWhile ticks the cluster until f returns.
func tickWhile(c *nodetest.Cluster, f func() error) error {
	done := make(chan error, 1)
	go func() { done <- f() }()
	for {
		select {
		case err := <-done:
			return err
		default:
			c.Tick()
		}
	}
}

func TestTransferLeadership_HandsOffToAnotherVoter(t *testing.T) {
	forSeeds(t, func(t *testing.T, seed int64) {
		c := nodetest.New(t, nodetest.Options{Seed: seed})
		old := c.WaitLeader(electionTimeouts)
		mustPropose(t, c, old, "before")
		converge(t, c, electionTimeouts)
		if err := transfer(t, c, old, 0); err != nil {
			t.Fatalf("TransferLeadership: %v", err)
		}
		lead := c.WaitLeader(electionTimeouts)
		if lead == old {
			t.Fatalf("leader is still %d", old)
		}
		mustPropose(t, c, lead, "after")
		converge(t, c, electionTimeouts)
	})
}

func TestTransferLeadership_ToNamedVoter(t *testing.T) {
	c := nodetest.New(t, nodetest.Options{Seed: 1})
	old := c.WaitLeader(electionTimeouts)
	converge(t, c, electionTimeouts)
	to := c.Others(old)[1]
	if err := transfer(t, c, old, to); err != nil {
		t.Fatalf("TransferLeadership: %v", err)
	}
	if lead := c.WaitLeader(electionTimeouts); lead != to {
		t.Fatalf("leader = %d, want %d", lead, to)
	}
}

func TestTransferLeadership_RefusedOffTheLeader(t *testing.T) {
	c := nodetest.New(t, nodetest.Options{Seed: 1})
	lead := c.WaitLeader(electionTimeouts)
	follower := c.Others(lead)[0]
	if err := c.Node(follower).TransferLeadership(ctx(t), 0); !errors.Is(err, node.ErrNotLeader) {
		t.Fatalf("follower TransferLeadership = %v, want ErrNotLeader", err)
	}
}

// TestTransferLeadership_FailsFastWithNoReachableFollower: with every follower cut off no transfer
// can finish, so the call returns within about an election timeout (nil once CheckQuorum steps
// the leader down) instead of waiting out its deadline: a whole cluster stopping together would
// otherwise wait on every leader.
func TestTransferLeadership_FailsFastWithNoReachableFollower(t *testing.T) {
	c := nodetest.New(t, nodetest.Options{Seed: 1})
	lead := c.WaitLeader(electionTimeouts)
	converge(t, c, electionTimeouts)
	c.Fault.Partition([]uint64{lead}, c.Others(lead))
	for range 5 { // under an election timeout: still leader, but no follower recently active
		c.Tick()
	}
	if !c.TickUntil(func() bool { return c.Node(lead).Status().IsLeader }, 1) {
		t.Skip("the leader already stepped down") // CheckQuorum ran first: nothing left to test
	}
	tctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := tickWhile(c, func() error { return c.Node(lead).TransferLeadership(tctx, 0) })
	if (err != nil && !errors.Is(err, node.ErrNoQuorum)) || tctx.Err() != nil {
		t.Fatalf("TransferLeadership = %v (deadline: %v), want nil or ErrNoQuorum well before the deadline", err, tctx.Err())
	}
	if c.Node(lead).Status().IsLeader {
		t.Fatal("still leader")
	}
}

// TestTransferLeadership_SkipsAnUnreachableFollower: on an idle cluster every follower has the same
// match index; the handoff must pick a follower the leader hears from, not the lowest id when that
// one is cut off (code review, 04.04).
func TestTransferLeadership_SkipsAnUnreachableFollower(t *testing.T) {
	forSeeds(t, func(t *testing.T, seed int64) {
		c := nodetest.New(t, nodetest.Options{Seed: seed})
		old := c.WaitLeader(electionTimeouts)
		converge(t, c, electionTimeouts)
		others := c.Others(old)
		dead, alive := others[0], others[1] // the tie-break would pick others[0], the lower id
		c.Fault.Partition([]uint64{dead}, []uint64{old, alive})
		for range 3 * 10 { // a few election timeouts: CheckQuorum marks dead inactive
			c.Tick()
		}
		if err := transfer(t, c, old, 0); err != nil {
			t.Fatalf("TransferLeadership: %v", err)
		}
		if lead := c.Node(alive).Status(); !lead.IsLeader {
			t.Fatalf("leadership went to %d, want the reachable follower %d", lead.Leader, alive)
		}
	})
}

func TestTransferLeadership_RejectsANonVoter(t *testing.T) {
	c := nodetest.New(t, nodetest.Options{Seed: 1})
	lead := c.WaitLeader(electionTimeouts)
	if err := c.Node(lead).TransferLeadership(ctx(t), 7); !errors.Is(err, node.ErrInvalidConfig) {
		t.Fatalf("TransferLeadership to a non-voter = %v, want ErrInvalidConfig", err)
	}
}
