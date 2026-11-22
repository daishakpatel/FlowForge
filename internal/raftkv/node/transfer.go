package node

import (
	"context"
	"errors"
	"fmt"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/tracker"
)

// TransferLeadership asks this node, the leader, to hand leadership to voter to (0: the reachable
// follower with the most replicated log) and waits until this node no longer leads, or ctx ends
// (task 04.04: flowforge-raft's graceful shutdown). It is best effort: raft gives up on a
// transfer after an election timeout, so the request is renewed until ctx ends; a node that stays
// leader returns ctx's error. Losing leadership another way also returns nil: with no follower
// reachable, CheckQuorum steps the leader down within an election timeout, so a whole cluster
// stopping together doesn't wait out ctx. It fails with ErrInvalidConfig when to is not another
// voter. A follower returns its NotLeaderError (or ErrNoQuorum). While a transfer
// is pending raft drops proposals (ErrProposalDropped), which clients retry on the new leader.
func (n *Node) TransferLeadership(ctx context.Context, to uint64) error {
	if err := n.requestTransfer(ctx, to); err != nil {
		return err
	}
	poll := n.cfg.Clock.NewTicker(n.cfg.TickInterval)
	defer poll.Stop()
	for {
		select {
		case <-poll.C():
		case <-ctx.Done():
			return fmt.Errorf("transfer leadership: %w", ctx.Err())
		case <-n.donec:
			return ErrNodeStopped
		}
		if !n.Status().IsLeader {
			return nil
		}
		// Still leader: raft may have abandoned the transfer. A not-leader answer here means
		// leadership moved between the status read and the loop, which the next poll sees.
		if err := n.requestTransfer(ctx, to); err != nil && !errors.Is(err, ErrNotLeader) && !errors.Is(err, ErrNoQuorum) {
			return err
		}
	}
}

func (n *Node) requestTransfer(ctx context.Context, to uint64) error {
	x := transferReq{to: to, done: make(chan error, 1)}
	select {
	case n.xferc <- x:
	case <-ctx.Done():
		return fmt.Errorf("transfer leadership: %w", ctx.Err())
	case <-n.donec:
		return ErrNodeStopped
	}
	select {
	case err := <-x.done:
		return err
	case <-ctx.Done():
		return fmt.Errorf("transfer leadership: %w", ctx.Err())
	case <-n.donec:
		return ErrNodeStopped
	}
}

type transferReq struct {
	to   uint64
	done chan error // buffered 1
}

// transfer runs on the loop. A transfer already pending is left to finish. With to == 0 it picks
// the follower with the highest match index, preferring followers raft heard from recently
// (RecentActive): on an idle cluster every match is equal, and picking by id alone could choose
// a dead follower on every renewal. RecentActive is cleared for everyone at each CheckQuorum
// until the followers answer again, so with none active it falls back to all followers.
func (n *Node) transfer(to uint64) error {
	st := n.rn.Status()
	if st.RaftState != raft.StateLeader {
		return n.notLeader(st.Lead)
	}
	if st.LeadTransferee != raft.None {
		return nil
	}
	if to != 0 {
		if _, ok := st.Progress[to]; !ok || to == n.cfg.ID {
			return fmt.Errorf("%w: %d is not another voter", ErrInvalidConfig, to)
		}
	}
	if to == 0 {
		to = bestFollower(st.Progress, n.cfg.ID, true)
	}
	if to == 0 {
		to = bestFollower(st.Progress, n.cfg.ID, false)
	}
	n.rn.TransferLeader(to)
	return nil
}

// bestFollower returns the follower with the highest match index (lowest id on a tie), only
// among recently active ones if activeOnly; 0 if there is none.
func bestFollower(progress map[uint64]tracker.Progress, self uint64, activeOnly bool) uint64 {
	var best uint64
	for id, pr := range progress {
		if id == self || (activeOnly && !pr.RecentActive) {
			continue
		}
		if best == 0 || pr.Match > progress[best].Match || (pr.Match == progress[best].Match && id < best) {
			best = id
		}
	}
	return best
}
