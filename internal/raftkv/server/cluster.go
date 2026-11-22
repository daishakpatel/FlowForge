package server

import (
	"context"

	pb "github.com/daishakpatel/flowforge/internal/gen/proto/coordination/v1"
)

// healthMaxLag is how far this node's applied index may trail its commit index and still be
// READY: a node replaying or installing a snapshot isn't ready to serve yet (04.06).
const healthMaxLag = 100

// Status reports this node's view of the group (04.06): its role, indexes and log bounds, the
// leader's view of every voter's progress (on the leader only), and the state machine's hash at
// an applied index, which equals every other replica's at the same index. Computing the hash
// encodes the whole state, so Status is for operators, not for polling in a hot loop.
func (c clusterService) Status(context.Context, *pb.StatusRequest) (*pb.StatusResponse, error) {
	st := c.s.node.Status()
	hash, hashIndex := c.s.fsm.StateHashAt()
	res := &pb.StatusResponse{
		NodeId: c.s.cfg.NodeID, LeaderId: st.Leader, Term: st.Term, CommitIndex: st.Commit, AppliedIndex: st.Applied,
		Members: c.s.cfg.Members, State: st.State, StateHash: hash[:], StateHashIndex: hashIndex,
		SnapshotIndex: st.SnapshotIndex, FirstIndex: st.FirstIndex,
		UptimeMs: c.s.cfg.Clock.Now().Sub(c.s.started).Milliseconds(),
	}
	for _, p := range st.Peers {
		ps := &pb.PeerStatus{Id: p.ID, Addr: p.Addr, Reachable: p.Reachable, MatchIndex: p.Match}
		if p.Match < st.Commit {
			ps.Lag = st.Commit - p.Match
		}
		res.Peers = append(res.Peers, ps)
	}
	return res, nil
}

// Health is READY while the node runs, knows a leader, and has applied to within healthMaxLag
// entries of the leader's commit index; NOT_READY otherwise (04.06; the Compose healthcheck uses
// it). The leader's commit, not the node's own: a wiped or far-behind follower's own commit is
// bounded by its short log, so it would look caught up.
func (c clusterService) Health(context.Context, *pb.HealthRequest) (*pb.HealthResponse, error) {
	st := c.s.node.Status()
	running := true
	select {
	case <-c.s.node.Done():
		running = false
	default:
	}
	ready := running && st.Leader != 0 && st.LeaderCommit-min(st.LeaderCommit, st.Applied) <= healthMaxLag
	res := &pb.HealthResponse{Healthy: ready, LeaderId: st.Leader, Status: "NOT_READY", AppliedIndex: st.Applied, CommitIndex: st.Commit}
	if ready {
		res.Status = "READY"
	}
	return res, nil
}
