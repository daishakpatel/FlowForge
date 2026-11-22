package server

import (
	"context"

	pb "github.com/daishakpatel/flowforge/internal/gen/proto/coordination/v1"
)

// Status reports this node's view of the group. 04.06 extends it (peers' progress, state hash).
func (c clusterService) Status(context.Context, *pb.StatusRequest) (*pb.StatusResponse, error) {
	st := c.s.node.Status()
	return &pb.StatusResponse{
		NodeId: c.s.cfg.NodeID, LeaderId: st.Leader, Term: st.Term,
		CommitIndex: st.Commit, AppliedIndex: st.Applied, Members: c.s.cfg.Members,
	}, nil
}

// Health is healthy while the node runs and knows a leader. 04.06 adds the applied-lag bound.
func (c clusterService) Health(context.Context, *pb.HealthRequest) (*pb.HealthResponse, error) {
	st := c.s.node.Status()
	running := true
	select {
	case <-c.s.node.Done():
		running = false
	default:
	}
	return &pb.HealthResponse{Healthy: running && st.Leader != 0, LeaderId: st.Leader}, nil
}
