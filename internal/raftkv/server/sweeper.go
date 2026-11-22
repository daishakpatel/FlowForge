package server

import (
	"context"
	"fmt"
	"runtime/debug"

	pb "github.com/daishakpatel/flowforge/internal/gen/proto/coordination/v1"
)

// RunSweeper deletes expired leases until ctx ends (ADR-005): every SweepInterval, if this node
// is the leader, it proposes LeaseExpire for up to SweepBatch leases its clock says expired. A
// follower's ticks do nothing, so the sweep follows leadership. LeaseExpire is idempotent and
// the FSM re-checks expires_at against the command's timestamp, so a sweep from a stale view
// or a deposed leader changes nothing.
func (s *Server) RunSweeper(ctx context.Context) {
	t := s.cfg.Clock.NewTicker(s.cfg.SweepInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C():
			s.sweep(ctx)
		}
	}
}

func (s *Server) sweep(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			s.cfg.Logger.ErrorContext(ctx, "raft kv: sweeper panicked", "panic", r, "stack", string(debug.Stack()))
			s.cfg.Metrics.swept("error", 1)
		}
	}()
	if !s.node.Status().IsLeader {
		return
	}
	now := s.now()
	ids := s.fsm.ExpiredLeaseIDs(now)
	if len(ids) > s.cfg.SweepBatch {
		ids = ids[:s.cfg.SweepBatch]
	}
	for _, id := range ids {
		r, err := s.write(ctx, nil, now, func(c *pb.Command) {
			c.Op = &pb.Command_LeaseExpire{LeaseExpire: &pb.LeaseExpire{LeaseId: id}}
		})
		if err != nil && ctx.Err() != nil {
			return // shutting down, not a failed sweep
		}
		if err != nil {
			// Most likely leadership moved; the next leader's sweep picks the lease up.
			s.cfg.Logger.DebugContext(ctx, "raft kv: lease sweep stopped", "lease_id", id, "err", fmt.Sprint(err))
			s.cfg.Metrics.swept("error", 1)
			return
		}
		if r.GetDeleted() {
			s.cfg.Metrics.swept("expired", 1)
		}
	}
}
