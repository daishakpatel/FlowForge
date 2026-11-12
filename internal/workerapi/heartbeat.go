package workerapi

import (
	"context"
	"slices"

	"google.golang.org/protobuf/types/known/timestamppb"

	workerv1 "github.com/daishakpatel/flowforge/internal/gen/proto/worker/v1"
)

// Heartbeat implements workerv1.WorkerServiceServer. It records a heartbeat on every listed
// attempt that is still RUNNING on this worker with that fencing token, returns the others as
// stale (the worker must abort them), renews the worker's lease, and asks the CancelSource which
// live attempts to cancel.
func (s *Service) Heartbeat(ctx context.Context, req *workerv1.HeartbeatRequest) (*workerv1.HeartbeatResponse, error) {
	w, err := s.registeredWorker(ctx, req.GetWorkerId())
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(req.GetRunning()))
	tokens := make([]int64, 0, len(req.GetRunning()))
	for _, r := range req.GetRunning() {
		ids = append(ids, r.GetAttemptId())
		tokens = append(tokens, r.GetFencingToken())
	}
	live := []string{}
	if len(ids) > 0 {
		if live, err = s.Attempts.TouchRunning(ctx, s.DB, w.WorkerID, ids, tokens, s.Clock.Now()); err != nil {
			return nil, err
		}
	}
	var stale []string
	for _, id := range ids {
		if !slices.Contains(live, id) {
			stale = append(stale, id)
		}
	}
	if err := s.Leases.Renew(ctx, w.WorkerID, caller(ctx), len(live)); err != nil {
		return nil, err
	}
	cancel, err := s.Cancels.CancelledAttempts(ctx, live)
	if err != nil {
		return nil, err
	}
	return &workerv1.HeartbeatResponse{
		CancelAttemptIds: cancel, StaleAttemptIds: stale, LeaseTtlMs: s.LeaseTTL.Milliseconds(),
		ServerTime: timestamppb.New(s.Clock.Now()),
	}, nil
}
