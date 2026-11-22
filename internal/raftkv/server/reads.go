package server

import (
	"context"

	"github.com/daishakpatel/flowforge/internal/apperrors"
	pb "github.com/daishakpatel/flowforge/internal/gen/proto/coordination/v1"
	"github.com/daishakpatel/flowforge/internal/raftkv/fsm"
)

// List page limits: at most listMaxLimit keys (default when 0), and a page stops early once it
// holds listMaxBytes of keys and values, so a response stays under gRPC's 4 MiB default.
const (
	listMaxLimit = 1000
	listMaxBytes = 2 << 20
)

// view is how a read judges lease expiry and flags its response.
type view struct {
	now       int64 // the clock expiry is judged by
	leaderNow int64 // the response's leader_now_ms (LINEARIZABLE only)
	stale     bool
}

func (s *Server) view(ctx context.Context, c pb.Consistency) (view, error) {
	switch c {
	case pb.Consistency_CONSISTENCY_UNSPECIFIED, pb.Consistency_CONSISTENCY_LINEARIZABLE:
		now, err := s.confirmLeader(ctx)
		return view{now: now, leaderNow: now}, err
	case pb.Consistency_CONSISTENCY_SERIALIZABLE:
		// Not the leader's judgement: this node's clock, on state that may lag (flagged stale).
		return view{now: s.now(), stale: true}, nil
	}
	return view{}, invalid("unknown consistency")
}

// confirmLeader makes this node, which must be the leader, prove its leadership with a ReadIndex
// and catch up through it, then reads its clock: state read afterwards reflects every write
// acknowledged before the call, and expiry is judged at the returned instant (ADR-005). A
// leader cut off from its quorum can't complete the ReadIndex (CheckQuorum), so it never
// serves a stale read.
func (s *Server) confirmLeader(ctx context.Context) (int64, error) {
	st := s.node.Status()
	if !st.IsLeader {
		return 0, notLeader(st.Leader)
	}
	rctx, cancel := context.WithTimeout(ctx, s.cfg.ProposalTimeout)
	defer cancel()
	start := s.cfg.Clock.Now()
	_, err := s.node.ReadIndex(rctx)
	s.cfg.Metrics.readIndex(s.cfg.Clock.Now().Sub(start).Seconds())
	if err != nil {
		return 0, nodeErr(ctx, err)
	}
	// ReadIndex also works on a follower: if leadership moved meanwhile, this node's clock is no
	// longer the leader's and must not judge expiry.
	if after := s.node.Status(); !after.IsLeader || after.Term != st.Term {
		return 0, notLeader(after.Leader)
	}
	return s.now(), nil
}

// live reports whether kv is visible at now: unbound, or bound to a lease not yet expired.
func (s *Server) live(kv *pb.KeyValue, now int64) bool {
	return kv.GetLeaseId() == 0 || !s.fsm.IsExpired(kv.GetLeaseId(), now)
}

func checkKey(key string) error {
	if key == "" || len(key) > fsm.MaxKeyBytes {
		return invalid("key must be 1–512 bytes")
	}
	return nil
}

// Get is NOT_FOUND for an absent key and for one bound to an expired lease.
func (k kvService) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if err := checkKey(req.GetKey()); err != nil {
		return nil, err
	}
	v, err := k.s.view(ctx, req.GetConsistency())
	if err != nil {
		return nil, err
	}
	applied := k.s.fsm.AppliedIndex() // read first: the state returned reflects at least this index
	kv, ok := k.s.fsm.Get(req.GetKey())
	if !ok || !k.s.live(kv, v.now) {
		return nil, newErr(apperrors.CodeNotFound, "key not found", nil)
	}
	return &pb.GetResponse{Kv: kv, AppliedIndex: applied, LeaderNowMs: v.leaderNow, Stale: v.stale}, nil
}

// List returns one page of the keys under prefix from start_key, in key order, without expired
// lease-bound keys; next_key continues it. Each page is one consistent read; a listing of
// several pages is not one snapshot.
func (k kvService) List(ctx context.Context, req *pb.ListRequest) (*pb.ListResponse, error) {
	limit := int(req.GetLimit())
	if limit == 0 || limit > listMaxLimit {
		limit = listMaxLimit
	}
	v, err := k.s.view(ctx, req.GetConsistency())
	if err != nil {
		return nil, err
	}
	res := &pb.ListResponse{AppliedIndex: k.s.fsm.AppliedIndex(), LeaderNowMs: v.leaderNow, Stale: v.stale} // a lower bound, as in Get
	kvs, more := k.s.fsm.ListFrom(req.GetPrefix(), req.GetStartKey(), limit)
	size := 0
	for _, kv := range kvs {
		n := len(kv.GetKey()) + len(kv.GetValue())
		if size > 0 && size+n > listMaxBytes {
			res.More, res.NextKey = true, kv.GetKey()
			return res, nil
		}
		if k.s.live(kv, v.now) {
			res.Kvs = append(res.Kvs, kv)
			size += n
		}
	}
	if more {
		res.More, res.NextKey = true, kvs[len(kvs)-1].GetKey()+"\x00" // the smallest key after it
	}
	return res, nil
}
