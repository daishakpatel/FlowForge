package server

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"

	pb "github.com/daishakpatel/flowforge/internal/gen/proto/coordination/v1"
)

const resultOK = pb.ResultCode_RESULT_CODE_OK

// identity turns a request's meta into the command's. A request id needs a client id: the FSM
// dedupes on the pair. Without one the command is not deduplicated, so the client can't retry
// it safely (the client library always sends both).
func identity(m *pb.RequestMeta) (*pb.CommandMeta, error) {
	if m.GetRequestId() != "" && m.GetClientId() == "" {
		return nil, invalid("meta.request_id needs meta.client_id")
	}
	return &pb.CommandMeta{ClientId: m.GetClientId(), RequestId: m.GetRequestId()}, nil
}

// write proposes one op stamped with ts (this node's clock, ADR-005) and returns its FSM result
// once applied here. Only the leader accepts proposals (node: NOT_LEADER with the hint).
func (s *Server) write(ctx context.Context, m *pb.RequestMeta, ts int64, set func(*pb.Command)) (*pb.CommandResult, error) {
	meta, err := identity(m)
	if err != nil {
		return nil, err
	}
	meta.TsUnixMs = ts
	cmd := &pb.Command{Meta: meta}
	set(cmd)
	data, err := proto.Marshal(cmd)
	if err != nil {
		return nil, internal(fmt.Errorf("encode command: %w", err))
	}
	pctx, cancel := context.WithTimeout(ctx, s.cfg.ProposalTimeout)
	defer cancel()
	res, err := s.node.Propose(pctx, data)
	if err != nil {
		return nil, nodeErr(ctx, err)
	}
	if res.Err != nil {
		return nil, internal(fmt.Errorf("apply entry %d: %w", res.Index, res.Err))
	}
	r := &pb.CommandResult{}
	if err := proto.Unmarshal(res.Data, r); err != nil {
		return nil, internal(fmt.Errorf("decode result of entry %d: %w", res.Index, err))
	}
	return r, nil
}

// expireTouched makes writes see what reads see (ADR-005): for every key a write touches that is
// bound to a lease this node's clock says expired at ts, it first proposes LeaseExpire stamped
// ts, so the write finds the key absent instead of waiting for the sweeper. The FSM re-checks
// expires_at ≤ ts against the latest state, so a lease a concurrent keep-alive extended is left
// alone, and a stale local view only costs a no-op entry.
func (s *Server) expireTouched(ctx context.Context, ts int64, keys []string) error {
	done := map[uint64]bool{}
	for _, k := range keys {
		kv, ok := s.fsm.Get(k)
		id := kv.GetLeaseId()
		if !ok || id == 0 || done[id] || !s.fsm.IsExpired(id, ts) {
			continue
		}
		done[id] = true
		if _, err := s.write(ctx, nil, ts, func(c *pb.Command) {
			c.Op = &pb.Command_LeaseExpire{LeaseExpire: &pb.LeaseExpire{LeaseId: id}}
		}); err != nil {
			return err
		}
	}
	return nil
}

// touchAndWrite stamps one timestamp, expires the touched keys' expired leases, then writes.
func (s *Server) touchAndWrite(ctx context.Context, m *pb.RequestMeta, keys []string, set func(*pb.Command)) (*pb.CommandResult, error) {
	ts := s.now()
	if err := s.expireTouched(ctx, ts, keys); err != nil {
		return nil, err
	}
	return s.write(ctx, m, ts, set)
}

// txnKeys lists every key a Txn compares or operates on.
func txnKeys(req *pb.TxnRequest) []string {
	var keys []string
	for _, c := range req.GetCompare() {
		keys = append(keys, c.GetKey())
	}
	for _, op := range append(append([]*pb.Op{}, req.GetSuccess()...), req.GetFailure()...) {
		keys = append(keys, op.GetPut().GetKey(), op.GetDelete().GetKey(), op.GetGet().GetKey())
	}
	return keys
}

// writeOK is touchAndWrite for ops whose only success is OK.
func (s *Server) writeOK(ctx context.Context, m *pb.RequestMeta, keys []string, set func(*pb.Command)) (*pb.CommandResult, error) {
	r, err := s.touchAndWrite(ctx, m, keys, set)
	if err != nil {
		return nil, err
	}
	if r.GetCode() != resultOK {
		return nil, resultErr(r)
	}
	return r, nil
}

func (k kvService) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	r, err := k.s.writeOK(ctx, req.GetMeta(), []string{req.GetKey()}, func(c *pb.Command) { c.Op = &pb.Command_Put{Put: req} })
	if err != nil {
		return nil, err
	}
	return &pb.PutResponse{Kv: r.GetKv()}, nil
}

func (k kvService) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	r, err := k.s.writeOK(ctx, req.GetMeta(), []string{req.GetKey()}, func(c *pb.Command) { c.Op = &pb.Command_Delete{Delete: req} })
	if err != nil {
		return nil, err
	}
	return &pb.DeleteResponse{Deleted: r.GetDeleted()}, nil
}

// CAS: a version mismatch is not an error (kv.proto): succeeded is false and kv holds the
// current key, or is unset when the key is absent.
func (k kvService) CAS(ctx context.Context, req *pb.CASRequest) (*pb.CASResponse, error) {
	r, err := k.s.touchAndWrite(ctx, req.GetMeta(), []string{req.GetKey()}, func(c *pb.Command) { c.Op = &pb.Command_Cas{Cas: req} })
	if err != nil {
		return nil, err
	}
	switch r.GetCode() {
	case resultOK:
		return &pb.CASResponse{Succeeded: true, Kv: r.GetKv()}, nil
	case pb.ResultCode_RESULT_CODE_VERSION_MISMATCH, pb.ResultCode_RESULT_CODE_KEY_NOT_FOUND:
		return &pb.CASResponse{Kv: r.GetKv()}, nil
	}
	return nil, resultErr(r)
}

func (k kvService) Txn(ctx context.Context, req *pb.TxnRequest) (*pb.TxnResponse, error) {
	r, err := k.s.writeOK(ctx, req.GetMeta(), txnKeys(req), func(c *pb.Command) { c.Op = &pb.Command_Txn{Txn: req} })
	if err != nil {
		return nil, err
	}
	return &pb.TxnResponse{Succeeded: r.GetSucceeded(), Results: r.GetResults()}, nil
}

func (k kvService) LeaseGrant(ctx context.Context, req *pb.LeaseGrantRequest) (*pb.LeaseGrantResponse, error) {
	r, err := k.s.writeOK(ctx, req.GetMeta(), nil, func(c *pb.Command) { c.Op = &pb.Command_LeaseGrant{LeaseGrant: req} })
	if err != nil {
		return nil, err
	}
	return &pb.LeaseGrantResponse{LeaseId: r.GetLeaseId(), FencingToken: r.GetFencingToken(), TtlMs: r.GetTtlMs()}, nil
}

// LeaseKeepAlive refuses, with LEASE_EXPIRED, a lease the leader's clock says has expired, even
// before a sweep removes it (ADR-005): the holder must re-grant. The check reads the state after
// a ReadIndex, and the command carries the same timestamp, so the FSM judges it at the same
// instant; a concurrent keep-alive can only extend the lease, never shorten it.
func (k kvService) LeaseKeepAlive(ctx context.Context, req *pb.LeaseKeepAliveRequest) (*pb.LeaseKeepAliveResponse, error) {
	now, err := k.s.confirmLeader(ctx)
	if err != nil {
		return nil, err
	}
	// A wrong token falls through to the FSM's LEASE_NOT_FOUND: only the holder learns "expired".
	if l, ok := k.s.fsm.GetLease(req.GetLeaseId()); ok && l.FencingToken == req.GetFencingToken() && l.ExpiresAtMs <= now {
		return nil, resultErr(&pb.CommandResult{Code: pb.ResultCode_RESULT_CODE_LEASE_EXPIRED})
	}
	r, err := k.s.write(ctx, req.GetMeta(), now, func(c *pb.Command) { c.Op = &pb.Command_LeaseKeepAlive{LeaseKeepAlive: req} })
	if err != nil {
		return nil, err
	}
	if r.GetCode() != resultOK {
		return nil, resultErr(r)
	}
	return &pb.LeaseKeepAliveResponse{LeaseId: r.GetLeaseId(), TtlMs: r.GetTtlMs()}, nil
}

func (k kvService) LeaseRevoke(ctx context.Context, req *pb.LeaseRevokeRequest) (*pb.LeaseRevokeResponse, error) {
	if _, err := k.s.writeOK(ctx, req.GetMeta(), nil, func(c *pb.Command) { c.Op = &pb.Command_LeaseRevoke{LeaseRevoke: req} }); err != nil {
		return nil, err
	}
	return &pb.LeaseRevokeResponse{}, nil
}

func (k kvService) IncrCounter(ctx context.Context, req *pb.IncrCounterRequest) (*pb.IncrCounterResponse, error) {
	r, err := k.s.writeOK(ctx, req.GetMeta(), nil, func(c *pb.Command) { c.Op = &pb.Command_IncrCounter{IncrCounter: req} })
	if err != nil {
		return nil, err
	}
	return &pb.IncrCounterResponse{Value: r.GetCounter()}, nil
}
