package fsm

import (
	"math"

	pb "github.com/daishakpatel/flowforge/internal/gen/proto/coordination/v1"
)

// Lease timing (ADR-005). The FSM stores only timestamps carried in commands:
//
//	LeaseGrant@ts       → expires_at = ts + ttl
//	KeepAlive@ts        → expires_at = max(expires_at, ts) + ttl   (never moves backwards)
//	LeaseExpire(id)@ts  → if expires_at <= ts: delete the lease and its keys (idempotent)
//	IsExpired(id, now)  → expires_at <= now, judged by the reader (the leader, 04.04)

// leaseLive reports whether a write may bind to lease id at ts.
func (s *state) leaseLive(id uint64, ts int64) pb.ResultCode {
	l := s.leases[id]
	switch {
	case l == nil:
		return codeNoLease
	case l.expiresAt <= ts:
		return codeExpired
	}
	return codeOK
}

// expiry returns base + ttl, or false if it would overflow.
func expiry(base int64, ttl uint64) (int64, bool) {
	if base > math.MaxInt64-int64(ttl) { //nolint:gosec // ttl ≤ MaxLeaseTTLMs
		return 0, false
	}
	return base + int64(ttl), true //nolint:gosec // ttl ≤ MaxLeaseTTLMs
}

func (s *state) grant(ts int64, r *pb.LeaseGrantRequest) *pb.CommandResult {
	ttl := r.GetTtlMs()
	if ttl == 0 || ttl > MaxLeaseTTLMs || len(r.GetOwner()) > MaxIDBytes {
		return result(codeInvalid)
	}
	exp, ok := expiry(ts, ttl)
	if !ok {
		return result(codeInvalid)
	}
	if len(s.leases) >= MaxLeases || !s.fits(len(r.GetOwner())) {
		return result(codeQuota)
	}
	s.dataBytes += len(r.GetOwner())
	s.nextLeaseID++
	s.fencing++
	s.leases[s.nextLeaseID] = &lease{ttlMs: ttl, expiresAt: exp, owner: r.GetOwner(), fencing: s.fencing, keys: map[string]struct{}{}}
	return &pb.CommandResult{Code: codeOK, LeaseId: s.nextLeaseID, FencingToken: s.fencing, TtlMs: ttl, ExpiresAtMs: exp}
}

// keepAlive extends a lease that still exists, even one past its expiry that no LeaseExpire has
// removed yet (task 04.03: only an expired *and* removed lease is rejected). The fencing token
// must match the grant's.
func (s *state) keepAlive(ts int64, r *pb.LeaseKeepAliveRequest) *pb.CommandResult {
	l := s.leases[r.GetLeaseId()]
	if l == nil || l.fencing != r.GetFencingToken() {
		return result(codeNoLease)
	}
	exp, ok := expiry(max(l.expiresAt, ts), l.ttlMs)
	if !ok {
		return result(codeInvalid)
	}
	l.expiresAt = exp
	return &pb.CommandResult{Code: codeOK, LeaseId: r.GetLeaseId(), TtlMs: l.ttlMs, ExpiresAtMs: exp}
}

func (s *state) revoke(r *pb.LeaseRevokeRequest) *pb.CommandResult {
	if s.leases[r.GetLeaseId()] == nil {
		return result(codeNoLease)
	}
	s.dropLease(r.GetLeaseId())
	return result(codeOK)
}

// expire removes a lease that expired by ts; an unknown or still-live lease is left alone, so a
// repeated or early LeaseExpire is harmless.
func (s *state) expire(ts int64, r *pb.LeaseExpire) *pb.CommandResult {
	l := s.leases[r.GetLeaseId()]
	if l == nil || l.expiresAt > ts {
		return result(codeOK)
	}
	s.dropLease(r.GetLeaseId())
	return &pb.CommandResult{Code: codeOK, Deleted: true}
}

// dropLease deletes a lease and every key bound to it.
func (s *state) dropLease(id uint64) {
	l := s.leases[id]
	for _, k := range sortedKeys(l.keys) {
		s.remove(k)
	}
	s.dataBytes -= len(l.owner)
	delete(s.leases, id)
}

// LeaseInfo is a lease as the read helpers report it.
type LeaseInfo struct {
	ID           uint64
	TTLMs        uint64
	ExpiresAtMs  int64
	Owner        string
	FencingToken uint64
	Keys         []string // sorted
}
