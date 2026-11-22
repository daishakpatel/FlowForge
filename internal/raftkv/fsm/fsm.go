// Package fsm is the Raft KV's replicated state machine (task 04.03, FR-RAFT-02, FR-RAFT-03,
// ADR-005, ADR-006): a key-value store with versions, CAS, small transactions, TTL leases,
// fencing tokens, counters and request-id deduplication.
//
// It is pure and deterministic. Replicas that apply the same log reach byte-identical state
// (StateHash): it reads no clock (all time is the command's ts_unix_ms, stamped by the receiving
// node), uses no randomness, network or goroutines, and never lets Go's map order reach its
// output (sortedKeys; TestNoMapIterationOutsideSortedKeys). Failed conditions are results with a
// ResultCode, never Go errors. See docs/architecture/raft-fsm.md.
package fsm

import (
	"slices"
	"strings"
	"sync"

	"google.golang.org/protobuf/proto"

	pb "github.com/daishakpatel/flowforge/internal/gen/proto/coordination/v1"
)

// Options configures an FSM. DedupeCapacity is part of the replicated behaviour (it decides when
// a retry re-executes), so every replica must use the same value; zero means
// DefaultDedupeCapacity.
type Options struct {
	DedupeCapacity int
}

// FSM is the state machine. Apply, Snapshot and Restore are for the one goroutine that drives it
// (the Raft node, through Applier); the read helpers may be called concurrently from any
// goroutine and see a consistent state between two applies (one RWMutex guards everything).
type FSM struct {
	mu        sync.RWMutex
	st        *state
	dedupeCap int
}

// New returns an empty FSM.
func New(o Options) *FSM {
	if o.DedupeCapacity <= 0 {
		o.DedupeCapacity = DefaultDedupeCapacity
	}
	return &FSM{st: newState(o.DedupeCapacity), dedupeCap: o.DedupeCapacity}
}

// Apply applies the command encoded in data (a Raft entry) at log index and returns its result.
// Every outcome, including undecodable data, is a deterministic CommandResult with a nil error.
// The only error is ErrAlreadyApplied, for index ≤ AppliedIndex(): the state is unchanged, and
// if the command carries a remembered (client_id, request_id) its stored result is returned
// instead (re-delivery is harmless).
func (f *FSM) Apply(index uint64, data []byte) (*pb.CommandResult, error) {
	cmd := &pb.Command{}
	if proto.Unmarshal(data, cmd) != nil {
		cmd = nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if index <= f.st.lastApplied {
		if k, ok := identity(cmd.GetMeta()); ok {
			if r := f.st.dedupe.get(k); r != nil {
				return cloneResult(r), nil
			}
		}
		return nil, ErrAlreadyApplied
	}
	res := f.st.apply(index, cmd)
	f.st.lastApplied = index
	return cloneResult(res), nil
}

func cloneResult(r *pb.CommandResult) *pb.CommandResult {
	return proto.Clone(r).(*pb.CommandResult) //nolint:forcetypeassert // Clone keeps the type
}

// identity returns a command's dedupe key when it has both ids.
func identity(m *pb.CommandMeta) (dedupeKey, bool) {
	k := dedupeKey{client: m.GetClientId(), request: m.GetRequestId()}
	return k, k.client != "" && k.request != ""
}

// apply runs one decoded command (nil: undecodable), deduplicating identified commands.
func (s *state) apply(index uint64, cmd *pb.Command) *pb.CommandResult {
	meta := cmd.GetMeta()
	if cmd == nil || len(meta.GetClientId()) > MaxIDBytes || len(meta.GetRequestId()) > MaxIDBytes {
		return result(codeInvalid)
	}
	k, identified := identity(meta)
	if identified {
		if r := s.dedupe.get(k); r != nil {
			return r
		}
	}
	res := s.exec(index, meta.GetTsUnixMs(), cmd)
	if identified {
		s.dedupe.add(k, res)
	}
	return res
}

func (s *state) exec(index uint64, ts int64, cmd *pb.Command) *pb.CommandResult {
	switch op := cmd.GetOp().(type) {
	case *pb.Command_Put:
		r := op.Put
		return s.put(index, ts, r.GetKey(), r.GetValue(), r.ExpectedVersion, r.GetLeaseId())
	case *pb.Command_Delete:
		return s.del(op.Delete.GetKey(), op.Delete.ExpectedVersion)
	case *pb.Command_Cas:
		return s.cas(index, ts, op.Cas)
	case *pb.Command_Txn:
		return s.txn(index, ts, op.Txn)
	case *pb.Command_LeaseGrant:
		return s.grant(ts, op.LeaseGrant)
	case *pb.Command_LeaseKeepAlive:
		return s.keepAlive(ts, op.LeaseKeepAlive)
	case *pb.Command_LeaseRevoke:
		return s.revoke(op.LeaseRevoke)
	case *pb.Command_LeaseExpire:
		return s.expire(ts, op.LeaseExpire)
	case *pb.Command_IncrCounter:
		return s.incr(op.IncrCounter)
	case *pb.Command_Noop:
		return result(codeOK)
	default:
		return result(codeInvalid)
	}
}

// AppliedIndex returns the index of the last applied entry.
func (f *FSM) AppliedIndex() uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.st.lastApplied
}

// Get returns a copy of key. A key bound to an expired lease is still returned: the caller judges
// expiry with IsExpired and its own clock (04.04).
func (f *FSM) Get(key string) (*pb.KeyValue, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	e := f.st.keys[key]
	return kv(key, e), e != nil
}

// List returns copies of up to limit keys with prefix, in key order (limit ≤ 0: all), and
// whether more matched.
func (f *FSM) List(prefix string, limit int) ([]*pb.KeyValue, bool) {
	return f.ListFrom(prefix, "", limit)
}

// ListFrom is List starting at the first key ≥ start: a page boundary for 04.04's next_key.
func (f *FSM) ListFrom(prefix, start string, limit int) ([]*pb.KeyValue, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	var out []*pb.KeyValue
	for _, k := range sortedKeys(f.st.keys) {
		if k < start || !strings.HasPrefix(k, prefix) {
			continue
		}
		if limit > 0 && len(out) == limit {
			return out, true
		}
		out = append(out, kv(k, f.st.keys[k]))
	}
	return out, false
}

// GetLease returns a lease, whether or not it has expired.
func (f *FSM) GetLease(id uint64) (LeaseInfo, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	l := f.st.leases[id]
	if l == nil {
		return LeaseInfo{}, false
	}
	return LeaseInfo{ID: id, TTLMs: l.ttlMs, ExpiresAtMs: l.expiresAt, Owner: l.owner, FencingToken: l.fencing,
		Keys: sortedKeys(l.keys)}, true
}

// IsExpired judges a lease against the caller's clock nowMs (ADR-005: the leader's, at read
// time). An unknown lease (never granted, revoked or already expired) counts as expired.
func (f *FSM) IsExpired(id uint64, nowMs int64) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	l := f.st.leases[id]
	return l == nil || l.expiresAt <= nowMs
}

// ExpiredLeaseIDs returns the leases expired at nowMs, ascending, for the leader's sweeper to
// propose LeaseExpire commands.
func (f *FSM) ExpiredLeaseIDs(nowMs int64) []uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	ids := sortedKeys(f.st.leases)
	return slices.DeleteFunc(ids, func(id uint64) bool { return f.st.leases[id].expiresAt > nowMs })
}
