package fsm

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	pb "github.com/daishakpatel/flowforge/internal/gen/proto/coordination/v1"
)

// snapshotVersion is the first byte of every snapshot. A future format bumps it; Restore refuses
// versions it doesn't know (ErrSnapshotVersion).
const snapshotVersion byte = 1

// Snapshot encodes the whole state: the version byte, then an FSMState in canonical order (keys
// sorted, leases by id, counters by name, dedupe oldest first). Equal states encode to identical
// bytes. A state over MaxSnapshotBytes is ErrSnapshotTooLarge.
func (f *FSM) Snapshot() ([]byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	b, err := f.st.canonical()
	if err != nil {
		return nil, err
	}
	if len(b) > MaxSnapshotBytes {
		return nil, fmt.Errorf("%w: %d bytes", ErrSnapshotTooLarge, len(b))
	}
	return b, nil
}

// Restore replaces the whole state with a snapshot's. It validates everything before swapping,
// so on error the state is unchanged.
func (f *FSM) Restore(data []byte) error {
	switch {
	case len(data) > MaxSnapshotBytes:
		return fmt.Errorf("%w: %d bytes", ErrSnapshotTooLarge, len(data))
	case len(data) == 0:
		return fmt.Errorf("%w: empty", ErrCorruptSnapshot)
	case data[0] != snapshotVersion:
		return fmt.Errorf("%w: %d", ErrSnapshotVersion, data[0])
	}
	m := &pb.FSMState{}
	if err := proto.Unmarshal(data[1:], m); err != nil {
		return fmt.Errorf("%w: %w", ErrCorruptSnapshot, err)
	}
	st, err := decode(m, f.dedupeCap)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrCorruptSnapshot, err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.st = st
	return nil
}

// canonical returns the version byte followed by the deterministic encoding of the state.
func (s *state) canonical() ([]byte, error) {
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(s.encode())
	if err != nil {
		return nil, fmt.Errorf("encode fsm state: %w", err)
	}
	return append([]byte{snapshotVersion}, b...), nil
}

func (s *state) encode() *pb.FSMState {
	m := &pb.FSMState{LastApplied: s.lastApplied, NextLeaseId: s.nextLeaseID, Fencing: s.fencing}
	for _, k := range sortedKeys(s.keys) {
		e := s.keys[k]
		m.Keys = append(m.Keys, &pb.StoredKey{Key: k, Value: e.value, Version: e.version, ModIndex: e.modIndex,
			CreateIndex: e.createIndex, LeaseId: e.leaseID})
	}
	for _, id := range sortedKeys(s.leases) {
		l := s.leases[id]
		m.Leases = append(m.Leases, &pb.StoredLease{Id: id, TtlMs: l.ttlMs, ExpiresAtMs: l.expiresAt, Owner: l.owner, FencingToken: l.fencing})
	}
	for _, n := range sortedKeys(s.counters) {
		m.Counters = append(m.Counters, &pb.StoredCounter{Name: n, Value: s.counters[n]})
	}
	for _, k := range s.dedupe.live() {
		m.Dedupe = append(m.Dedupe, &pb.StoredDedupe{ClientId: k.client, RequestId: k.request, Result: s.dedupe.get(k)})
	}
	return m
}

// decode rebuilds and validates a state: canonical order, limits, and references (a key's lease
// exists, ids and tokens are within the counters, indexes within last_applied).
func decode(m *pb.FSMState, dedupeCap int) (*state, error) {
	s := newState(dedupeCap)
	s.lastApplied, s.nextLeaseID, s.fencing = m.GetLastApplied(), m.GetNextLeaseId(), m.GetFencing()
	if err := s.decodeLeases(m.GetLeases()); err != nil {
		return nil, err
	}
	if err := s.decodeKeys(m.GetKeys()); err != nil {
		return nil, err
	}
	if err := s.decodeCounters(m.GetCounters()); err != nil {
		return nil, err
	}
	if s.dataBytes > MaxStateBytes {
		return nil, fmt.Errorf("state holds %d bytes", s.dataBytes)
	}
	for _, d := range m.GetDedupe() {
		k, ok := identity(&pb.CommandMeta{ClientId: d.GetClientId(), RequestId: d.GetRequestId()})
		if !ok || len(k.client) > MaxIDBytes || len(k.request) > MaxIDBytes || d.GetResult().GetCode() == pb.ResultCode_RESULT_CODE_UNSPECIFIED || s.dedupe.get(k) != nil {
			return nil, fmt.Errorf("bad dedupe entry %q/%q", k.client, k.request)
		}
		s.dedupe.add(k, d.GetResult())
	}
	// A table this FSM would have trimmed (another capacity, or over the byte budget) would make
	// the replicas' retries diverge: refuse it rather than drop entries silently.
	if len(s.dedupe.live()) != len(m.GetDedupe()) {
		return nil, fmt.Errorf("%d dedupe entries exceed this FSM's capacity", len(m.GetDedupe()))
	}
	return s, nil
}

func (s *state) decodeLeases(ls []*pb.StoredLease) error {
	if len(ls) > MaxLeases {
		return fmt.Errorf("%d leases", len(ls))
	}
	var prev, prevToken uint64
	for _, l := range ls {
		id := l.GetId()
		switch {
		case id <= prev || id > s.nextLeaseID:
			return fmt.Errorf("lease %d out of order or beyond next id %d", id, s.nextLeaseID)
		case l.GetFencingToken() <= prevToken || l.GetFencingToken() > s.fencing: // granted in id order
			return fmt.Errorf("lease %d fencing token %d beyond %d", id, l.GetFencingToken(), s.fencing)
		case l.GetTtlMs() == 0 || l.GetTtlMs() > MaxLeaseTTLMs || len(l.GetOwner()) > MaxIDBytes:
			return fmt.Errorf("lease %d has a bad ttl or owner", id)
		}
		s.leases[id] = &lease{ttlMs: l.GetTtlMs(), expiresAt: l.GetExpiresAtMs(), owner: l.GetOwner(), fencing: l.GetFencingToken(), keys: map[string]struct{}{}}
		s.dataBytes += len(l.GetOwner())
		prev, prevToken = id, l.GetFencingToken()
	}
	return nil
}

func (s *state) decodeKeys(ks []*pb.StoredKey) error {
	if len(ks) > MaxKeys {
		return fmt.Errorf("%d keys", len(ks))
	}
	prev := ""
	for i, k := range ks {
		key := k.GetKey()
		switch {
		case i > 0 && key <= prev, checkKV(key, k.GetValue()) != codeOK:
			return fmt.Errorf("key %q out of order or invalid", key)
		case k.GetVersion() == 0 || k.GetCreateIndex() == 0 || k.GetCreateIndex() > k.GetModIndex() || k.GetModIndex() > s.lastApplied:
			return fmt.Errorf("key %q has bad version or indexes", key)
		case k.GetLeaseId() != 0 && s.leases[k.GetLeaseId()] == nil:
			return fmt.Errorf("key %q bound to unknown lease %d", key, k.GetLeaseId())
		}
		s.keys[key] = &entry{value: k.GetValue(), version: k.GetVersion(), modIndex: k.GetModIndex(), createIndex: k.GetCreateIndex(), leaseID: k.GetLeaseId()}
		s.dataBytes += len(key) + len(k.GetValue())
		s.attach(key, k.GetLeaseId())
		prev = key
	}
	return nil
}

func (s *state) decodeCounters(cs []*pb.StoredCounter) error {
	if len(cs) > MaxCounters {
		return fmt.Errorf("%d counters", len(cs))
	}
	prev := ""
	for i, c := range cs {
		if (i > 0 && c.GetName() <= prev) || checkKey(c.GetName()) != codeOK || c.GetValue() == 0 {
			return fmt.Errorf("counter %q out of order or invalid", c.GetName())
		}
		s.counters[c.GetName()] = c.GetValue()
		s.dataBytes += len(c.GetName())
		prev = c.GetName()
	}
	return nil
}
