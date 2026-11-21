package fsm

import (
	"cmp"
	"slices"

	pb "github.com/daishakpatel/flowforge/internal/gen/proto/coordination/v1"
)

// state is the replicated state. Only apply (under FSM.mu) mutates it. Every iteration over its
// maps goes through sortedKeys, so nothing the FSM produces depends on map order.
type state struct {
	lastApplied uint64
	nextLeaseID uint64 // last lease id handed out
	fencing     uint64 // global fencing counter: last token handed out
	keys        map[string]*entry
	leases      map[uint64]*lease
	counters    map[string]uint64
	dedupe      *dedupe
	dataBytes   int // keys + values + counter names + lease owners (MaxStateBytes)
}

type entry struct {
	value       []byte
	version     uint64 // 1 on creation
	modIndex    uint64 // log index of the last write
	createIndex uint64 // log index that created this life of the key
	leaseID     uint64 // 0: none
}

type lease struct {
	ttlMs     uint64
	expiresAt int64 // unix ms
	owner     string
	fencing   uint64
	keys      map[string]struct{} // derived from the entries; rebuilt on restore
}

func newState(dedupeCap int) *state {
	return &state{
		keys: map[string]*entry{}, leases: map[uint64]*lease{}, counters: map[string]uint64{},
		dedupe: newDedupe(dedupeCap),
	}
}

// sortedKeys is the only place the FSM ranges over a map (TestNoMapIterationOutsideSortedKeys).
func sortedKeys[K cmp.Ordered, V any](m map[K]V) []K {
	ks := make([]K, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	slices.Sort(ks)
	return ks
}

// fits reports whether the state may grow by delta bytes.
func (s *state) fits(delta int) bool { return s.dataBytes+delta <= MaxStateBytes }

// kv returns a copy of key's entry as the API type, or nil when absent.
func kv(key string, e *entry) *pb.KeyValue {
	if e == nil {
		return nil
	}
	return &pb.KeyValue{Key: key, Value: slices.Clone(e.value), Version: e.version, ModIndex: e.modIndex, LeaseId: e.leaseID}
}

func (e *entry) ver() uint64 {
	if e == nil {
		return 0
	}
	return e.version
}

// write sets key's value at index, creating it if absent and moving it between leases.
func (s *state) write(index uint64, key string, value []byte, leaseID uint64) {
	e := s.keys[key]
	if e == nil {
		e = &entry{createIndex: index}
		s.keys[key] = e
	} else {
		s.dataBytes -= len(key) + len(e.value)
	}
	s.dataBytes += len(key) + len(value)
	if e.leaseID != leaseID {
		s.detach(key, e.leaseID)
		s.attach(key, leaseID)
	}
	e.value = slices.Clone(value)
	e.version++
	e.modIndex = index
	e.leaseID = leaseID
}

// remove deletes key and detaches it from its lease.
func (s *state) remove(key string) {
	if e := s.keys[key]; e != nil {
		s.detach(key, e.leaseID)
		delete(s.keys, key)
		s.dataBytes -= len(key) + len(e.value)
	}
}

func (s *state) attach(key string, id uint64) {
	if l := s.leases[id]; l != nil {
		l.keys[key] = struct{}{}
	}
}

func (s *state) detach(key string, id uint64) {
	if l := s.leases[id]; l != nil {
		delete(l.keys, key)
	}
}
