package storage

import (
	"fmt"
	"sync"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// Mem is an in-memory Storage for tests, wrapping the raft library's MemoryStorage. It checks
// every write before applying it, so a rejected write changes nothing, and it returns errors
// where MemoryStorage would panic. Every method holds mu, so readers never see half a Save,
// and values are copied in and out, so callers share no memory with it. Nothing survives the
// process.
type Mem struct {
	mu sync.Mutex
	ms *raft.MemoryStorage
}

var _ Storage = (*Mem)(nil)

// NewMem returns an empty Mem.
func NewMem() *Mem { return &Mem{ms: raft.NewMemoryStorage()} }

// InitialState implements raft.Storage.
func (m *Mem) InitialState() (*pb.HardState, *pb.ConfState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	hs, cs, err := m.ms.InitialState()
	if hs == nil {
		hs = &pb.HardState{}
	}
	return proto.Clone(hs).(*pb.HardState), proto.Clone(cs).(*pb.ConfState), err
}

// Entries implements raft.Storage.
func (m *Mem) Entries(lo, hi, maxSize uint64) ([]*pb.Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	last, _ := m.ms.LastIndex() // MemoryStorage never fails
	if hi > last+1 {
		return nil, raft.ErrUnavailable // MemoryStorage would panic
	}
	return m.ms.Entries(lo, hi, maxSize)
}

// Term implements raft.Storage.
func (m *Mem) Term(i uint64) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ms.Term(i)
}

// LastIndex implements raft.Storage.
func (m *Mem) LastIndex() (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ms.LastIndex()
}

// FirstIndex implements raft.Storage.
func (m *Mem) FirstIndex() (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ms.FirstIndex()
}

// Snapshot implements raft.Storage; MemoryStorage returns a copy.
func (m *Mem) Snapshot() (*pb.Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ms.Snapshot()
}

// LoadSnapshot implements Storage.
func (m *Mem) LoadSnapshot() (*pb.Snapshot, error) { return m.Snapshot() }

// Save implements Storage. Every check, including checkInvariants on the state the write would
// leave, runs before the first change.
func (m *Mem) Save(hs *pb.HardState, ents []*pb.Entry, snap *pb.Snapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	first, _ := m.ms.FirstIndex()
	last, _ := m.ms.LastIndex()
	lastTerm, _ := m.ms.Term(last)
	compact := first - 1
	hasSnap := snap != nil && !raft.IsEmptySnap(snap)
	if hasSnap {
		cur, _ := m.ms.Snapshot()
		if err := checkSnapshot(snap, cur.GetMetadata().GetIndex()); err != nil {
			return err
		}
		md := snap.GetMetadata()
		compact, last, lastTerm = md.GetIndex(), md.GetIndex(), md.GetTerm()
		first = compact + 1
	}
	app, err := toAppend(first, last, ents)
	if err != nil {
		return err
	}
	if len(app) > 0 {
		last, lastTerm = app[len(app)-1].GetIndex(), app[len(app)-1].GetTerm()
	}
	newHS := hs != nil && !raft.IsEmptyHardState(hs)
	after, _, _ := m.ms.InitialState()
	if newHS {
		after = hs
	}
	if problem := checkInvariants(after, compact, last, lastTerm); problem != "" {
		return fmt.Errorf("%w: %s", ErrBrokenInvariant, problem)
	}

	if hasSnap {
		if err := m.ms.ApplySnapshot(snap); err != nil { // ApplySnapshot copies snap
			return fmt.Errorf("apply snapshot: %w", err)
		}
	}
	copied := make([]*pb.Entry, len(app))
	for i, e := range app {
		copied[i] = proto.Clone(e).(*pb.Entry)
	}
	if err := m.ms.Append(copied); err != nil {
		return fmt.Errorf("append entries: %w", err)
	}
	if newHS {
		_ = m.ms.SetHardState(proto.Clone(hs).(*pb.HardState)) // never fails
	}
	return nil
}

// SaveSnapshot implements Storage.
func (m *Mem) SaveSnapshot(snap *pb.Snapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, _ := m.ms.Snapshot()
	if err := checkSnapshot(snap, cur.GetMetadata().GetIndex()); err != nil {
		return err
	}
	idx := snap.GetMetadata().GetIndex()
	if last, _ := m.ms.LastIndex(); idx > last {
		return raft.ErrUnavailable
	}
	if hs, _, _ := m.ms.InitialState(); idx > hs.GetCommit() {
		return fmt.Errorf("%w: snapshot at %d is past the commit index %d", ErrBrokenInvariant, idx, hs.GetCommit())
	}
	term, err := m.ms.Term(idx)
	if err != nil {
		return err
	}
	if term != snap.GetMetadata().GetTerm() {
		return fmt.Errorf("snapshot at %d has term %d, the log has %d", idx, snap.GetMetadata().GetTerm(), term)
	}
	_, err = m.ms.CreateSnapshot(idx, snap.GetMetadata().GetConfState(), snap.GetData()) // copies the ConfState
	return err
}

// Compact implements Storage.
func (m *Mem) Compact(index uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	first, _ := m.ms.FirstIndex()
	last, _ := m.ms.LastIndex()
	cur, _ := m.ms.Snapshot()
	switch {
	case index < first:
		return raft.ErrCompacted
	case index > last:
		return raft.ErrUnavailable
	case index > cur.GetMetadata().GetIndex():
		return ErrCompactBeyondSnapshot
	}
	return m.ms.Compact(index)
}

// Close implements Storage; it does nothing.
func (m *Mem) Close() error { return nil }
