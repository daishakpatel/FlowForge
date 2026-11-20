// Package storagetest is the conformance suite every storage.Storage must pass (task 04.01):
// the raft.Storage contract as the etcd raft library's MemoryStorage implements it, plus the
// write methods Storage adds.
package storagetest

import (
	"bytes"
	"encoding/binary"
	"errors"
	"slices"
	"testing"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/daishakpatel/flowforge/internal/raftkv/storage"
)

// Factory returns a new, empty Storage; the suite closes it.
type Factory func(t *testing.T) storage.Storage

// Run runs every conformance test against storages made by factory.
func Run(t *testing.T, factory Factory) {
	tests := []struct {
		name string
		fn   func(t *testing.T, s storage.Storage)
	}{
		{"Empty", testEmpty},
		{"AppendAndRead", testAppendAndRead},
		{"EntriesMaxSize", testEntriesMaxSize},
		{"OverwriteSuffix", testOverwriteSuffix},
		{"GapRejectedAtomically", testGapRejectedAtomically},
		{"AppendDropsCompacted", testAppendDropsCompacted},
		{"SaveSnapshotAndLoad", testSaveSnapshotAndLoad},
		{"Compact", testCompact},
		{"SaveAppliesSnapshot", testSaveAppliesSnapshot},
		{"ReturnedSlicesAreCopies", testReturnedSlicesAreCopies},
		{"InvariantsEnforced", testInvariantsEnforced},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := factory(t)
			t.Cleanup(func() {
				if err := s.Close(); err != nil {
					t.Errorf("close: %v", err)
				}
			})
			tc.fn(t, s)
		})
	}
}

// Entry returns entry index at term with a small payload.
func Entry(index, term uint64) *pb.Entry {
	return &pb.Entry{Index: proto.Uint64(index), Term: proto.Uint64(term), Type: pb.EntryType_EntryNormal.Enum(),
		Data: binary.BigEndian.AppendUint64(nil, index<<8|term)}
}

// Entries returns consecutive entries from first, one per term.
func Entries(first uint64, terms ...uint64) []*pb.Entry {
	out := make([]*pb.Entry, len(terms))
	for i, term := range terms {
		out[i] = Entry(first+uint64(i), term)
	}
	return out
}

// HardState returns a hard state.
func HardState(term, vote, commit uint64) *pb.HardState {
	return &pb.HardState{Term: proto.Uint64(term), Vote: proto.Uint64(vote), Commit: proto.Uint64(commit)}
}

// Snapshot returns a snapshot at index and term with voters 1, 2, 3 and data.
func Snapshot(index, term uint64, data []byte) *pb.Snapshot {
	return &pb.Snapshot{Data: data, Metadata: &pb.SnapshotMetadata{Index: proto.Uint64(index), Term: proto.Uint64(term),
		ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}}}}
}

// MustSave calls Save and fails the test on error.
func MustSave(t *testing.T, s storage.Storage, hs *pb.HardState, ents []*pb.Entry, snap *pb.Snapshot) {
	t.Helper()
	if err := s.Save(hs, ents, snap); err != nil {
		t.Fatalf("save: %v", err)
	}
}

// Bounds checks FirstIndex and LastIndex.
func Bounds(t *testing.T, s storage.Storage, first, last uint64) {
	t.Helper()
	f, err := s.FirstIndex()
	if err != nil {
		t.Fatal(err)
	}
	l, err := s.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	if f != first || l != last {
		t.Fatalf("first, last = %d, %d; want %d, %d", f, l, first, last)
	}
}

// EntriesEqual checks Entries(lo, hi, maxSize) against want.
func EntriesEqual(t *testing.T, s storage.Storage, lo, hi, maxSize uint64, want []*pb.Entry) {
	t.Helper()
	got, err := s.Entries(lo, hi, maxSize)
	if err != nil {
		t.Fatalf("entries [%d, %d): %v", lo, hi, err)
	}
	if len(got) != len(want) {
		t.Fatalf("entries [%d, %d) returned %d entries, want %d", lo, hi, len(got), len(want))
	}
	for i := range got {
		if !proto.Equal(got[i], want[i]) {
			t.Fatalf("entry %d is %v, want %v", i, got[i], want[i])
		}
	}
}

// SnapshotEqual reports whether two snapshots have the same index, term, data and ConfState.
// proto.Equal would also tell an explicit proto2 false (auto_leave) from an unset field.
func SnapshotEqual(a, b *pb.Snapshot) bool {
	am, bm := a.GetMetadata(), b.GetMetadata()
	return am.GetIndex() == bm.GetIndex() && am.GetTerm() == bm.GetTerm() && bytes.Equal(a.GetData(), b.GetData()) &&
		ConfStateEqual(am.GetConfState(), bm.GetConfState())
}

// ConfStateEqual compares two ConfStates field by field.
func ConfStateEqual(a, b *pb.ConfState) bool {
	return slices.Equal(a.GetVoters(), b.GetVoters()) && slices.Equal(a.GetLearners(), b.GetLearners()) &&
		slices.Equal(a.GetVotersOutgoing(), b.GetVotersOutgoing()) &&
		slices.Equal(a.GetLearnersNext(), b.GetLearnersNext()) && a.GetAutoLeave() == b.GetAutoLeave()
}

// TermIs checks Term(i).
func TermIs(t *testing.T, s storage.Storage, i, want uint64, wantErr error) {
	t.Helper()
	got, err := s.Term(i)
	if !errors.Is(err, wantErr) || (wantErr == nil && got != want) {
		t.Fatalf("term(%d) = %d, %v; want %d, %v", i, got, err, want, wantErr)
	}
}

func isErr(t *testing.T, what string, err, want error) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("%s: error %v, want %v", what, err, want)
	}
}

func testEmpty(t *testing.T, s storage.Storage) {
	Bounds(t, s, 1, 0)
	hs, cs, err := s.InitialState()
	if err != nil || !raft.IsEmptyHardState(hs) || cs == nil || len(cs.GetVoters()) != 0 {
		t.Fatalf("initial state %v, %v, %v; want empty", hs, cs, err)
	}
	snap, err := s.Snapshot()
	if err != nil || !raft.IsEmptySnap(snap) {
		t.Fatalf("snapshot %v, %v; want empty", snap, err)
	}
	TermIs(t, s, 0, 0, nil)
	TermIs(t, s, 1, 0, raft.ErrUnavailable)
	_, err = s.Entries(1, 1, 100)
	isErr(t, "entries of an empty log", err, raft.ErrUnavailable)
}

func testAppendAndRead(t *testing.T, s storage.Storage) {
	ents := Entries(1, 1, 1, 2, 2, 3)
	MustSave(t, s, HardState(3, 2, 4), ents, nil)
	Bounds(t, s, 1, 5)
	EntriesEqual(t, s, 1, 6, 1<<20, ents)
	EntriesEqual(t, s, 2, 4, 1<<20, ents[1:3])
	TermIs(t, s, 3, 2, nil)
	TermIs(t, s, 5, 3, nil)
	TermIs(t, s, 6, 0, raft.ErrUnavailable)
	_, err := s.Entries(1, 7, 1<<20)
	isErr(t, "entries past the last index", err, raft.ErrUnavailable)
	hs, _, err := s.InitialState()
	if err != nil || !proto.Equal(hs, HardState(3, 2, 4)) {
		t.Fatalf("hard state %v, %v; want term 3 vote 2 commit 4", hs, err)
	}
	MustSave(t, s, HardState(4, 0, 5), nil, nil) // hard state alone
	if hs, _, _ := s.InitialState(); !proto.Equal(hs, HardState(4, 0, 5)) {
		t.Fatalf("hard state %v after an update", hs)
	}
	MustSave(t, s, &pb.HardState{}, nil, nil) // an empty hard state is not written
	if hs, _, _ := s.InitialState(); !proto.Equal(hs, HardState(4, 0, 5)) {
		t.Fatalf("an empty hard state overwrote %v", hs)
	}
}

func testEntriesMaxSize(t *testing.T, s storage.Storage) {
	ents := Entries(1, 1, 1, 1, 1, 1)
	MustSave(t, s, nil, ents, nil)
	one := uint64(proto.Size(ents[0])) //nolint:gosec // a size is never negative
	for _, tc := range []struct {
		maxSize uint64
		want    int
	}{{0, 1}, {one, 1}, {2*one - 1, 1}, {2 * one, 2}, {3*one + one/2, 3}, {1 << 20, 5}} {
		EntriesEqual(t, s, 1, 6, tc.maxSize, ents[:tc.want])
	}
}

func testOverwriteSuffix(t *testing.T, s storage.Storage) {
	MustSave(t, s, nil, Entries(1, 1, 1, 1, 1, 1), nil)
	MustSave(t, s, nil, Entries(3, 2, 2), nil) // a new leader replaced 3..5 with 3..4
	Bounds(t, s, 1, 4)
	TermIs(t, s, 2, 1, nil)
	TermIs(t, s, 3, 2, nil)
	TermIs(t, s, 5, 0, raft.ErrUnavailable)
	MustSave(t, s, nil, Entries(5, 2), nil) // appending at last+1
	Bounds(t, s, 1, 5)
}

func testGapRejectedAtomically(t *testing.T, s storage.Storage) {
	MustSave(t, s, HardState(1, 1, 3), Entries(1, 1, 1, 1), nil)
	err := s.Save(HardState(2, 1, 6), Entries(5, 2, 2), nil)
	isErr(t, "entries after a gap", err, storage.ErrLogGap)
	err = s.Save(HardState(2, 1, 6), []*pb.Entry{Entry(4, 2), Entry(6, 2)}, nil)
	isErr(t, "non-consecutive entries", err, storage.ErrLogGap)
	Bounds(t, s, 1, 3)
	if hs, _, _ := s.InitialState(); !proto.Equal(hs, HardState(1, 1, 3)) {
		t.Fatalf("a rejected Save wrote its hard state: %v", hs)
	}
}

func testAppendDropsCompacted(t *testing.T, s storage.Storage) {
	MustSave(t, s, HardState(2, 1, 5), Entries(1, 1, 1, 1, 1, 1), nil)
	if err := s.SaveSnapshot(Snapshot(3, 1, []byte("s"))); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(3); err != nil {
		t.Fatal(err)
	}
	MustSave(t, s, nil, Entries(1, 1, 1), nil) // entirely compacted: a no-op
	Bounds(t, s, 4, 5)
	MustSave(t, s, nil, Entries(2, 1, 1, 2, 2), nil) // 2..3 dropped, 4..5 replaced
	Bounds(t, s, 4, 5)
	TermIs(t, s, 4, 2, nil)
}

func testSaveSnapshotAndLoad(t *testing.T, s storage.Storage) {
	MustSave(t, s, HardState(2, 1, 4), Entries(1, 1, 1, 2, 2), nil)
	snap := Snapshot(3, 2, []byte("state at 3"))
	if err := s.SaveSnapshot(snap); err != nil {
		t.Fatal(err)
	}
	for _, get := range []func() (*pb.Snapshot, error){s.Snapshot, s.LoadSnapshot} {
		got, err := get()
		if err != nil || !SnapshotEqual(got, snap) {
			t.Fatalf("snapshot %v, %v; want %v", got, err, snap)
		}
	}
	if _, cs, _ := s.InitialState(); !ConfStateEqual(cs, snap.GetMetadata().GetConfState()) {
		t.Fatalf("conf state %v, want the snapshot's", cs)
	}
	Bounds(t, s, 1, 4) // the log is untouched
	isErr(t, "an older snapshot", s.SaveSnapshot(Snapshot(2, 1, nil)), raft.ErrSnapOutOfDate)
	isErr(t, "the same snapshot again", s.SaveSnapshot(Snapshot(3, 2, nil)), raft.ErrSnapOutOfDate)
	isErr(t, "a snapshot past the log", s.SaveSnapshot(Snapshot(9, 2, nil)), raft.ErrUnavailable)
	if err := s.SaveSnapshot(Snapshot(4, 1, nil)); err == nil {
		t.Fatal("a snapshot with the wrong term was accepted")
	}
	big := Snapshot(4, 2, bytes.Repeat([]byte{1}, storage.MaxSnapshotBytes+1))
	isErr(t, "a snapshot over the limit", s.SaveSnapshot(big), storage.ErrSnapshotTooLarge)
	if got, _ := s.LoadSnapshot(); !SnapshotEqual(got, snap) {
		t.Fatalf("a rejected snapshot replaced the current one: %v", got.GetMetadata())
	}
}

func testCompact(t *testing.T, s storage.Storage) {
	ents := Entries(1, 1, 1, 2, 2, 3)
	MustSave(t, s, HardState(3, 1, 5), ents, nil)
	isErr(t, "compacting with no snapshot", s.Compact(2), storage.ErrCompactBeyondSnapshot)
	if err := s.SaveSnapshot(Snapshot(4, 2, []byte("s"))); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(3); err != nil {
		t.Fatal(err)
	}
	Bounds(t, s, 4, 5)
	TermIs(t, s, 3, 2, nil) // kept for matching
	TermIs(t, s, 2, 0, raft.ErrCompacted)
	_, err := s.Entries(3, 5, 1<<20)
	isErr(t, "entries of a compacted index", err, raft.ErrCompacted)
	EntriesEqual(t, s, 4, 6, 1<<20, ents[3:])
	isErr(t, "compacting again", s.Compact(3), raft.ErrCompacted)
	isErr(t, "compacting past the log", s.Compact(9), raft.ErrUnavailable)
	isErr(t, "compacting past the snapshot", s.Compact(5), storage.ErrCompactBeyondSnapshot)
	if err := s.Compact(4); err != nil { // up to the snapshot: nothing left before 5
		t.Fatal(err)
	}
	Bounds(t, s, 5, 5)
	TermIs(t, s, 4, 2, nil)
}

func testSaveAppliesSnapshot(t *testing.T, s storage.Storage) {
	MustSave(t, s, HardState(2, 1, 3), Entries(1, 1, 1, 2), nil)
	snap := Snapshot(10, 4, []byte("leader state"))
	MustSave(t, s, HardState(5, 0, 12), Entries(11, 4, 5), snap) // install + append in one Ready
	Bounds(t, s, 11, 12)
	TermIs(t, s, 10, 4, nil)
	TermIs(t, s, 12, 5, nil)
	_, err := s.Entries(10, 12, 1<<20)
	isErr(t, "entries before the snapshot", err, raft.ErrCompacted)
	hs, cs, err := s.InitialState()
	if err != nil || !proto.Equal(hs, HardState(5, 0, 12)) || !ConfStateEqual(cs, snap.GetMetadata().GetConfState()) {
		t.Fatalf("initial state %v, %v, %v", hs, cs, err)
	}
	if got, _ := s.Snapshot(); !SnapshotEqual(got, snap) {
		t.Fatalf("snapshot %v", got.GetMetadata())
	}
	err = s.Save(HardState(5, 0, 13), Entries(13, 5), Snapshot(9, 4, nil))
	isErr(t, "installing an older snapshot", err, raft.ErrSnapOutOfDate)
	Bounds(t, s, 11, 12) // nothing of the rejected Save was written
	isErr(t, "a snapshot over the limit", s.Save(nil, nil, Snapshot(20, 5, bytes.Repeat([]byte{1}, storage.MaxSnapshotBytes+1))), storage.ErrSnapshotTooLarge)
}

func testReturnedSlicesAreCopies(t *testing.T, s storage.Storage) {
	ents := Entries(1, 1, 1, 1)
	MustSave(t, s, nil, ents, nil)
	got, err := s.Entries(1, 3, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	_ = append(got, Entry(99, 9)) // the caller may append (raft.Storage contract)
	EntriesEqual(t, s, 1, 4, 1<<20, ents)
}

// testInvariantsEnforced: writes that would leave state the raft library can't restart from are
// refused whole (ErrBrokenInvariant), so OpenBolt never meets them.
func testInvariantsEnforced(t *testing.T, s storage.Storage) {
	MustSave(t, s, HardState(2, 1, 3), Entries(1, 1, 1, 2, 2), nil)
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"hard state term behind the log", s.Save(HardState(2, 1, 3), Entries(5, 3), nil)},
		{"entries of a newer term under the old hard state", s.Save(nil, Entries(5, 3), nil)},
		{"commit past the last index", s.Save(HardState(2, 1, 9), nil, nil)},
		{"commit before an installed snapshot", s.Save(nil, nil, Snapshot(10, 2, nil))},
		{"snapshot of an uncommitted index", s.SaveSnapshot(Snapshot(4, 2, nil))},
	} {
		isErr(t, tc.name, tc.err, storage.ErrBrokenInvariant)
	}
	Bounds(t, s, 1, 4)
	if hs, _, _ := s.InitialState(); !proto.Equal(hs, HardState(2, 1, 3)) {
		t.Fatalf("a refused write changed the hard state to %v", hs)
	}
	if snap, _ := s.Snapshot(); !raft.IsEmptySnap(snap) {
		t.Fatalf("a refused write stored snapshot %v", snap.GetMetadata())
	}
	MustSave(t, s, HardState(3, 1, 5), Entries(5, 3), nil) // the same entries with a valid hard state
}
