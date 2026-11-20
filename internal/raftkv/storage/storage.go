// Package storage persists a Raft node's log, hard state and snapshots (task 04.01,
// FR-RAFT-01, ADR-002). Storage satisfies the etcd raft library's raft.Storage, so a node reads
// its log through it, and adds the writes the Ready loop needs. Bolt is the durable
// implementation, and Mem the in-memory one for tests. Both pass the same conformance suite
// (storagetest). See docs/architecture/raft-storage.md.
package storage

import (
	"errors"
	"fmt"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

// MaxSnapshotBytes bounds a snapshot's data. Bolt keeps the data in one value, read whole into
// memory, so larger state machines are out of scope (docs/architecture/raft-storage.md).
const MaxSnapshotBytes = 64 << 20

var (
	// ErrCorruptStorage is returned by OpenBolt when the stored log breaks an invariant: a gap
	// in the indexes, a term that goes down, a commit index past the last entry, or a hard state
	// older than the last entry's term. The wrapping error names the problem.
	ErrCorruptStorage = errors.New("raft storage is corrupt")
	// ErrIdentityMismatch is returned by OpenBolt for a data directory created by a different
	// node or cluster.
	ErrIdentityMismatch = errors.New("raft storage belongs to another node")
	// ErrLogGap is returned by Save when the entries are not consecutive or would start after
	// LastIndex()+1. Nothing is written.
	ErrLogGap = errors.New("entries leave a gap in the raft log")
	// ErrCompactBeyondSnapshot is returned by Compact for an index past the latest snapshot:
	// after a restart, the entries between them could be recovered from neither.
	ErrCompactBeyondSnapshot = errors.New("compaction index is beyond the latest snapshot")
	// ErrSnapshotTooLarge is returned for snapshot data over MaxSnapshotBytes.
	ErrSnapshotTooLarge = errors.New("snapshot data exceeds MaxSnapshotBytes")
	// ErrBrokenInvariant is returned by a write that would leave state the raft library can't
	// restart from (see checkInvariants). Nothing is written.
	ErrBrokenInvariant = errors.New("write would break a raft storage invariant")
)

// Storage is a Raft node's durable state. The raft.Storage methods may be called from any
// goroutine (peer senders read Entries concurrently); the write methods are called by one
// goroutine, the node's Ready loop, and Bolt makes each of them durable before it returns.
//
// Ready-loop contract (ADR-002): persist with Save before sending the Ready's messages, then
// apply its committed entries, then Advance.
type Storage interface {
	raft.Storage

	// Save persists one Ready in one atomic, durable write, in this order:
	//  1. snap, unless nil or empty: replaces the whole log (its index becomes FirstIndex()-1)
	//     and the ConfState. A snapshot not newer than the current one is raft.ErrSnapOutOfDate.
	//  2. ents: appended after dropping those at or before FirstIndex()-1. Existing entries at
	//     or after ents[0] are replaced (a new leader overwrote them). Entries that are not
	//     consecutive, or start after LastIndex()+1, are ErrLogGap.
	//  3. hs, unless nil or empty.
	// On error nothing is written.
	Save(hs *pb.HardState, ents []*pb.Entry, snap *pb.Snapshot) error

	// SaveSnapshot stores a snapshot this node took of its own state machine at an index in
	// its log; the log is untouched (Compact trims it). The snapshot must be newer than the
	// current one (raft.ErrSnapOutOfDate), at or before LastIndex() (raft.ErrUnavailable), and
	// carry that entry's term. Its ConfState becomes InitialState's.
	SaveSnapshot(snap *pb.Snapshot) error

	// LoadSnapshot returns the latest snapshot, or an empty one (raft.IsEmptySnap).
	LoadSnapshot() (*pb.Snapshot, error)

	// Compact discards the entries up to and including index, keeping index's term for
	// matching. index must be after FirstIndex()-1 (raft.ErrCompacted), at or before
	// LastIndex() (raft.ErrUnavailable), and at or before the latest snapshot's index
	// (ErrCompactBeyondSnapshot).
	Compact(index uint64) error

	// Close releases the storage. No method may be called after it.
	Close() error
}

// checkInvariants reports how stored state breaks what the raft library needs to restart from it,
// or "" when it holds. compact is the entry before FirstIndex (index, term) and last the last
// entry (index, term), equal to compact when the log is empty:
//   - the hard state's term is at or after the last entry's term (raft's term never trails its log);
//   - its commit lies in [compact index, last index] (raft panics on restart otherwise);
//   - a log with a compaction marker has a hard state (a snapshot is always committed).
//
// OpenBolt reports a violation as ErrCorruptStorage; the writes refuse one as ErrBrokenInvariant.
func checkInvariants(hs *pb.HardState, compactIndex, lastIndex, lastTerm uint64) string {
	if raft.IsEmptyHardState(hs) {
		if compactIndex > 0 {
			return fmt.Sprintf("no hard state, but the log is compacted to %d", compactIndex)
		}
		return ""
	}
	switch {
	case hs.GetTerm() < lastTerm:
		return fmt.Sprintf("hard state term %d is older than the last entry's term %d", hs.GetTerm(), lastTerm)
	case hs.GetCommit() > lastIndex:
		return fmt.Sprintf("commit index %d is past the last index %d", hs.GetCommit(), lastIndex)
	case hs.GetCommit() < compactIndex:
		return fmt.Sprintf("commit index %d is before the compaction marker %d", hs.GetCommit(), compactIndex)
	}
	return ""
}

// checkSnapshot validates a snapshot passed to Save or SaveSnapshot against the current
// snapshot index cur.
func checkSnapshot(snap *pb.Snapshot, cur uint64) error {
	if len(snap.GetData()) > MaxSnapshotBytes {
		return fmt.Errorf("%w: %d bytes", ErrSnapshotTooLarge, len(snap.GetData()))
	}
	if idx := snap.GetMetadata().GetIndex(); idx == 0 || idx <= cur {
		return raft.ErrSnapOutOfDate
	}
	return nil
}

// toAppend returns the part of ents that Save must write to a log whose first available index
// is first and last index is last, or ErrLogGap.
func toAppend(first, last uint64, ents []*pb.Entry) ([]*pb.Entry, error) {
	if len(ents) == 0 {
		return nil, nil
	}
	for i := 1; i < len(ents); i++ {
		if ents[i].GetIndex() != ents[i-1].GetIndex()+1 {
			return nil, fmt.Errorf("%w: entry %d follows %d", ErrLogGap, ents[i].GetIndex(), ents[i-1].GetIndex())
		}
	}
	start := ents[0].GetIndex()
	if start > last+1 {
		return nil, fmt.Errorf("%w: entries start at %d, the log ends at %d", ErrLogGap, start, last)
	}
	if start+uint64(len(ents))-1 < first {
		return nil, nil // all already compacted
	}
	if start < first {
		ents = ents[first-start:]
	}
	return ents, nil
}
