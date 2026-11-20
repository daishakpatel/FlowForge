package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"go.etcd.io/bbolt"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// Bolt is the durable Storage: one bbolt file per node (docs/architecture/raft-storage.md).
//
// Layout:
//
//	meta/identity    cluster id, node id (16 bytes, big-endian)
//	meta/hardstate   raftpb.HardState
//	meta/confstate   raftpb.ConfState of the latest snapshot
//	meta/compacted   index and term of the entry before FirstIndex (16 bytes, big-endian)
//	meta/applied     reserved for the state machine's last applied index (04.03)
//	entries/<index>  raftpb.Entry, keyed by its index as 8 big-endian bytes
//	snap/meta        raftpb.SnapshotMetadata of the latest snapshot
//	snap/data        that snapshot's state machine bytes (≤ MaxSnapshotBytes)
//
// Every write method is one bbolt read-write transaction, which bbolt fsyncs on commit
// (Options.NoSync is false), so a write is atomic and durable when it returns. Reads use
// read-only transactions and may run concurrently with the single writer.
type Bolt struct {
	db *bbolt.DB
}

var _ Storage = (*Bolt)(nil)

// Identity names the node a data directory belongs to. Both ids are non-zero.
type Identity struct {
	ClusterID uint64
	NodeID    uint64
}

var (
	bucketMeta    = []byte("meta")
	bucketEntries = []byte("entries")
	bucketSnap    = []byte("snap")
	keyIdentity   = []byte("identity")
	keyHardState  = []byte("hardstate")
	keyConfState  = []byte("confstate")
	keyCompacted  = []byte("compacted")
	keySnapMeta   = []byte("meta")
	keySnapData   = []byte("data")
)

// openTimeout bounds the wait for bbolt's file lock, held by another process using the file.
const openTimeout = time.Second

// OpenBolt opens or creates the storage file at path for node id. It refuses a file created
// for another identity (ErrIdentityMismatch) and a file whose stored log breaks an invariant
// (ErrCorruptStorage).
func OpenBolt(path string, id Identity) (*Bolt, error) {
	if id.ClusterID == 0 || id.NodeID == 0 {
		return nil, errors.New("open raft storage: cluster and node ids must be non-zero")
	}
	db, err := bbolt.Open(path, 0o600, &bbolt.Options{Timeout: openTimeout, NoSync: false})
	if err != nil {
		return nil, fmt.Errorf("open raft storage %s: %w", path, err)
	}
	err = db.Update(func(tx *bbolt.Tx) error {
		for _, name := range [][]byte{bucketMeta, bucketEntries, bucketSnap} {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return fmt.Errorf("create bucket %s: %w", name, err)
			}
		}
		return checkIdentity(tx, id)
	})
	if err == nil {
		err = db.View(validate)
	}
	if err != nil {
		_ = db.Close() // the open error is the one to report
		return nil, fmt.Errorf("open raft storage %s: %w", path, err)
	}
	return &Bolt{db: db}, nil
}

// Close implements Storage.
func (b *Bolt) Close() error { return b.db.Close() }

// InitialState implements raft.Storage.
func (b *Bolt) InitialState() (*pb.HardState, *pb.ConfState, error) {
	hs, cs := &pb.HardState{}, &pb.ConfState{}
	err := b.db.View(func(tx *bbolt.Tx) error {
		meta := tx.Bucket(bucketMeta)
		if err := unmarshalIf(meta.Get(keyHardState), hs); err != nil {
			return fmt.Errorf("hard state: %w", err)
		}
		if err := unmarshalIf(meta.Get(keyConfState), cs); err != nil {
			return fmt.Errorf("conf state: %w", err)
		}
		return nil
	})
	return hs, cs, err
}

// Entries implements raft.Storage. It reads from a read-only transaction, so it is safe
// alongside the writer, and returns freshly allocated entries.
func (b *Bolt) Entries(lo, hi, maxSize uint64) ([]*pb.Entry, error) {
	var out []*pb.Entry
	err := b.db.View(func(tx *bbolt.Tx) error {
		st, err := readLog(tx)
		switch {
		case err != nil:
			return err
		case lo <= st.compactIndex:
			return raft.ErrCompacted
		case hi > st.last+1, st.last == st.compactIndex:
			return raft.ErrUnavailable
		}
		// The raft library's size rule: always the first entry, then while the total fits.
		var total uint64
		c := tx.Bucket(bucketEntries).Cursor()
		for k, v := c.Seek(indexKey(lo)); k != nil && keyIndex(k) < hi; k, v = c.Next() {
			total += uint64(len(v))
			if len(out) > 0 && total > maxSize {
				break
			}
			e := &pb.Entry{}
			if err := proto.Unmarshal(v, e); err != nil {
				return fmt.Errorf("entry %d: %w", keyIndex(k), err)
			}
			out = append(out, e)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Term implements raft.Storage.
func (b *Bolt) Term(i uint64) (uint64, error) {
	var term uint64
	err := b.db.View(func(tx *bbolt.Tx) error {
		st, err := readLog(tx)
		if err != nil {
			return err
		}
		term, err = termAt(tx, st, i)
		return err
	})
	return term, err
}

// LastIndex implements raft.Storage.
func (b *Bolt) LastIndex() (uint64, error) {
	var last uint64
	err := b.db.View(func(tx *bbolt.Tx) error {
		st, err := readLog(tx)
		last = st.last
		return err
	})
	return last, err
}

// FirstIndex implements raft.Storage.
func (b *Bolt) FirstIndex() (uint64, error) {
	var first uint64
	err := b.db.View(func(tx *bbolt.Tx) error {
		st, err := readLog(tx)
		first = st.compactIndex + 1
		return err
	})
	return first, err
}

// Snapshot implements raft.Storage.
func (b *Bolt) Snapshot() (*pb.Snapshot, error) { return b.LoadSnapshot() }

// LoadSnapshot implements Storage.
func (b *Bolt) LoadSnapshot() (*pb.Snapshot, error) {
	snap := pb.EnsureSnapshot(nil)
	err := b.db.View(func(tx *bbolt.Tx) error {
		sb := tx.Bucket(bucketSnap)
		raw := sb.Get(keySnapMeta)
		if raw == nil {
			return nil
		}
		if err := proto.Unmarshal(raw, snap.Metadata); err != nil {
			return fmt.Errorf("snapshot metadata: %w", err)
		}
		snap.Data = bytes.Clone(sb.Get(keySnapData)) // bbolt values die with the transaction
		return nil
	})
	if err != nil {
		return nil, err
	}
	return snap, nil
}

// Save implements Storage: snapshot, entries and hard state in one fsynced transaction.
func (b *Bolt) Save(hs *pb.HardState, ents []*pb.Entry, snap *pb.Snapshot) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		st, err := readState(tx)
		if err != nil {
			return err
		}
		first, last := st.compactIndex+1, st.last
		hasSnap := snap != nil && !raft.IsEmptySnap(snap)
		if hasSnap {
			if err := checkSnapshot(snap, st.snapIndex); err != nil {
				return err
			}
			first, last = snap.GetMetadata().GetIndex()+1, snap.GetMetadata().GetIndex()
		}
		app, err := toAppend(first, last, ents)
		if err != nil {
			return err
		}
		if hasSnap {
			if err := installSnapshot(tx, snap); err != nil {
				return err
			}
		}
		if err := appendEntries(tx, app); err != nil {
			return err
		}
		if hs != nil && !raft.IsEmptyHardState(hs) {
			if err := putProto(tx.Bucket(bucketMeta), keyHardState, hs); err != nil {
				return err
			}
		}
		return checkStored(tx) // a violation rolls the whole Save back
	})
}

// checkStored applies checkInvariants to the transaction's current state.
func checkStored(tx *bbolt.Tx) error {
	st, err := readLog(tx)
	if err != nil {
		return err
	}
	lastTerm, err := termAt(tx, st, st.last)
	if err != nil {
		return err
	}
	hs := &pb.HardState{}
	if err := unmarshalIf(tx.Bucket(bucketMeta).Get(keyHardState), hs); err != nil {
		return corrupt("hard state: %v", err)
	}
	if problem := checkInvariants(hs, st.compactIndex, st.last, lastTerm); problem != "" {
		return fmt.Errorf("%w: %s", ErrBrokenInvariant, problem)
	}
	return nil
}

// SaveSnapshot implements Storage.
func (b *Bolt) SaveSnapshot(snap *pb.Snapshot) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		st, err := readState(tx)
		if err != nil {
			return err
		}
		if err := checkSnapshot(snap, st.snapIndex); err != nil {
			return err
		}
		idx := snap.GetMetadata().GetIndex()
		if idx > st.last {
			return raft.ErrUnavailable
		}
		hs := &pb.HardState{}
		if err := unmarshalIf(tx.Bucket(bucketMeta).Get(keyHardState), hs); err != nil {
			return corrupt("hard state: %v", err)
		}
		if idx > hs.GetCommit() {
			return fmt.Errorf("%w: snapshot at %d is past the commit index %d", ErrBrokenInvariant, idx, hs.GetCommit())
		}
		term, err := termAt(tx, st.logState, idx)
		if err != nil {
			return err
		}
		if term != snap.GetMetadata().GetTerm() {
			return fmt.Errorf("snapshot at %d has term %d, the log has %d", idx, snap.GetMetadata().GetTerm(), term)
		}
		return putSnapshot(tx, snap)
	})
}

// Compact implements Storage.
func (b *Bolt) Compact(index uint64) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		st, err := readState(tx)
		switch {
		case err != nil:
			return err
		case index <= st.compactIndex:
			return raft.ErrCompacted
		case index > st.last:
			return raft.ErrUnavailable
		case index > st.snapIndex:
			return ErrCompactBeyondSnapshot
		}
		term, err := termAt(tx, st.logState, index)
		if err != nil {
			return err
		}
		c := tx.Bucket(bucketEntries).Cursor()
		for k, _ := c.First(); k != nil && keyIndex(k) <= index; k, _ = c.First() {
			if err := c.Delete(); err != nil {
				return fmt.Errorf("delete entry %d: %w", keyIndex(k), err)
			}
		}
		return tx.Bucket(bucketMeta).Put(keyCompacted, pair(index, term))
	})
}

// logState is the stored log's bounds: what every read needs, cheap to get (one 16-byte value
// and one cursor seek).
type logState struct {
	compactIndex, compactTerm uint64 // the entry before FirstIndex (0, 0 for a new log)
	last                      uint64 // LastIndex
}

// writeState adds what the write paths also need.
type writeState struct {
	logState
	snapIndex uint64 // the latest snapshot's index, 0 if none
}

func readLog(tx *bbolt.Tx) (logState, error) {
	var st logState
	if raw := tx.Bucket(bucketMeta).Get(keyCompacted); raw != nil {
		var ok bool
		if st.compactIndex, st.compactTerm, ok = unpair(raw); !ok {
			return st, corrupt("compaction marker is %d bytes", len(raw))
		}
	}
	st.last = st.compactIndex
	if k, _ := tx.Bucket(bucketEntries).Cursor().Last(); k != nil {
		st.last = keyIndex(k)
	}
	return st, nil
}

func readState(tx *bbolt.Tx) (writeState, error) {
	log, err := readLog(tx)
	st := writeState{logState: log}
	if err != nil {
		return st, err
	}
	if raw := tx.Bucket(bucketSnap).Get(keySnapMeta); raw != nil {
		md := &pb.SnapshotMetadata{}
		if err := proto.Unmarshal(raw, md); err != nil {
			return st, corrupt("snapshot metadata: %v", err)
		}
		st.snapIndex = md.GetIndex()
	}
	return st, nil
}

// termAt returns the term of entry i of the log st describes (the compaction marker's at
// FirstIndex-1).
func termAt(tx *bbolt.Tx, st logState, i uint64) (uint64, error) {
	switch {
	case i < st.compactIndex:
		return 0, raft.ErrCompacted
	case i == st.compactIndex:
		return st.compactTerm, nil
	}
	raw := tx.Bucket(bucketEntries).Get(indexKey(i))
	if raw == nil {
		return 0, raft.ErrUnavailable
	}
	e := &pb.Entry{}
	if err := proto.Unmarshal(raw, e); err != nil {
		return 0, fmt.Errorf("entry %d: %w", i, err)
	}
	return e.GetTerm(), nil
}

// installSnapshot replaces the log with snap: no entries, the marker at the snapshot.
func installSnapshot(tx *bbolt.Tx, snap *pb.Snapshot) error {
	if err := tx.DeleteBucket(bucketEntries); err != nil {
		return fmt.Errorf("drop the log: %w", err)
	}
	if _, err := tx.CreateBucket(bucketEntries); err != nil {
		return fmt.Errorf("recreate the log: %w", err)
	}
	md := snap.GetMetadata()
	if err := tx.Bucket(bucketMeta).Put(keyCompacted, pair(md.GetIndex(), md.GetTerm())); err != nil {
		return fmt.Errorf("put compaction marker: %w", err)
	}
	return putSnapshot(tx, snap)
}

// putSnapshot stores snap as the latest snapshot and its ConfState.
func putSnapshot(tx *bbolt.Tx, snap *pb.Snapshot) error {
	sb := tx.Bucket(bucketSnap)
	if err := putProto(sb, keySnapMeta, snap.GetMetadata()); err != nil {
		return err
	}
	if err := sb.Put(keySnapData, snap.GetData()); err != nil {
		return fmt.Errorf("put snapshot data: %w", err)
	}
	return putProto(tx.Bucket(bucketMeta), keyConfState, pb.EnsureConfState(snap.GetMetadata().GetConfState()))
}

// appendEntries replaces every entry from ents[0] on with ents (already checked by toAppend).
func appendEntries(tx *bbolt.Tx, ents []*pb.Entry) error {
	if len(ents) == 0 {
		return nil
	}
	eb := tx.Bucket(bucketEntries)
	c := eb.Cursor()
	start := indexKey(ents[0].GetIndex())
	for k, _ := c.Seek(start); k != nil; k, _ = c.Seek(start) {
		if err := c.Delete(); err != nil {
			return fmt.Errorf("truncate entry %d: %w", keyIndex(k), err)
		}
	}
	for _, e := range ents {
		if err := putProto(eb, indexKey(e.GetIndex()), e); err != nil {
			return err
		}
	}
	return nil
}

func putProto(b *bbolt.Bucket, key []byte, m proto.Message) error {
	raw, err := proto.Marshal(m)
	if err != nil {
		return fmt.Errorf("marshal %s: %w", key, err)
	}
	if err := b.Put(key, raw); err != nil {
		return fmt.Errorf("put %s: %w", key, err)
	}
	return nil
}

func unmarshalIf(raw []byte, m proto.Message) error {
	if raw == nil {
		return nil
	}
	return proto.Unmarshal(raw, m)
}

func indexKey(i uint64) []byte { return binary.BigEndian.AppendUint64(nil, i) }

func keyIndex(k []byte) uint64 { return binary.BigEndian.Uint64(k) }

func pair(a, b uint64) []byte {
	return binary.BigEndian.AppendUint64(binary.BigEndian.AppendUint64(nil, a), b)
}

// unpair decodes pair's 16 bytes.
func unpair(raw []byte) (a, b uint64, ok bool) {
	if len(raw) != 16 {
		return 0, 0, false
	}
	return binary.BigEndian.Uint64(raw[:8]), binary.BigEndian.Uint64(raw[8:]), true
}
