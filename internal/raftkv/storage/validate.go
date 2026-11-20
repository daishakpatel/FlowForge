package storage

import (
	"fmt"

	"go.etcd.io/bbolt"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// checkIdentity records id in a new file, or refuses a file recorded for another node.
func checkIdentity(tx *bbolt.Tx, id Identity) error {
	meta := tx.Bucket(bucketMeta)
	want := pair(id.ClusterID, id.NodeID)
	raw := meta.Get(keyIdentity)
	if raw == nil {
		if err := meta.Put(keyIdentity, want); err != nil {
			return fmt.Errorf("put identity: %w", err)
		}
		return nil
	}
	gotCluster, gotNode, ok := unpair(raw)
	if !ok {
		return corrupt("identity is %d bytes", len(raw))
	}
	if gotCluster != id.ClusterID || gotNode != id.NodeID {
		return fmt.Errorf("%w: the file is cluster %d node %d, not cluster %d node %d",
			ErrIdentityMismatch, gotCluster, gotNode, id.ClusterID, id.NodeID)
	}
	return nil
}

// validate checks the stored log's invariants at open:
//   - the compaction marker is at or before the latest snapshot (Compact refuses to pass it);
//   - entries run without gaps from the marker's index + 1, each stored under its own index;
//   - terms never go down along the log, starting from the marker's term;
//   - checkInvariants: the hard state against the log's bounds.
//
// Save writes entries and hard state in one transaction, so a crash can't break these; a
// violation means the file was damaged or written by something else.
func validate(tx *bbolt.Tx) error {
	st, err := readState(tx)
	if err != nil {
		return err
	}
	if st.compactIndex > st.snapIndex {
		return corrupt("log compacted to %d, past the latest snapshot at %d", st.compactIndex, st.snapIndex)
	}
	want, prevTerm := st.compactIndex+1, st.compactTerm
	c := tx.Bucket(bucketEntries).Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if len(k) != 8 {
			return corrupt("entry key of %d bytes", len(k))
		}
		idx := keyIndex(k)
		if idx != want {
			return corrupt("entry %d where %d was expected", idx, want)
		}
		e := &pb.Entry{}
		if err := proto.Unmarshal(v, e); err != nil {
			return corrupt("entry %d: %v", idx, err)
		}
		if e.GetIndex() != idx {
			return corrupt("entry stored at %d says index %d", idx, e.GetIndex())
		}
		if e.GetTerm() < prevTerm {
			return corrupt("entry %d has term %d after term %d", idx, e.GetTerm(), prevTerm)
		}
		want, prevTerm = idx+1, e.GetTerm()
	}
	hs := &pb.HardState{}
	if err := unmarshalIf(tx.Bucket(bucketMeta).Get(keyHardState), hs); err != nil {
		return corrupt("hard state: %v", err)
	}
	if problem := checkInvariants(hs, st.compactIndex, st.last, prevTerm); problem != "" {
		return corrupt("%s", problem)
	}
	return nil
}

func corrupt(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrCorruptStorage, fmt.Sprintf(format, args...))
}
