package node

import (
	"fmt"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// processReady handles every pending Ready, each in the contract order (ADR-002):
//  1. persist the hard state, entries and snapshot in one durable Save. Messages may promise
//     what they describe (a vote, an appended entry), so nothing is sent before it is on disk
//     (rule 1). This conservative order holds for leaders too.
//  2. send the messages;
//  3. apply the committed entries, which raft only hands out once they are committed and
//     persisted, skipping any at or below applied (rule 2);
//  4. Advance.
//
// A storage error stops the node: continuing could acknowledge what is not durable.
func (n *Node) processReady() error {
	for n.rn.HasReady() {
		rd := n.rn.Ready()
		if err := n.handleReady(rd); err != nil {
			return err
		}
		n.rn.Advance(rd)
	}
	return nil
}

func (n *Node) handleReady(rd raft.Ready) error {
	if !raft.IsEmptyHardState(rd.HardState) || len(rd.Entries) > 0 || !raft.IsEmptySnap(rd.Snapshot) {
		start := n.cfg.Clock.Now()
		if err := n.st.Save(rd.HardState, rd.Entries, rd.Snapshot); err != nil {
			return fmt.Errorf("persist ready: %w", err)
		}
		n.cfg.Metrics.write(n.cfg.Clock.Now().Sub(start).Seconds())
	}
	n.trackEntries(rd.Entries)
	if !raft.IsEmptySnap(rd.Snapshot) {
		if err := n.installSnapshot(rd.Snapshot); err != nil {
			return err
		}
	}
	if len(rd.Messages) > 0 {
		n.tr.Send(rd.Messages)
	}
	if err := n.applyCommitted(rd.CommittedEntries); err != nil {
		return err
	}
	n.readStates(rd.ReadStates)
	n.resolveReads()
	n.maybeSnapshot()
	return nil
}

// installSnapshot loads a snapshot the leader sent (already persisted by Save) into the Applier.
// Pending proposals at or below its index can't be told apart from those it includes, so their
// waiters are dropped without an answer: the caller sees ErrProposalTimeout (outcome unknown),
// never a definite failure.
func (n *Node) installSnapshot(snap *pb.Snapshot) error {
	idx := snap.GetMetadata().GetIndex()
	if err := n.app.Restore(snap.GetData()); err != nil {
		return fmt.Errorf("restore snapshot %d: %w", idx, err)
	}
	for id, p := range n.waiters {
		if p.index != 0 && p.index <= idx {
			delete(n.waiters, id)
		}
	}
	n.applied, n.snapIndex = idx, idx
	n.confState = snap.GetMetadata().GetConfState()
	return nil
}

func (n *Node) applyCommitted(ents []*pb.Entry) error {
	for _, e := range ents {
		idx := e.GetIndex()
		if idx <= n.applied {
			continue
		}
		switch e.GetType() {
		case pb.EntryNormal:
			if err := n.applyNormal(e); err != nil {
				return err
			}
		case pb.EntryConfChange:
			cc := &pb.ConfChange{}
			if err := proto.Unmarshal(e.GetData(), cc); err != nil {
				return fmt.Errorf("decode conf change at %d: %w", idx, err)
			}
			n.confState = n.rn.ApplyConfChange(cc)
		case pb.EntryConfChangeV2:
			cc := &pb.ConfChangeV2{}
			if err := proto.Unmarshal(e.GetData(), cc); err != nil {
				return fmt.Errorf("decode conf change at %d: %w", idx, err)
			}
			n.confState = n.rn.ApplyConfChange(cc)
		}
		n.applied = idx
	}
	n.failOverwritten()
	return nil
}

// applyNormal applies one normal entry. Empty entries are the no-ops a new leader appends; the
// Applier never sees them.
func (n *Node) applyNormal(e *pb.Entry) error {
	if len(e.GetData()) == 0 {
		return nil
	}
	id, payload, ok := decodeEntry(e.GetData())
	if !ok {
		return fmt.Errorf("entry %d has no proposal header: the log is corrupt", e.GetIndex())
	}
	res, err := n.app.Apply(payload, e.GetIndex())
	n.complete(id, Result{Index: e.GetIndex(), Data: res, Err: err})
	return nil
}

// maybeSnapshot snapshots the Applier every SnapshotEntries applied entries and compacts the log
// to SnapshotTrailing entries before the snapshot. A failure is logged and retried only after
// another SnapshotEntries entries (not on every Ready: snapshotting serialises the whole state);
// the log stays complete meanwhile, so nothing is lost but disk.
func (n *Node) maybeSnapshot() {
	if n.applied-max(n.snapIndex, n.snapTried) < n.cfg.SnapshotEntries {
		return
	}
	n.snapTried = n.applied
	if err := n.snapshot(); err != nil {
		n.cfg.Logger.Warn("raft node: snapshot failed, retrying after more entries", "index", n.applied, "err", err)
	}
}

func (n *Node) snapshot() error {
	data, err := n.app.Snapshot()
	if err != nil {
		return fmt.Errorf("applier snapshot: %w", err)
	}
	term, err := n.st.Term(n.applied)
	if err != nil {
		return fmt.Errorf("term of %d: %w", n.applied, err)
	}
	snap := &pb.Snapshot{Data: data, Metadata: &pb.SnapshotMetadata{
		Index: proto.Uint64(n.applied), Term: proto.Uint64(term), ConfState: n.confState,
	}}
	if err := n.st.SaveSnapshot(snap); err != nil {
		return fmt.Errorf("save snapshot: %w", err)
	}
	n.snapIndex = n.applied
	if n.applied <= n.cfg.SnapshotTrailing {
		return nil
	}
	first, err := n.st.FirstIndex()
	if err != nil {
		return fmt.Errorf("first index: %w", err)
	}
	if to := n.applied - n.cfg.SnapshotTrailing; to >= first {
		if err := n.st.Compact(to); err != nil {
			return fmt.Errorf("compact to %d: %w", to, err)
		}
	}
	return nil
}
