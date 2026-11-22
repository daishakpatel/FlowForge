package node

import (
	"context"
	"fmt"

	"go.etcd.io/raft/v3"
)

// Snapshot policy (task 04.06, 00E §5.3): every Config.SnapshotEntries applied entries the loop
// snapshots the Applier and compacts the log to Config.SnapshotTrailing entries before the
// snapshot (maybeSnapshot, ready.go), so the log stays bounded while slightly lagging followers
// still catch up from the log. A follower further behind, or wiped, gets the snapshot through the
// transport. SnapshotNow and CompactNow let an operator do both on demand.

// SnapshotNow snapshots the Applier at the applied index now, compacting the log to
// SnapshotTrailing entries before it as the automatic trigger does, and returns the snapshot's
// index (unchanged when nothing was applied since the last one).
func (n *Node) SnapshotNow(ctx context.Context) (uint64, error) {
	return n.exec(ctx, func() (uint64, error) {
		if n.applied > n.snapIndex {
			if err := n.snapshot(); err != nil {
				return 0, err
			}
		}
		return n.snapIndex, nil
	})
}

// CompactNow drops the whole log up to and including the latest snapshot, keeping no trailing
// entries, and returns the new first index. Followers behind it then need the snapshot.
func (n *Node) CompactNow(ctx context.Context) (uint64, error) {
	return n.exec(ctx, func() (uint64, error) {
		if err := n.compact(n.snapIndex); err != nil {
			return 0, err
		}
		return n.firstIndex, nil
	})
}

type op struct {
	f    func() (uint64, error)
	done chan opResult // buffered 1
}

type opResult struct {
	v   uint64
	err error
}

// exec runs f on the loop and returns its result.
func (n *Node) exec(ctx context.Context, f func() (uint64, error)) (uint64, error) {
	o := &op{f: f, done: make(chan opResult, 1)}
	select {
	case n.opc <- o:
	case <-ctx.Done():
		return 0, fmt.Errorf("raft node: %w", ctx.Err())
	case <-n.donec:
		return 0, ErrNodeStopped
	}
	select {
	case r := <-o.done:
		return r.v, r.err
	case <-ctx.Done():
		return 0, fmt.Errorf("raft node: %w", ctx.Err())
	case <-n.donec:
		return 0, ErrNodeStopped
	}
}

// roleName is the Status.State of a raft role.
func roleName(s raft.StateType) string {
	switch s {
	case raft.StateLeader:
		return "LEADER"
	case raft.StateCandidate:
		return "CANDIDATE"
	case raft.StatePreCandidate:
		return "PRE_CANDIDATE"
	}
	return "FOLLOWER"
}
