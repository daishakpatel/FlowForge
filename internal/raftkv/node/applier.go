package node

// Applier is the replicated state machine the node drives (04.03's fsm.NewApplier). The node calls
// it from its one goroutine only, so an Applier needs no locking against the node (it does
// against its own readers).
//
// Contract:
//   - Apply receives each committed, non-empty normal entry exactly once per node start, in
//     strictly increasing index order and never beyond the commit index. The bytes are the
//     proposal's data as passed to Node.Propose. The returned result and error go to the
//     proposer as Result.Data and Result.Err; an error is the command's deterministic outcome
//     (every replica returns it), not a replication failure, and does not stop the node.
//   - Snapshot returns the state after the last applied entry. The node stores it as the
//     snapshot at that index and later sends it to lagging followers.
//   - Restore replaces the whole state with a snapshot's data, either when the node starts (the
//     latest stored snapshot) or when the leader sends one. After Restore, Apply continues from
//     the snapshot's index + 1. data is exactly what Snapshot returned, possibly empty, so Restore
//     must accept an empty snapshot if Snapshot can produce one. An Applier passed to New must
//     be empty (the bootstrap snapshot at index 1 is not restored).
type Applier interface {
	Apply(entry []byte, index uint64) (result []byte, err error)
	Snapshot() ([]byte, error)
	Restore(data []byte) error
}
