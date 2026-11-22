package fsm

import "crypto/sha256"

// StateHash is the SHA-256 of the canonical encoding (the same bytes Snapshot returns, without
// its size limit). Replicas at the same applied index have equal hashes; determinism tests and
// 04.06's ClusterStatus.state_hash compare them.
func (f *FSM) StateHash() [32]byte {
	h, _ := f.StateHashAt()
	return h
}

// StateHashAt returns StateHash together with the applied index it describes, read under one
// lock (04.06's Cluster.Status reports state_hash@applied_index).
func (f *FSM) StateHashAt() ([32]byte, uint64) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.hashLocked(), f.st.lastApplied
}

func (f *FSM) hashLocked() [32]byte {
	b, err := f.st.canonical()
	if err != nil {
		// Encoding a well-formed in-memory message cannot fail; hash the error so a broken
		// replica differs from every healthy one instead of panicking.
		return sha256.Sum256([]byte(err.Error()))
	}
	return sha256.Sum256(b)
}
