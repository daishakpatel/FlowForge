package fsm

import "crypto/sha256"

// StateHash is the SHA-256 of the canonical encoding (the same bytes Snapshot returns, without
// its size limit). Replicas at the same applied index have equal hashes; determinism tests and
// 04.06's ClusterStatus.state_hash compare them.
func (f *FSM) StateHash() [32]byte {
	f.mu.RLock()
	defer f.mu.RUnlock()
	b, err := f.st.canonical()
	if err != nil {
		// Encoding a well-formed in-memory message cannot fail; hash the error so a broken
		// replica differs from every healthy one instead of panicking.
		return sha256.Sum256([]byte(err.Error()))
	}
	return sha256.Sum256(b)
}
