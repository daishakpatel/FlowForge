package coordination

// Fence is the epoch a leader's decisions carry (ADR-003). A scheduler transaction commits only
// while its fence is the epoch stored in Postgres's cluster_state (Current), which a newer
// leader's EpochFence activation raises; a deposed leader's decisions then fail.
type Fence struct{ Epoch int64 }

// leadership is the part of a scheduler.Gate a fence is read from.
type leadership interface {
	IsLeader() bool
	Epoch() int64
}

// FenceFromLeader returns the fence of g's current term, or false while g does not lead: without
// proven leadership, do nothing (00C §10.1).
func FenceFromLeader(g leadership) (Fence, bool) {
	if t, ok := g.(interface{ Term() (int64, bool) }); ok { // one consistent read (Gate)
		epoch, leading := t.Term()
		return Fence{Epoch: epoch}, leading
	}
	if !g.IsLeader() {
		return Fence{}, false
	}
	return Fence{Epoch: g.Epoch()}, true
}

// Current reports whether f may commit against the epoch stored in Postgres: only an equal
// epoch. A lower stored epoch means activation never happened; a higher one, a newer leader.
func (f Fence) Current(stored int64) bool { return f.Epoch == stored }

// Supersedes reports whether f belongs to a later term than other.
func (f Fence) Supersedes(other Fence) bool { return f.Epoch > other.Epoch }
