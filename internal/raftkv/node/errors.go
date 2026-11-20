package node

import (
	"errors"
	"fmt"
)

// Proposal and read outcomes. Two kinds:
//
//   - Definite: ErrNotLeader, ErrNoQuorum, ErrProposalDropped and ErrProposalTooLarge mean the command was not and
//     will never be applied. The caller may retry it, on the leader the hint names.
//   - Unknown: ErrProposalTimeout and ErrNodeStopped returned after a proposal was submitted mean
//     it may still be applied. Retrying is safe only for commands the state machine dedupes
//     (request ids, 04.03).
//
// No proposal both fails with a definite error and is applied. All of these are retriable
// (apperrors NOT_LEADER, NO_QUORUM, UNAVAILABLE); the API layer (04.04) maps them.
var (
	// ErrNotLeader is matched by *NotLeaderError: a follower got the proposal and knows the leader.
	ErrNotLeader = errors.New("not the raft leader")
	// ErrNoQuorum: no leader is known. An election is under way, or this node (CheckQuorum
	// stepped it down) can't reach a majority.
	ErrNoQuorum = errors.New("no raft quorum")
	// ErrProposalDropped: raft refused the proposal (a leadership transfer is under way), or the
	// entry was overwritten by a new leader's log before it committed.
	ErrProposalDropped = errors.New("raft proposal dropped")
	// ErrProposalTooLarge: the data exceeds Config.MaxSizePerMsg. Raft sends at least one entry
	// per message whatever its size, so a larger entry could exceed the peers' gRPC receive
	// limit and stall replication for good. Definite: nothing was appended.
	ErrProposalTooLarge = errors.New("raft proposal too large")
	// ErrProposalTimeout: the context ended before the outcome was known.
	ErrProposalTimeout = errors.New("raft proposal timed out")
	// ErrReadIndexTimeout: the context ended before a ReadIndex confirmed leadership and the
	// local state caught up.
	ErrReadIndexTimeout = errors.New("raft read index timed out")
	// ErrNodeStopped: the node was stopped, or stopped itself after a storage failure (Err).
	ErrNodeStopped = errors.New("raft node stopped")
	// ErrPeersMismatch: the configured voters differ from the membership persisted in storage.
	ErrPeersMismatch = errors.New("configured raft peers differ from the persisted membership")
	// ErrInvalidConfig wraps every configuration problem.
	ErrInvalidConfig = errors.New("invalid raft node config")
)

// NotLeaderError is returned by a follower: Leader is the leader's id and Addr its peer address
// from the configured peers (the client maps it to that node's client address).
type NotLeaderError struct {
	Leader uint64
	Addr   string
}

func (e *NotLeaderError) Error() string {
	return fmt.Sprintf("not the raft leader (leader is %d at %s)", e.Leader, e.Addr)
}

// Is makes errors.Is(err, ErrNotLeader) true.
func (e *NotLeaderError) Is(target error) bool { return target == ErrNotLeader }
