// Package scheduler is flowforge-scheduler's core (task 03.02): leader-gated loops, and the
// scheduler loop that moves tasks PENDING → READY/SKIPPED → QUEUED and drives run status
// (00E §3, ADR-003, ADR-004, ADR-020). The scheduler never talks to workers; it only changes
// rows, and workers claim QUEUED tasks through flowforge-api.
package scheduler

import "context"

// Gate tells loops whether this instance may make scheduling decisions (ADR-003).
//
// Contract: a loop makes no decision after Done() is closed, and every decision transaction also
// asserts Epoch() against cluster_state in the same transaction, so a deposed leader that has
// not noticed yet still cannot commit (belt and braces). Implementations are safe for
// concurrent use.
type Gate interface {
	// IsLeader reports whether this instance holds leadership now.
	IsLeader() bool
	// Epoch is the fencing epoch of the current leadership term.
	Epoch() int64
	// Done returns a channel closed when the current leadership term ends. Called while not
	// leader it returns an already-closed channel. Each new term has a new channel.
	Done() <-chan struct{}
	// Wait blocks until this instance is leader, or returns ctx.Err() when ctx ends first.
	Wait(ctx context.Context) error
}

// AlwaysLeader is the Gate of single-scheduler mode: always leader at epoch 0, the unfenced
// value of cluster_state (00E §2). It needs no Raft and remains a supported dev mode; task 04.07
// adds the Raft-backed Gate.
type AlwaysLeader struct{}

var _ Gate = AlwaysLeader{}

// IsLeader implements Gate: always true.
func (AlwaysLeader) IsLeader() bool { return true }

// Epoch implements Gate: always 0.
func (AlwaysLeader) Epoch() int64 { return 0 }

// Done implements Gate: a nil channel, which never closes.
func (AlwaysLeader) Done() <-chan struct{} { return nil }

// Wait implements Gate: it returns at once unless ctx is already done.
func (AlwaysLeader) Wait(ctx context.Context) error { return ctx.Err() }
