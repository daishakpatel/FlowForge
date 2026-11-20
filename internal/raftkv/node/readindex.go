package node

import (
	"context"
	"encoding/binary"
	"fmt"

	"go.etcd.io/raft/v3"
)

// ReadIndex confirms with a quorum that this node's leader is still the leader, takes the commit
// index at that moment, and waits until this node has applied through it. State read from the
// Applier afterwards reflects every write that committed before ReadIndex was called (04.04's
// linearizable reads). It works on followers too (raft asks the leader). With no leader known
// it fails at once with ErrNoQuorum; a leader cut off from its quorum never answers, so the
// call ends with ErrReadIndexTimeout.
func (n *Node) ReadIndex(ctx context.Context) (uint64, error) {
	id := n.nextID()
	key := make([]byte, 24)
	binary.BigEndian.PutUint64(key, id.node)
	binary.BigEndian.PutUint64(key[8:], id.boot)
	binary.BigEndian.PutUint64(key[16:], id.seq)
	r := &readReq{ctx: ctx, key: string(key), done: make(chan readResult, 1)}
	select {
	case n.readc <- r:
	case <-ctx.Done():
		return 0, fmt.Errorf("%w: %w", ErrReadIndexTimeout, ctx.Err())
	case <-n.donec:
		return 0, ErrNodeStopped
	}
	select {
	case res := <-r.done:
		return res.index, res.err
	case <-ctx.Done():
		return 0, fmt.Errorf("%w: %w", ErrReadIndexTimeout, ctx.Err())
	case <-n.donec:
		return 0, ErrNodeStopped
	}
}

type readReq struct {
	ctx   context.Context //nolint:containedctx // the caller's ctx, checked by the loop to drop abandoned reads
	key   string
	index uint64
	ticks int             // ticks since it was last handed to raft
	done  chan readResult // buffered 1
}

type readResult struct {
	index uint64
	err   error
}

// readIndex runs on the loop.
func (n *Node) readIndex(r *readReq) {
	if lead := n.rn.BasicStatus().Lead; lead == raft.None {
		r.done <- readResult{err: ErrNoQuorum}
		return
	}
	n.reads[r.key] = r
	n.rn.ReadIndex([]byte(r.key))
}

// reissueReads hands reads still unconfirmed after an election timeout to raft again: raft never
// retransmits a lost MsgReadIndex or its response, and the leader ignores a key it already holds,
// so re-issuing is safe. Runs on every tick.
func (n *Node) reissueReads() {
	lead := n.rn.BasicStatus().Lead
	for _, r := range n.reads {
		if r.ticks++; r.ticks >= n.cfg.ElectionTick && lead != raft.None {
			r.ticks = 0
			n.rn.ReadIndex([]byte(r.key))
		}
	}
}

// readStates moves confirmed reads to the apply wait list.
func (n *Node) readStates(states []raft.ReadState) {
	for _, s := range states {
		if r := n.reads[string(s.RequestCtx)]; r != nil {
			delete(n.reads, r.key)
			r.index = s.Index
			n.readWait = append(n.readWait, r)
		}
	}
}

// resolveReads answers the confirmed reads this node has applied through.
func (n *Node) resolveReads() {
	keep := n.readWait[:0]
	for _, r := range n.readWait {
		if r.index <= n.applied {
			r.done <- readResult{index: r.index}
			continue
		}
		keep = append(keep, r)
	}
	n.readWait = keep
}

func pruneReads(rs []*readReq) []*readReq {
	keep := rs[:0]
	for _, r := range rs {
		if r.ctx.Err() == nil {
			keep = append(keep, r)
		}
	}
	return keep
}
