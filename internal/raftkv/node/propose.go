package node

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

// Result is a proposal's outcome once its entry is applied on this node.
type Result struct {
	Index uint64 // the entry's log index
	Data  []byte // the Applier's result
	Err   error  // the Applier's error: the command's deterministic outcome, not a replication failure
}

// Propose replicates data and waits until this node has applied it, so a read on this node
// that follows sees it (rule 3). Only the leader accepts proposals; see errors.go for what each
// error means and which ones leave the outcome unknown. data must be non-empty and at most
// Config.MaxSizePerMsg bytes (ErrProposalTooLarge).
func (n *Node) Propose(ctx context.Context, data []byte) (Result, error) {
	if len(data) == 0 {
		return Result{}, errors.New("propose: empty data")
	}
	if uint64(len(data)) > n.cfg.MaxSizePerMsg {
		return Result{}, fmt.Errorf("%w: %d bytes, limit %d", ErrProposalTooLarge, len(data), n.cfg.MaxSizePerMsg)
	}
	p := &proposal{ctx: ctx, id: n.nextID(), data: data, done: make(chan proposalResult, 1)}
	select {
	case n.propc <- p:
	case <-ctx.Done():
		return Result{}, fmt.Errorf("%w: %w", ErrProposalTimeout, ctx.Err())
	case <-n.donec:
		return Result{}, ErrNodeStopped
	}
	select {
	case r := <-p.done:
		return r.res, r.err
	case <-ctx.Done():
		return Result{}, fmt.Errorf("%w: %w", ErrProposalTimeout, ctx.Err())
	case <-n.donec:
		select {
		case r := <-p.done: // the loop answered before it stopped
			return r.res, r.err
		default:
			return Result{}, ErrNodeStopped
		}
	}
}

// proposal is a pending Propose, owned by the loop once sent on propc. index is set when the
// entry shows up in this node's log, so the loop can tell when it was overwritten.
type proposal struct {
	ctx   context.Context //nolint:containedctx // the caller's ctx, checked by the loop to drop abandoned waiters
	id    proposalID
	data  []byte
	index uint64
	done  chan proposalResult // buffered 1: the loop never blocks on it
}

type proposalResult struct {
	res Result
	err error
}

// propose runs on the loop: only a leader appends. A follower answers with the leader hint and
// never forwards (DisableProposalForwarding), so a NotLeader error is definite.
func (n *Node) propose(p *proposal) {
	if st := n.rn.BasicStatus(); st.RaftState != raft.StateLeader {
		p.done <- proposalResult{err: n.notLeader(st.Lead)}
		return
	}
	if err := n.rn.Propose(encodeEntry(p.id, p.data)); err != nil {
		if errors.Is(err, raft.ErrProposalDropped) {
			err = fmt.Errorf("%w: %w", ErrProposalDropped, err)
		}
		p.done <- proposalResult{err: err}
		return
	}
	n.waiters[p.id] = p
}

func (n *Node) notLeader(lead uint64) error {
	if lead == raft.None {
		return ErrNoQuorum
	}
	return &NotLeaderError{Leader: lead, Addr: n.addrs[lead]}
}

// trackEntries records where this node's own pending proposals landed in its log.
func (n *Node) trackEntries(ents []*pb.Entry) {
	for _, e := range ents {
		if e.GetType() != pb.EntryType_EntryNormal {
			continue
		}
		if id, _, ok := decodeEntry(e.GetData()); ok {
			if p := n.waiters[id]; p != nil {
				p.index = e.GetIndex()
			}
		}
	}
}

// complete answers the waiter of the entry just applied, if this node proposed it.
func (n *Node) complete(id proposalID, res Result) {
	if p := n.waiters[id]; p != nil {
		p.done <- proposalResult{res: res}
		delete(n.waiters, id)
	}
}

// failOverwritten fails waiters whose entry's index has been applied with a different entry:
// a new leader overwrote it, and since an entry never moves to another index it will never be
// applied (a definite failure).
func (n *Node) failOverwritten() {
	for id, p := range n.waiters {
		if p.index != 0 && p.index <= n.applied {
			p.done <- proposalResult{err: fmt.Errorf("%w: entry %d was replaced by a new leader", ErrProposalDropped, p.index)}
			delete(n.waiters, id)
		}
	}
}

// pruneAbandoned forgets waiters and reads whose caller gave up, bounding both maps.
func (n *Node) pruneAbandoned() {
	for id, p := range n.waiters {
		if p.ctx.Err() != nil {
			delete(n.waiters, id)
		}
	}
	for k, r := range n.reads {
		if r.ctx.Err() != nil {
			delete(n.reads, k)
		}
	}
	n.readWait = pruneReads(n.readWait)
}

// proposalID makes every entry this node proposes unique across its restarts: boot is a random
// nonce per start, seq counts within it. node keeps ids from different nodes apart even if two
// nonces collide.
type proposalID struct{ node, boot, seq uint64 }

const (
	entryVersion   = 1
	entryHeaderLen = 1 + 3*8
)

func (n *Node) nextID() proposalID {
	return proposalID{node: n.cfg.ID, boot: n.boot, seq: n.seq.Add(1)}
}

// encodeEntry frames a proposal: version byte, node, boot, seq (big endian), then the data.
func encodeEntry(id proposalID, data []byte) []byte {
	b := make([]byte, entryHeaderLen, entryHeaderLen+len(data))
	b[0] = entryVersion
	binary.BigEndian.PutUint64(b[1:], id.node)
	binary.BigEndian.PutUint64(b[9:], id.boot)
	binary.BigEndian.PutUint64(b[17:], id.seq)
	return append(b, data...)
}

func decodeEntry(b []byte) (proposalID, []byte, bool) {
	if len(b) < entryHeaderLen || b[0] != entryVersion {
		return proposalID{}, nil, false
	}
	id := proposalID{
		node: binary.BigEndian.Uint64(b[1:]),
		boot: binary.BigEndian.Uint64(b[9:]),
		seq:  binary.BigEndian.Uint64(b[17:]),
	}
	return id, b[entryHeaderLen:], true
}
