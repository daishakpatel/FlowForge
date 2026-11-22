// Package node runs one Raft replica on the etcd raft library (task 04.02, FR-RAFT-01,
// FR-RAFT-11, ADR-002): the consensus algorithm is the library's; this package drives it.
//
// One goroutine owns a raft.RawNode and runs the Ready loop in the order ADR-002 requires:
// persist (storage.Save) → send (Transport) → apply (Applier) → Advance. Proposals, reads,
// inbound messages, transport reports and ticks reach that goroutine through bounded channels,
// so no raft state is shared. See docs/architecture/raft-node.md.
package node

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"runtime/debug"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/raftkv/storage"
)

// Transport sends the Ready loop's outbound messages. Send must not block (queue and return):
// it runs on the loop. Delivery is best effort; raft retransmits. An implementation reports
// back through Node.ReportUnreachable and, for every MsgSnap, Node.ReportSnapshot.
type Transport interface {
	Send(msgs []*pb.Message)
}

// recvQueue bounds inbound messages waiting for the loop; beyond it Step drops (raft tolerates
// loss and retransmits).
const recvQueue = 4096

// Status is a point-in-time view of the node, updated after every loop iteration.
type Status struct {
	ID         uint64
	Leader     uint64 // 0 when no leader is known
	LeaderAddr string // the leader's peer address from Config.Peers
	IsLeader   bool
	Term       uint64
	Commit     uint64
	Applied    uint64
}

// Node is one Raft replica. Create it with New, start it with Start, and stop it with Stop; a
// Node is not restartable (create a new one over the same storage). All methods are safe for
// concurrent use.
type Node struct {
	cfg   Config
	st    storage.Storage
	app   Applier
	addrs map[uint64]string
	boot  uint64
	seq   atomic.Uint64

	propc   chan *proposal
	readc   chan *readReq
	recvc   chan *pb.Message
	reportc chan struct{} // signals pending reports (capacity 1)
	confc   chan *pb.ConfChange
	xferc   chan transferReq
	stopc   chan struct{}
	donec   chan struct{}
	started atomic.Bool
	stop    sync.Once

	// Owned by the loop goroutine.
	rn        *raft.RawNode
	tr        Transport
	ticker    clock.Ticker
	applied   uint64
	snapIndex uint64
	snapTried uint64 // applied index of the last snapshot attempt
	confState *pb.ConfState
	waiters   map[proposalID]*proposal
	reads     map[string]*readReq
	readWait  []*readReq

	// Transport reports, coalesced per peer so they never queue up or get dropped: raft keeps a
	// follower paused until it gets its snapshot report.
	reportMu    sync.Mutex
	unreachable map[uint64]struct{}
	snapReports map[uint64]raft.SnapshotStatus

	mu     sync.Mutex // guards status and err; never held together with reportMu
	status Status
	err    error
}

// New opens a node over st, which it does not close. On first start (empty storage) it
// bootstraps the static membership: a snapshot at index 1 whose ConfState holds every configured
// voter, identical on every node. On later starts it refuses (ErrPeersMismatch) a peer list that
// differs from the persisted ConfState, restores app from the latest snapshot, and resumes from
// the snapshot's index: committed entries after it are applied again (rule 5).
func New(cfg Config, st storage.Storage, app Applier) (*Node, error) {
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	var nonce [8]byte
	if _, err := io.ReadFull(cfg.Entropy, nonce[:]); err != nil {
		return nil, fmt.Errorf("raft node: read entropy: %w", err)
	}
	n := &Node{
		cfg: cfg, st: st, app: app, addrs: map[uint64]string{},
		boot:    binary.BigEndian.Uint64(nonce[:]),
		propc:   make(chan *proposal, 256),
		readc:   make(chan *readReq, 256),
		recvc:   make(chan *pb.Message, recvQueue),
		reportc: make(chan struct{}, 1),
		confc:   make(chan *pb.ConfChange, 1),
		xferc:   make(chan transferReq, 1),
		stopc:   make(chan struct{}),
		donec:   make(chan struct{}),
		waiters: map[proposalID]*proposal{},
		reads:   map[string]*readReq{},

		unreachable: map[uint64]struct{}{},
		snapReports: map[uint64]raft.SnapshotStatus{},
	}
	for _, p := range cfg.Peers {
		n.addrs[p.ID] = p.Addr
	}
	if err := n.load(); err != nil {
		return nil, err
	}
	rn, err := raft.NewRawNode(&raft.Config{
		ID: cfg.ID, ElectionTick: cfg.ElectionTick, HeartbeatTick: cfg.HeartbeatTick,
		Storage: st, Applied: n.applied,
		MaxSizePerMsg: cfg.MaxSizePerMsg, MaxInflightMsgs: cfg.MaxInflightMsgs,
		CheckQuorum: true, PreVote: true, DisableProposalForwarding: true,
		Logger: raftLogger{cfg.Logger},
	})
	if err != nil {
		return nil, fmt.Errorf("raft node: %w", err)
	}
	n.rn = rn
	n.updateStatus()
	return n, nil
}

// load bootstraps empty storage or validates and restores existing storage.
func (n *Node) load() error {
	hs, cs, err := n.st.InitialState()
	if err != nil {
		return fmt.Errorf("raft node: read storage: %w", err)
	}
	last, err := n.st.LastIndex()
	if err != nil {
		return fmt.Errorf("raft node: read storage: %w", err)
	}
	if raft.IsEmptyHardState(hs) && last == 0 {
		cs = &pb.ConfState{Voters: n.cfg.voterIDs()}
		boot := &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: proto.Uint64(1), Term: proto.Uint64(1), ConfState: cs}}
		if err := n.st.Save(&pb.HardState{Term: proto.Uint64(1), Commit: proto.Uint64(1)}, nil, boot); err != nil {
			return fmt.Errorf("raft node: bootstrap: %w", err)
		}
	} else if voters := slices.Sorted(slices.Values(cs.GetVoters())); !slices.Equal(voters, n.cfg.voterIDs()) || len(cs.GetLearners()) > 0 {
		return fmt.Errorf("%w: configured %v, persisted voters %v learners %v",
			ErrPeersMismatch, n.cfg.voterIDs(), voters, cs.GetLearners())
	}
	snap, err := n.st.LoadSnapshot()
	if err != nil {
		return fmt.Errorf("raft node: load snapshot: %w", err)
	}
	// Restore every snapshot as installSnapshot does, empty data included, except the bootstrap
	// snapshot this node wrote itself: it carries no Applier state.
	if idx := snap.GetMetadata().GetIndex(); idx > 1 || len(snap.GetData()) > 0 {
		if err := n.app.Restore(snap.GetData()); err != nil {
			return fmt.Errorf("raft node: restore snapshot %d: %w", snap.GetMetadata().GetIndex(), err)
		}
	}
	n.applied = snap.GetMetadata().GetIndex()
	n.snapIndex = n.applied
	n.confState = cs
	return nil
}

// Start starts the loop with tr as the outbound transport. Inbound messages arrive through Step.
func (n *Node) Start(tr Transport) error {
	if !n.started.CompareAndSwap(false, true) {
		return errors.New("raft node: already started or stopped")
	}
	n.tr = tr
	n.ticker = n.cfg.Ticker
	if n.ticker == nil {
		n.ticker = n.cfg.Clock.NewTicker(n.cfg.TickInterval)
	}
	go n.run()
	return nil
}

// Stop stops the loop and waits for it. Pending proposals and reads fail with ErrNodeStopped.
// It is idempotent; the caller closes the storage afterwards.
func (n *Node) Stop() {
	n.stop.Do(func() {
		close(n.stopc)
		if n.started.CompareAndSwap(false, true) { // never started: nothing to wait for, and Start now fails
			close(n.donec)
		}
	})
	<-n.donec
}

// Done is closed once the loop has stopped, by Stop or by a fatal error (Err).
func (n *Node) Done() <-chan struct{} { return n.donec }

// Err returns the error that stopped the loop on its own (a storage or Applier.Restore failure,
// or a panic in the raft library), or nil.
func (n *Node) Err() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.err
}

// Status returns the node's latest status.
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.status
}

// Step hands an inbound message from the transport to the loop without blocking. It returns an
// error when the message is dropped: the inbound queue is full or the node stopped.
func (n *Node) Step(m *pb.Message) error {
	select {
	case n.recvc <- m:
		if _, ok := n.addrs[m.GetFrom()]; ok { // From is unauthenticated: no series for unknown ids
			n.cfg.Metrics.peerReachable(m.GetFrom(), true)
		}
		return nil
	case <-n.donec:
		return ErrNodeStopped
	default:
		return errors.New("raft node: inbound queue full, message dropped")
	}
}

// ReportUnreachable tells raft that a message to peer id could not be sent, so the leader stops
// pipelining to it and probes instead. It never blocks; repeated reports for one peer coalesce
// until the loop takes them. Reports for ids that aren't configured peers are ignored.
func (n *Node) ReportUnreachable(id uint64) {
	if _, ok := n.addrs[id]; !ok {
		return
	}
	n.cfg.Metrics.peerReachable(id, false)
	n.reportMu.Lock()
	n.unreachable[id] = struct{}{}
	n.reportMu.Unlock()
	n.signalReports()
}

// ReportSnapshot reports whether a MsgSnap to peer id was delivered; raft resumes replication
// to that peer only after it. It never blocks and is never dropped: the latest status per peer
// is kept until the loop takes it.
func (n *Node) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	if _, ok := n.addrs[id]; !ok {
		return
	}
	n.reportMu.Lock()
	n.snapReports[id] = status
	n.reportMu.Unlock()
	n.signalReports()
}

func (n *Node) signalReports() {
	select {
	case n.reportc <- struct{}{}:
	default: // a signal is already pending; the loop takes every report at once
	}
}

// flushReports hands the pending reports to raft, on the loop.
func (n *Node) flushReports() {
	n.reportMu.Lock()
	unreachable, snaps := n.unreachable, n.snapReports
	n.unreachable, n.snapReports = map[uint64]struct{}{}, map[uint64]raft.SnapshotStatus{}
	n.reportMu.Unlock()
	for id := range unreachable {
		n.rn.ReportUnreachable(id)
	}
	for id, st := range snaps {
		n.rn.ReportSnapshot(id, st)
	}
}

// proposeConfChange is the dynamic-membership path (FR-RAFT-11, optional): wired through the loop
// and ApplyConfChange so the code path exists, but not exported until membership changes are
// designed (restart validation compares the snapshot's ConfState only).
func (n *Node) proposeConfChange(cc *pb.ConfChange) error {
	select {
	case n.confc <- cc:
		return nil
	case <-n.donec:
		return ErrNodeStopped
	}
}

// run is the loop: one input, then every Ready it produced. A tick first drains the inbound
// queue, so a message that arrived before a tick is always stepped before it; with a
// ManualTicker this bounds message latency to one tick for deterministic tests.
func (n *Node) run() {
	defer close(n.donec)
	defer n.ticker.Stop() // releases a ManualTicker.Tick blocked on this node
	defer n.failAll()
	defer func() {
		if r := recover(); r != nil {
			n.cfg.Logger.Error("raft node: loop panicked", "panic", r, "stack", string(debug.Stack()))
			n.setErr(fmt.Errorf("raft node: panic: %v", r))
		}
	}()
	for {
		ticked := false
		select {
		case <-n.stopc:
			return
		case <-n.ticker.C():
			n.drainRecv()
			n.rn.Tick()
			n.pruneAbandoned()
			n.reissueReads()
			ticked = true
		case m := <-n.recvc:
			n.step(m)
		case p := <-n.propc:
			n.propose(p)
		case r := <-n.readc:
			n.readIndex(r)
		case <-n.reportc:
			n.flushReports()
		case x := <-n.xferc:
			x.done <- n.transfer(x.to)
		case cc := <-n.confc:
			if err := n.rn.ProposeConfChange(cc); err != nil {
				n.cfg.Logger.Warn("raft node: conf change dropped", "err", err)
			}
		}
		if err := n.processReady(); err != nil {
			n.cfg.Logger.Error("raft node: stopping", "err", err)
			n.setErr(err)
			return
		}
		n.updateStatus()
		if mt, ok := n.ticker.(*ManualTicker); ok && ticked {
			mt.ack()
		}
	}
}

func (n *Node) drainRecv() {
	for {
		select {
		case m := <-n.recvc:
			n.step(m)
		default:
			return
		}
	}
}

func (n *Node) step(m *pb.Message) {
	// Local message types never come from the network; stepping one would let a peer drive this
	// node's internals.
	if raft.IsLocalMsg(m.GetType()) {
		return
	}
	if err := n.rn.Step(m); err != nil && !errors.Is(err, raft.ErrStepPeerNotFound) {
		n.cfg.Logger.Debug("raft node: step", "err", err)
	}
}

func (n *Node) failAll() {
	for id, p := range n.waiters {
		p.done <- proposalResult{err: ErrNodeStopped}
		delete(n.waiters, id)
	}
	for k, r := range n.reads {
		r.done <- readResult{err: ErrNodeStopped}
		delete(n.reads, k)
	}
	for _, r := range n.readWait {
		r.done <- readResult{err: ErrNodeStopped}
	}
	n.readWait = nil
}

func (n *Node) setErr(err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.err == nil {
		n.err = err
	}
}

func (n *Node) updateStatus() {
	bs := n.rn.BasicStatus()
	s := Status{
		ID: n.cfg.ID, Leader: bs.Lead, LeaderAddr: n.addrs[bs.Lead], IsLeader: bs.RaftState == raft.StateLeader,
		Term: bs.GetTerm(), Commit: bs.GetCommit(), Applied: n.applied,
	}
	n.mu.Lock()
	n.status = s
	n.mu.Unlock()
	n.cfg.Metrics.observe(s, len(n.waiters))
}

// ManualTicker is a clock.Ticker that tests drive by hand. Tick delivers one tick and returns
// after the node has stepped every message queued before it, ticked raft, and handled the
// resulting Ready, which makes elections reproducible regardless of machine speed.
type ManualTicker struct {
	c    chan time.Time
	acks chan struct{}
	stop chan struct{}
	once sync.Once
}

// NewManualTicker returns a ManualTicker for Config.Ticker.
func NewManualTicker() *ManualTicker {
	return &ManualTicker{c: make(chan time.Time), acks: make(chan struct{}), stop: make(chan struct{})}
}

// Tick delivers one tick and waits until the node processed it. It returns false once the ticker
// is stopped (the node stopped).
func (t *ManualTicker) Tick() bool {
	select {
	case t.c <- time.Time{}:
	case <-t.stop:
		return false
	}
	select {
	case <-t.acks:
		return true
	case <-t.stop:
		return false
	}
}

// C implements clock.Ticker.
func (t *ManualTicker) C() <-chan time.Time { return t.c }

// Stop implements clock.Ticker; Node.Stop calls it, which releases a blocked Tick.
func (t *ManualTicker) Stop() { t.once.Do(func() { close(t.stop) }) }

// Reset implements clock.Ticker; a manual ticker has no period.
func (t *ManualTicker) Reset(time.Duration) {}

func (t *ManualTicker) ack() {
	select {
	case t.acks <- struct{}{}:
	case <-t.stop:
	}
}
