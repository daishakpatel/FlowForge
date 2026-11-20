// Package nodetest is an in-process Raft cluster for tests (task 04.02, ADR-019): real
// node.Nodes over an in-memory network behind a transport.Fault, driven by manual ticks. A Tick
// round ticks the Fault and then every running node, each tick returning only after that node
// stepped every message queued before it, so elections don't depend on machine speed.
//
// Typical use:
//
//	c := nodetest.New(t, nodetest.Options{Seed: seed})
//	leader := c.WaitLeader(100)
//	res, err := c.Node(leader).Propose(ctx, []byte("x"))
//	c.Fault.Partition([]uint64{leader}, c.Others(leader))
package nodetest

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"sync"
	"testing"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/daishakpatel/flowforge/internal/raftkv/node"
	"github.com/daishakpatel/flowforge/internal/raftkv/storage"
	"github.com/daishakpatel/flowforge/internal/raftkv/transport"
)

// Options configures a Cluster. Zero values take the defaults noted.
type Options struct {
	Size int   // voters, default 3
	Seed int64 // seeds the Fault and the nodes' proposal nonces
	// SnapshotEntries and SnapshotTrailing default to node's (10 000 and 1 000).
	SnapshotEntries, SnapshotTrailing uint64
	// Storage opens a node's storage. It is called on every start; Stop closes the previous one
	// first. Default: one storage.Mem per node, kept across restarts (Mem.Close is a no-op).
	Storage func(id uint64) (storage.Storage, error)
	// OnSend, if set, sees every message a node sends, on that node's loop, before faults apply.
	OnSend func(m *pb.Message)
	Logger *slog.Logger // default: discard
}

// Cluster is a running in-process cluster. Its methods are for the test goroutine; the nodes
// run concurrently.
type Cluster struct {
	t     testing.TB
	opts  Options
	peers []node.Peer
	// Fault sits between every pair of nodes. Configure partitions and link faults on it.
	Fault *transport.Fault

	mu      sync.RWMutex // guards members' fields, read by the network on node loops
	members map[uint64]*member
	nonces  map[uint64]bool // every proposal nonce handed out (test goroutine only)
}

type member struct {
	node   *node.Node // nil while stopped
	ticker *node.ManualTicker
	app    *Recorder
	st     storage.Storage
	starts uint64
}

// New starts a cluster and stops it in t.Cleanup.
func New(t testing.TB, opts Options) *Cluster {
	t.Helper()
	if opts.Size == 0 {
		opts.Size = 3
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if opts.Storage == nil {
		mems := map[uint64]storage.Storage{}
		opts.Storage = func(id uint64) (storage.Storage, error) {
			if mems[id] == nil {
				mems[id] = storage.NewMem()
			}
			return mems[id], nil
		}
	}
	c := &Cluster{t: t, opts: opts, members: map[uint64]*member{}, nonces: map[uint64]bool{}}
	for i := range opts.Size {
		id := uint64(i) + 1 //nolint:gosec // i ≥ 0
		c.peers = append(c.peers, node.Peer{ID: id, Addr: fmt.Sprintf("n%d:7001", id)})
		c.members[id] = &member{app: &Recorder{}}
	}
	c.Fault = transport.NewFault(network{c}, opts.Seed, c.dropped)
	t.Cleanup(c.StopAll)
	for _, id := range c.IDs() {
		c.Start(id)
	}
	return c
}

// IDs returns every member's id, running or not.
func (c *Cluster) IDs() []uint64 {
	ids := make([]uint64, 0, len(c.members))
	for id := range c.members {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

// Others returns every id but the given ones.
func (c *Cluster) Others(not ...uint64) []uint64 {
	return slices.DeleteFunc(c.IDs(), func(id uint64) bool { return slices.Contains(not, id) })
}

// Start starts node id (again) from its storage, with a fresh Recorder that the node restores
// from its latest snapshot. It fails the test if the node can't start.
func (c *Cluster) Start(id uint64) {
	c.t.Helper()
	if c.Node(id) != nil {
		c.t.Fatalf("nodetest: node %d is already running", id)
	}
	st, err := c.opts.Storage(id)
	if err != nil {
		c.t.Fatalf("nodetest: open storage %d: %v", id, err)
	}
	m := c.members[id]
	m.starts++
	// node.New reads 8 bytes: the nonce must differ per node and per start, or a restarted node
	// would reuse proposal ids and complete a new waiter with an old entry.
	boot := uint64(c.opts.Seed)<<40 ^ id<<24 ^ m.starts //nolint:gosec // bit pattern only
	if c.nonces[boot] {
		c.t.Fatalf("nodetest: proposal nonce %#x reused", boot)
	}
	c.nonces[boot] = true
	var nonce [8]byte
	binary.BigEndian.PutUint64(nonce[:], boot)
	tk, app := node.NewManualTicker(), &Recorder{}
	n, err := node.New(node.Config{
		ID: id, Peers: c.peers, Ticker: tk, Entropy: bytes.NewReader(nonce[:]), Logger: c.opts.Logger.With("node", id),
		SnapshotEntries: c.opts.SnapshotEntries, SnapshotTrailing: c.opts.SnapshotTrailing,
	}, st, app)
	if err != nil {
		c.t.Fatalf("nodetest: start node %d: %v", id, err)
	}
	c.mu.Lock()
	m.node, m.ticker, m.app, m.st = n, tk, app, st
	c.mu.Unlock()
	if err := n.Start(sender{c: c}); err != nil {
		c.t.Fatalf("nodetest: start node %d: %v", id, err)
	}
}

// Stop stops node id like a crash (nothing beyond what each Ready already persisted survives)
// and closes its storage. Its Recorder stays readable.
func (c *Cluster) Stop(id uint64) {
	c.mu.Lock()
	m := c.members[id]
	n := m.node
	m.node = nil // the network stops delivering to it before it stops
	c.mu.Unlock()
	if n == nil {
		return
	}
	n.Stop()
	if err := m.st.Close(); err != nil {
		c.t.Errorf("nodetest: close storage %d: %v", id, err)
	}
	if err := n.Err(); err != nil {
		c.t.Errorf("nodetest: node %d had stopped on its own: %v", id, err)
	}
}

// Restart stops and starts node id.
func (c *Cluster) Restart(id uint64) {
	c.t.Helper()
	c.Stop(id)
	c.Start(id)
}

// StopAll stops every running node.
func (c *Cluster) StopAll() {
	for _, id := range c.IDs() {
		c.Stop(id)
	}
}

// Node returns node id's running node, or nil.
func (c *Cluster) Node(id uint64) *node.Node {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if m := c.members[id]; m != nil {
		return m.node
	}
	return nil
}

// Applier returns node id's current Recorder (its latest start's).
func (c *Cluster) Applier(id uint64) *Recorder {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.members[id].app
}

// Tick runs one round: the Fault's clock, then one tick of every running node, in id order.
func (c *Cluster) Tick() {
	c.Fault.Tick()
	for _, id := range c.IDs() {
		c.mu.RLock()
		m := c.members[id]
		running, tk := m.node != nil, m.ticker
		c.mu.RUnlock()
		if running {
			tk.Tick()
		}
	}
}

// TickUntil ticks until cond holds, at most maxRounds rounds, and reports whether it held.
func (c *Cluster) TickUntil(cond func() bool, maxRounds int) bool {
	for range maxRounds {
		if cond() {
			return true
		}
		c.Tick()
	}
	return cond()
}

// Leader returns the leader every running node agrees on, in the same term, or 0.
func (c *Cluster) Leader() uint64 {
	var lead, term uint64
	for _, s := range c.Statuses() {
		if s.Leader == 0 || (lead != 0 && (s.Leader != lead || s.Term != term)) {
			return 0
		}
		lead, term = s.Leader, s.Term
	}
	if n := c.Node(lead); n == nil || !n.Status().IsLeader {
		return 0
	}
	return lead
}

// WaitLeader ticks until the running nodes agree on a leader (at most maxRounds) and returns it,
// failing the test otherwise.
func (c *Cluster) WaitLeader(maxRounds int) uint64 {
	c.t.Helper()
	var lead uint64
	if !c.TickUntil(func() bool { lead = c.Leader(); return lead != 0 }, maxRounds) {
		c.t.Fatalf("nodetest: no agreed leader after %d rounds: %+v", maxRounds, c.Statuses())
	}
	return lead
}

// Statuses returns the running nodes' statuses in id order.
func (c *Cluster) Statuses() []node.Status {
	var ss []node.Status
	for _, id := range c.IDs() {
		if n := c.Node(id); n != nil {
			ss = append(ss, n.Status())
		}
	}
	return ss
}

// CheckPrefixes returns an error unless every member's applied sequence (running or not) is a
// prefix of the longest one and no Recorder saw an out-of-order entry.
func (c *Cluster) CheckPrefixes() error {
	var longest []Entry
	seqs := map[uint64][]Entry{}
	for _, id := range c.IDs() {
		r := c.Applier(id)
		if v := r.Violations(); len(v) > 0 {
			return fmt.Errorf("node %d applied out of order: %v", id, v)
		}
		seqs[id] = r.Entries()
		if len(seqs[id]) > len(longest) {
			longest = seqs[id]
		}
	}
	for _, id := range c.IDs() {
		if s := seqs[id]; !slices.Equal(s, longest[:len(s)]) {
			return fmt.Errorf("node %d's applied sequence diverges from the longest (%d vs %d entries)", id, len(s), len(longest))
		}
	}
	return nil
}

// Converged reports whether every running node applied the same sequence.
func (c *Cluster) Converged() bool {
	var first []Entry
	seen := false
	for _, id := range c.IDs() {
		if c.Node(id) == nil {
			continue
		}
		s := c.Applier(id).Entries()
		if seen && !slices.Equal(s, first) {
			return false
		}
		first, seen = s, true
	}
	return true
}

// dropped reports a MsgSnap the Fault dropped as failed to its sender.
func (c *Cluster) dropped(m *pb.Message) {
	if m.GetType() != pb.MsgSnap {
		return
	}
	if n := c.Node(m.GetFrom()); n != nil {
		n.ReportSnapshot(m.GetTo(), raft.SnapshotFailure)
	}
}

// sender is every node's Transport: the OnSend hook, then the shared Fault.
type sender struct{ c *Cluster }

func (s sender) Send(msgs []*pb.Message) {
	if s.c.opts.OnSend != nil {
		for _, m := range msgs {
			s.c.opts.OnSend(m)
		}
	}
	s.c.Fault.Send(msgs)
}

// network delivers what the Fault lets through, as a copy (like a wire), and reports back to
// the sender as a transport would.
type network struct{ c *Cluster }

func (nw network) Send(msgs []*pb.Message) {
	for _, m := range msgs {
		err := node.ErrNodeStopped
		if to := nw.c.Node(m.GetTo()); to != nil {
			err = to.Step(proto.Clone(m).(*pb.Message)) //nolint:forcetypeassert // Clone keeps the type
		}
		from := nw.c.Node(m.GetFrom())
		if from == nil {
			continue
		}
		if err != nil {
			from.ReportUnreachable(m.GetTo())
		}
		if m.GetType() == pb.MsgSnap {
			st := raft.SnapshotFinish
			if err != nil {
				st = raft.SnapshotFailure
			}
			from.ReportSnapshot(m.GetTo(), st)
		}
	}
}

// Entry is one applied entry as a Recorder saw it.
type Entry struct {
	Index uint64
	Data  string
}

// Recorder is a node.Applier that records every applied entry. Its snapshot is the whole list,
// so a node restored from a snapshot carries the full history. It flags any entry whose index
// does not increase (a re-application or reordering), and returns "ok:<data>", or an error for
// data starting with "fail".
type Recorder struct {
	mu         sync.Mutex
	entries    []Entry
	violations []string
	calls      int
}

// Apply implements node.Applier.
func (r *Recorder) Apply(data []byte, index uint64) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if n := len(r.entries); n > 0 && index <= r.entries[n-1].Index {
		r.violations = append(r.violations, fmt.Sprintf("index %d after %d", index, r.entries[n-1].Index))
	}
	r.entries = append(r.entries, Entry{Index: index, Data: string(data)})
	if bytes.HasPrefix(data, []byte("fail")) {
		return nil, fmt.Errorf("command %q refused", data)
	}
	return append([]byte("ok:"), data...), nil
}

// Snapshot implements node.Applier.
func (r *Recorder) Snapshot() ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	b, err := json.Marshal(r.entries)
	if err != nil {
		return nil, fmt.Errorf("snapshot recorder: %w", err)
	}
	return b, nil
}

// Restore implements node.Applier.
func (r *Recorder) Restore(data []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var es []Entry
	if err := json.Unmarshal(data, &es); err != nil {
		return fmt.Errorf("restore recorder: %w", err)
	}
	r.entries = es
	return nil
}

// Entries returns a copy of the applied entries.
func (r *Recorder) Entries() []Entry {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.entries)
}

// Violations returns the ordering violations seen.
func (r *Recorder) Violations() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.violations)
}

// ApplyCalls counts Apply calls on this Recorder (one node start).
func (r *Recorder) ApplyCalls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}
