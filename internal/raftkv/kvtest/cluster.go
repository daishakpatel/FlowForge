// Package kvtest runs an in-process Raft KV cluster for tests (task 04.04): every member is the
// full stack flowforge-raft runs (storage.Mem, fsm.FSM, node.Node, the gRPC peer transport and
// the KV/Cluster server) behind one real loopback gRPC listener, ticking on the real clock. Unlike
// nodetest (manual ticks, a recording applier) it is for end-to-end tests of the server, the
// client and linearizability under leader kills.
//
// A member's address is its peer and client address. Kill stops a member abruptly (as a crashed
// process: in-flight RPCs fail); Start brings it back over the same storage, so it rejoins with
// its log.
//
// For fault injection (task 04.08) a cluster can also wrap every member's peer transport in a
// transport.Fault (Options.Faults: Partition, Heal, SetFaults), give members their own clocks
// (Options.MemberClock), wrap their storage (Options.WrapStorage), and copy and restore a stopped
// member's storage (Backup, Restore) as a copy of its data directory would.
package kvtest

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/daishakpatel/flowforge/internal/clock"
	pb "github.com/daishakpatel/flowforge/internal/gen/proto/coordination/v1"
	"github.com/daishakpatel/flowforge/internal/obs"
	"github.com/daishakpatel/flowforge/internal/raftkv/fsm"
	"github.com/daishakpatel/flowforge/internal/raftkv/node"
	"github.com/daishakpatel/flowforge/internal/raftkv/server"
	"github.com/daishakpatel/flowforge/internal/raftkv/storage"
	"github.com/daishakpatel/flowforge/internal/raftkv/transport"
	"github.com/daishakpatel/flowforge/internal/testutil"
)

// Options configures a Cluster. Zero values take the defaults noted.
type Options struct {
	Size int // voters, default 3
	// TickInterval is the raft tick, default 10ms: an election timeout of 100–200 ms.
	TickInterval time.Duration
	// Clock is the KV servers' clock (command timestamps, lease expiry, the sweeper); default
	// clock.Real. Raft always ticks on the real clock.
	Clock           clock.Clock
	ProposalTimeout time.Duration // default 1s
	SweepInterval   time.Duration // default the server's (500ms)
	SnapshotEntries uint64        // default the node's (10 000)
	// SnapshotTrailing is the log kept before a snapshot, default the node's (1 000) (04.06).
	SnapshotTrailing uint64
	// AdminToken enables the Admin service (04.06); empty disables it.
	AdminToken string
	Logger     *slog.Logger // default: discard

	// Faults wraps every member's peer transport in a transport.Fault seeded from FaultSeed,
	// driven by Partition, Heal and SetFaults; held messages are released every TickInterval.
	Faults    bool
	FaultSeed int64
	// MemberClock, when set and non-nil for a member, is that member's clock instead of Clock
	// (clock-skew tests: it stamps commands and judges lease expiry).
	MemberClock func(id uint64) clock.Clock
	// WrapStorage, when set, wraps a member's storage on every start (for example a slow disk).
	WrapStorage func(id uint64, st storage.Storage) storage.Storage
}

// ClusterID is the raft cluster id every kvtest member uses.
const ClusterID = 0xF10F

// Member is a running member's stack. It is replaced on every Start.
type Member struct {
	ID       uint64
	Addr     string
	FSM      *fsm.FSM
	Node     *node.Node
	Server   *server.Server
	Metrics  *server.Metrics
	Registry *obs.Registry
}

type member struct {
	Member
	st      *storage.Mem
	lis     net.Listener
	gs      *grpc.Server
	tr      *transport.GRPC
	cancel  context.CancelFunc
	swept   chan struct{}
	running bool

	// fault, with Options.Faults, sits between the node and its transport for the cluster's
	// lifetime; it forwards to whichever transport and node the member currently runs.
	fault  *transport.Fault
	curTr  atomic.Pointer[transport.GRPC]
	curNod atomic.Pointer[node.Node]
}

// Send forwards the fault's output to the member's current transport (none while it is down).
func (m *member) Send(msgs []*raftpb.Message) {
	if tr := m.curTr.Load(); tr != nil {
		tr.Send(msgs)
	}
}

// dropped reports a snapshot the fault dropped, so the sending node stops waiting for it; other
// dropped messages are plain losses raft recovers from.
func (m *member) dropped(msg *raftpb.Message) {
	if msg.GetType() != raftpb.MessageType_MsgSnap {
		return
	}
	if n := m.curNod.Load(); n != nil {
		n.ReportSnapshot(msg.GetTo(), raft.SnapshotFailure)
	}
}

// Cluster is an in-process cluster. Its methods are safe for concurrent use.
type Cluster struct {
	t     testing.TB
	opts  Options
	peers []node.Peer

	stopTicks context.CancelFunc // with Options.Faults: stops the fault ticker
	ticked    chan struct{}

	mu      sync.Mutex
	members map[uint64]*member
}

// New starts a cluster of opts.Size members and stops it in t.Cleanup.
func New(t testing.TB, opts Options) *Cluster {
	t.Helper()
	if opts.Size == 0 {
		opts.Size = 3
	}
	if opts.TickInterval == 0 {
		opts.TickInterval = 10 * time.Millisecond
	}
	if opts.Clock == nil {
		opts.Clock = clock.Real{}
	}
	if opts.ProposalTimeout == 0 {
		opts.ProposalTimeout = time.Second
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	c := &Cluster{t: t, opts: opts, members: map[uint64]*member{}}
	for i := 1; i <= opts.Size; i++ {
		lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("kvtest: listen: %v", err)
		}
		id := uint64(i)
		c.peers = append(c.peers, node.Peer{ID: id, Addr: lis.Addr().String()})
		m := &member{Member: Member{ID: id, Addr: lis.Addr().String()}, st: storage.NewMem(), lis: lis}
		if opts.Faults {
			m.fault = transport.NewFault(m, opts.FaultSeed+int64(id), m.dropped) //nolint:gosec // G115: small member id
		}
		c.members[id] = m
	}
	if opts.Faults {
		c.tickFaults()
	}
	t.Cleanup(c.stopAll)
	for _, id := range c.IDs() {
		c.Start(id)
	}
	return c
}

// tickFaults releases delayed messages every TickInterval until the cluster stops.
func (c *Cluster) tickFaults() {
	ctx, cancel := context.WithCancel(context.Background())
	c.stopTicks, c.ticked = cancel, make(chan struct{})
	go func() {
		defer close(c.ticked)
		tk := clock.Real{}.NewTicker(c.opts.TickInterval)
		defer tk.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tk.C():
				for _, id := range c.IDs() {
					c.members[id].fault.Tick()
				}
			}
		}
	}()
}

// faults returns every member's Fault, failing the test unless Options.Faults is set.
func (c *Cluster) faults() []*transport.Fault {
	c.t.Helper()
	if !c.opts.Faults {
		c.t.Fatal("kvtest: fault injection needs Options.Faults")
	}
	var out []*transport.Fault
	for _, id := range c.IDs() {
		out = append(out, c.members[id].fault)
	}
	return out
}

// Partition splits the members into groups that reach only their own group (both directions);
// a member in no group reaches nobody. Needs Options.Faults.
func (c *Cluster) Partition(groups ...[]uint64) {
	c.t.Helper()
	for _, f := range c.faults() {
		f.Partition(groups...)
	}
}

// Heal ends any partition and clears the link faults SetFaults set. Needs Options.Faults.
func (c *Cluster) Heal() {
	c.t.Helper()
	for _, f := range c.faults() {
		f.Heal()
	}
}

// SetFaults applies x to every link, for example delays (MaxDelayTicks × TickInterval). Needs
// Options.Faults.
func (c *Cluster) SetFaults(x transport.Faults) {
	c.t.Helper()
	for _, f := range c.faults() {
		f.SetDefault(x)
	}
}

// Backup copies stopped member id's storage (snapshot, log and hard state), as a copy of its data
// directory would.
func (c *Cluster) Backup(id uint64) *storage.Mem {
	c.t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.members[id]
	if m.running {
		c.t.Fatalf("kvtest: back up %d while it runs", id)
	}
	cp, err := clone(m.st)
	if err != nil {
		c.t.Fatalf("kvtest: back up %d: %v", id, err)
	}
	return cp
}

// Restore replaces stopped member id's storage with a copy of st, as restoring an old copy of its
// data directory would; the next Start runs on it.
func (c *Cluster) Restore(id uint64, st *storage.Mem) {
	c.t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.members[id]
	if m.running {
		c.t.Fatalf("kvtest: restore %d while it runs", id)
	}
	cp, err := clone(st)
	if err != nil {
		c.t.Fatalf("kvtest: restore %d: %v", id, err)
	}
	m.st = cp
}

// clone copies src in one write: its snapshot, the log after it, and the hard state. Entries a
// compaction kept before the snapshot are not copied; the snapshot covers them.
func clone(src *storage.Mem) (*storage.Mem, error) {
	snap, err := src.Snapshot()
	if err != nil {
		return nil, err
	}
	hs, _, err := src.InitialState()
	if err != nil {
		return nil, err
	}
	first, _ := src.FirstIndex() // Mem never fails these
	last, _ := src.LastIndex()
	var ents []*raftpb.Entry
	if last >= first {
		if ents, err = src.Entries(first, last+1, math.MaxUint64); err != nil {
			return nil, err
		}
	}
	dst := storage.NewMem()
	if err := dst.Save(hs, ents, snap); err != nil {
		return nil, fmt.Errorf("copy storage: %w", err)
	}
	return dst, nil
}

// IDs returns the member ids, ascending.
func (c *Cluster) IDs() []uint64 {
	ids := make([]uint64, 0, len(c.peers))
	for _, p := range c.peers {
		ids = append(ids, p.ID)
	}
	return ids
}

// Endpoints returns every member's client address, in id order.
func (c *Cluster) Endpoints() []string {
	eps := make([]string, 0, len(c.peers))
	for _, p := range c.peers {
		eps = append(eps, p.Addr)
	}
	return eps
}

// Member returns id's current stack, or nil while it is stopped.
func (c *Cluster) Member(id uint64) *Member {
	c.mu.Lock()
	defer c.mu.Unlock()
	if m := c.members[id]; m.running {
		mm := m.Member
		return &mm
	}
	return nil
}

// Start starts (or restarts) member id over its storage.
func (c *Cluster) Start(id uint64) {
	c.t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.members[id]
	if m.running {
		return
	}
	if err := c.start(m); err != nil {
		c.t.Fatalf("kvtest: start %d: %v", id, err)
	}
}

func (c *Cluster) start(m *member) error {
	if m.lis == nil {
		lis, err := listenAgain(m.Addr)
		if err != nil {
			return err
		}
		m.lis = lis
	}
	reg := obs.NewRegistry()
	nm, err := node.NewMetrics(reg)
	if err != nil {
		return err
	}
	sm, err := server.NewMetrics(reg)
	if err != nil {
		return err
	}
	f := fsm.New(fsm.Options{})
	var st storage.Storage = m.st
	if c.opts.WrapStorage != nil {
		st = c.opts.WrapStorage(m.ID, st)
	}
	n, err := node.New(node.Config{
		ID: m.ID, Peers: c.peers, TickInterval: c.opts.TickInterval, SnapshotEntries: c.opts.SnapshotEntries,
		SnapshotTrailing: c.opts.SnapshotTrailing, Logger: c.opts.Logger, Metrics: nm,
	}, st, fsm.NewApplier(f))
	if err != nil {
		return err
	}
	members := make([]*pb.Member, 0, len(c.peers))
	peers := map[uint64]string{}
	for _, p := range c.peers {
		members = append(members, &pb.Member{Id: p.ID, PeerAddr: p.Addr, ClientAddr: p.Addr})
		peers[p.ID] = p.Addr
	}
	srv, err := server.New(server.Config{
		NodeID: m.ID, Members: members, Clock: c.clockOf(m.ID), ProposalTimeout: c.opts.ProposalTimeout,
		SweepInterval: c.opts.SweepInterval, Logger: c.opts.Logger, Metrics: sm, AdminToken: c.opts.AdminToken,
	}, n, f)
	if err != nil {
		return err
	}
	tr, err := transport.New(transport.Config{ID: m.ID, ClusterID: ClusterID, Peers: peers, Logger: c.opts.Logger}, n)
	if err != nil {
		return err
	}
	gs := grpc.NewServer(grpc.ChainUnaryInterceptor(srv.UnaryInterceptor()))
	tr.Register(gs)
	srv.Register(gs)
	go func(lis net.Listener) { _ = gs.Serve(lis) }(m.lis) // returns when Kill stops gs
	var send node.Transport = tr
	if m.fault != nil {
		m.curTr.Store(tr)
		m.curNod.Store(n)
		send = m.fault
	}
	if err := n.Start(send); err != nil {
		gs.Stop()
		tr.Close()
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	swept := make(chan struct{})
	go func() {
		defer close(swept)
		srv.RunSweeper(ctx)
	}()
	m.FSM, m.Node, m.Server, m.Metrics, m.Registry = f, n, srv, sm, reg
	m.gs, m.tr, m.cancel, m.swept, m.running = gs, tr, cancel, swept, true
	return nil
}

// listenAgain re-binds a stopped member's address; the port can take a moment to free up.
func listenAgain(addr string) (net.Listener, error) {
	var lis net.Listener
	var err error
	testutil.Eventually(func() bool {
		lis, err = (&net.ListenConfig{}).Listen(context.Background(), "tcp", addr)
		return err == nil
	}, 5*time.Second, 20*time.Millisecond)
	if err != nil {
		return nil, fmt.Errorf("re-listen on %s: %w", addr, err)
	}
	return lis, nil
}

// Kill stops member id abruptly, like a crashed process: its connections close and in-flight
// requests fail. Its storage is kept for Start.
func (c *Cluster) Kill(id uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.kill(c.members[id])
}

func (c *Cluster) kill(m *member) {
	if !m.running {
		return
	}
	m.cancel()
	<-m.swept
	m.curTr.Store(nil) // the fault forwards to nothing until the next Start
	m.curNod.Store(nil)
	m.gs.Stop() // closes the listener too
	m.Node.Stop()
	m.tr.Close()
	m.lis, m.running = nil, false
}

// Wipe replaces stopped member id's storage with an empty one, as if its data directory were
// deleted; the next Start bootstraps it and it catches up from the leader (04.06).
func (c *Cluster) Wipe(id uint64) {
	c.t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.members[id]
	if m.running {
		c.t.Fatalf("kvtest: wipe %d while it runs", id)
	}
	m.st = storage.NewMem()
}

// clockOf is member id's clock: Options.MemberClock's, else Options.Clock.
func (c *Cluster) clockOf(id uint64) clock.Clock {
	if c.opts.MemberClock != nil {
		if clk := c.opts.MemberClock(id); clk != nil {
			return clk
		}
	}
	return c.opts.Clock
}

func (c *Cluster) stopAll() {
	if c.stopTicks != nil {
		c.stopTicks()
		<-c.ticked
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, m := range c.members {
		c.kill(m)
		if m.lis != nil {
			_ = m.lis.Close() // never started
		}
	}
}

// Leader returns the leader every running member agrees on, or 0.
func (c *Cluster) Leader() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	var lead uint64
	for _, id := range c.IDs() {
		m := c.members[id]
		if !m.running {
			continue
		}
		s := m.Node.Status()
		if s.Leader == 0 || (lead != 0 && s.Leader != lead) {
			return 0
		}
		lead = s.Leader
	}
	if m := c.members[lead]; lead == 0 || !m.running || !m.Node.Status().IsLeader {
		return 0
	}
	return lead
}

// WaitLeader waits up to timeout for an agreed leader and returns it, failing the test otherwise.
func (c *Cluster) WaitLeader(timeout time.Duration) uint64 {
	c.t.Helper()
	var lead uint64
	if !testutil.Eventually(func() bool { lead = c.Leader(); return lead != 0 }, timeout, 5*time.Millisecond) {
		c.t.Fatalf("kvtest: no agreed leader within %s", timeout)
	}
	return lead
}

// Others returns the member ids other than not.
func (c *Cluster) Others(not ...uint64) []uint64 {
	return slices.DeleteFunc(c.IDs(), func(id uint64) bool { return slices.Contains(not, id) })
}

// Conn dials member id's address; the connection is closed in t.Cleanup. It stays usable across
// Kill and Start (gRPC reconnects).
func (c *Cluster) Conn(id uint64) *grpc.ClientConn {
	c.t.Helper()
	conn, err := grpc.NewClient(c.members[id].Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		c.t.Fatalf("kvtest: dial %d: %v", id, err)
	}
	c.t.Cleanup(func() { _ = conn.Close() })
	return conn
}
