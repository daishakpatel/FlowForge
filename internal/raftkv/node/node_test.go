package node

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/daishakpatel/flowforge/internal/obs"
	"github.com/daishakpatel/flowforge/internal/raftkv/storage"
	"github.com/daishakpatel/flowforge/internal/testutil"
)

func TestMain(m *testing.M) { testutil.LeakCheck(m) }

// listApplier records applied entries; its snapshot is the whole list. It flags any index that
// does not increase (rule 2).
type listApplier struct {
	mu      sync.Mutex
	entries []applied
	bad     []string
}

type applied struct {
	Index uint64
	Data  string
}

func (a *listApplier) Apply(data []byte, index uint64) ([]byte, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if n := len(a.entries); n > 0 && index <= a.entries[n-1].Index {
		a.bad = append(a.bad, fmt.Sprintf("index %d after %d", index, a.entries[n-1].Index))
	}
	a.entries = append(a.entries, applied{index, string(data)})
	if bytes.HasPrefix(data, []byte("fail")) {
		return nil, errors.New("command refused")
	}
	return append([]byte("ok:"), data...), nil
}

func (a *listApplier) Snapshot() ([]byte, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return json.Marshal(a.entries)
}

func (a *listApplier) Restore(data []byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.entries = nil
	return json.Unmarshal(data, &a.entries)
}

func (a *listApplier) list() []applied {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.bad) > 0 {
		panic(fmt.Sprint("applier saw out-of-order entries: ", a.bad))
	}
	return slices.Clone(a.entries)
}

type single struct {
	n      *Node
	ticker *ManualTicker
	app    *listApplier
}

// startSingle starts a one-voter node over st and ticks it until it leads.
func startSingle(t *testing.T, st storage.Storage, mod func(*Config)) *single {
	t.Helper()
	tk := NewManualTicker()
	cfg := Config{ID: 1, Peers: []Peer{{ID: 1, Addr: "n1:7001"}}, Ticker: tk, SnapshotEntries: 5, SnapshotTrailing: 2,
		Entropy: bytes.NewReader([]byte("0123456789abcdef"))}
	if mod != nil {
		mod(&cfg)
	}
	app := &listApplier{}
	n, err := New(cfg, st, app)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := n.Start(nopTransport{}); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(n.Stop)
	for i := 0; i < 50 && !n.Status().IsLeader; i++ {
		tk.Tick()
	}
	if !n.Status().IsLeader {
		t.Fatalf("single node did not become leader: %+v", n.Status())
	}
	return &single{n: n, ticker: tk, app: app}
}

type nopTransport struct{}

func (nopTransport) Send([]*pb.Message) {}

func propose(t *testing.T, n *Node, data string) Result {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := n.Propose(ctx, []byte(data))
	if err != nil {
		t.Fatalf("Propose(%q): %v", data, err)
	}
	return res
}

func TestNode_ProposeWaitsForApply(t *testing.T) {
	s := startSingle(t, storage.NewMem(), nil)
	res := propose(t, s.n, "a")
	if string(res.Data) != "ok:a" || res.Err != nil || res.Index == 0 {
		t.Fatalf("result = %+v", res)
	}
	// Rule 3: once Propose returns, the entry is already applied here.
	if l := s.app.list(); len(l) != 1 || l[0].Index != res.Index {
		t.Fatalf("applied = %v, want entry %d", l, res.Index)
	}
	// Status is published after the loop iteration that applied the entry, so it can trail
	// Propose's return by an instant (a flake under -race, fixed in 04.04).
	var st Status
	if !testutil.Eventually(func() bool { st = s.n.Status(); return st.Applied >= res.Index }, 5*time.Second, time.Millisecond) ||
		st.Commit < res.Index || st.Leader != 1 || st.LeaderAddr != "n1:7001" {
		t.Errorf("status = %+v", st)
	}
	// The applier's error is the command's result, not a proposal failure.
	if res := propose(t, s.n, "fail-me"); res.Err == nil {
		t.Errorf("Result.Err = nil, want the applier's error")
	}
}

func TestNode_ReadIndexCoversCommittedWrites(t *testing.T) {
	s := startSingle(t, storage.NewMem(), nil)
	res := propose(t, s.n, "a")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	idx, err := s.n.ReadIndex(ctx)
	if err != nil || idx < res.Index || s.n.Status().Applied < idx {
		t.Fatalf("ReadIndex = %d, %v; write at %d, applied %d", idx, err, res.Index, s.n.Status().Applied)
	}
}

func TestNode_RestartResumesFromSnapshotWithoutReapplying(t *testing.T) {
	st := storage.NewMem()
	s := startSingle(t, st, nil)
	for i := range 12 {
		propose(t, s.n, fmt.Sprint("e", i))
	}
	before := s.app.list()
	s.n.Stop()
	snap, _ := st.LoadSnapshot()
	snapIdx := snap.GetMetadata().GetIndex()
	if snapIdx <= 1 {
		t.Fatalf("no snapshot taken (index %d) with SnapshotEntries=5", snapIdx)
	}
	if first, _ := st.FirstIndex(); first <= 2 {
		t.Errorf("log not compacted: first index %d", first)
	}

	s2 := startSingle(t, st, nil)
	after := s2.app.list() // restored from the snapshot, then the rest of the log re-applied
	if !slices.Equal(after, before) {
		t.Fatalf("after restart applied %v, want %v", after, before)
	}
	res := propose(t, s2.n, "after-restart")
	if l := s2.app.list(); l[len(l)-1].Index != res.Index || len(l) != len(before)+1 {
		t.Errorf("post-restart proposal not applied once: %v", l[len(before):])
	}
}

func TestNode_RefusesChangedPeers(t *testing.T) {
	st := storage.NewMem()
	s := startSingle(t, st, nil)
	s.n.Stop()
	_, err := New(Config{ID: 1, Peers: []Peer{{ID: 1, Addr: "a:1"}, {ID: 2, Addr: "b:1"}, {ID: 3, Addr: "c:1"}}}, st, &listApplier{})
	if !errors.Is(err, ErrPeersMismatch) {
		t.Fatalf("New with other peers = %v, want ErrPeersMismatch", err)
	}
}

func TestNode_BootstrapIsIdenticalOnEveryNode(t *testing.T) {
	peers := []Peer{{ID: 3, Addr: "c:1"}, {ID: 1, Addr: "a:1"}, {ID: 2, Addr: "b:1"}}
	var snaps []*pb.Snapshot
	for id := uint64(1); id <= 3; id++ {
		st := storage.NewMem()
		if _, err := New(Config{ID: id, Peers: peers}, st, &listApplier{}); err != nil {
			t.Fatalf("New(%d): %v", id, err)
		}
		snap, _ := st.LoadSnapshot()
		snaps = append(snaps, snap)
	}
	for _, s := range snaps[1:] {
		if !proto.Equal(s, snaps[0]) {
			t.Fatalf("bootstrap snapshots differ: %v vs %v", s, snaps[0])
		}
	}
	if v := snaps[0].GetMetadata().GetConfState().GetVoters(); !slices.Equal(v, []uint64{1, 2, 3}) {
		t.Errorf("bootstrap voters = %v", v)
	}
}

func TestNode_ConfChangePathIsWired(t *testing.T) {
	s := startSingle(t, storage.NewMem(), nil)
	cc := &pb.ConfChange{Type: pb.ConfChangeAddLearnerNode.Enum(), NodeId: proto.Uint64(9)}
	if err := s.n.proposeConfChange(cc); err != nil {
		t.Fatal(err)
	}
	propose(t, s.n, "after") // applied after the conf change entry
	s.n.Stop()
	if l := s.n.confState.GetLearners(); !slices.Equal(l, []uint64{9}) {
		t.Errorf("learners after ApplyConfChange = %v, want [9]", l)
	}
}

func TestNode_StoppedAndCancelled(t *testing.T) {
	s := startSingle(t, storage.NewMem(), nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.n.Propose(ctx, []byte("x")); !errors.Is(err, ErrProposalTimeout) {
		t.Errorf("cancelled Propose = %v, want ErrProposalTimeout", err)
	}
	if _, err := s.n.ReadIndex(ctx); !errors.Is(err, ErrReadIndexTimeout) {
		t.Errorf("cancelled ReadIndex = %v, want ErrReadIndexTimeout", err)
	}
	s.n.Stop()
	if _, err := s.n.Propose(context.Background(), []byte("x")); !errors.Is(err, ErrNodeStopped) {
		t.Errorf("Propose after Stop = %v, want ErrNodeStopped", err)
	}
	if s.ticker.Tick() {
		t.Error("Tick after Stop reported delivery")
	}
	if err := s.n.Start(nopTransport{}); err == nil {
		t.Error("Start after Stop succeeded")
	}
}

// failingStorage fails every Save once armed.
type failingStorage struct {
	storage.Storage
	mu    sync.Mutex
	armed bool
}

func (f *failingStorage) Save(hs *pb.HardState, ents []*pb.Entry, snap *pb.Snapshot) error {
	f.mu.Lock()
	armed := f.armed
	f.mu.Unlock()
	if armed {
		return errors.New("disk on fire")
	}
	return f.Storage.Save(hs, ents, snap)
}

func TestNode_StorageFailureStopsTheNode(t *testing.T) {
	fs := &failingStorage{Storage: storage.NewMem()}
	s := startSingle(t, fs, nil)
	fs.mu.Lock()
	fs.armed = true
	fs.mu.Unlock()
	_, err := s.n.Propose(context.Background(), []byte("x"))
	if !errors.Is(err, ErrNodeStopped) {
		t.Fatalf("Propose with failing storage = %v, want ErrNodeStopped", err)
	}
	<-s.n.Done()
	if s.n.Err() == nil || len(s.app.list()) != 0 {
		t.Errorf("Err = %v, applied %v: the unpersisted entry must not apply", s.n.Err(), s.app.list())
	}
}

func TestNode_NotLeaderErrors(t *testing.T) {
	n := &Node{addrs: map[uint64]string{2: "b:7001"}}
	var nle *NotLeaderError
	if err := n.notLeader(2); !errors.Is(err, ErrNotLeader) || !errors.As(err, &nle) || nle.Leader != 2 || nle.Addr != "b:7001" {
		t.Errorf("notLeader(2) = %v", err)
	}
	if err := n.notLeader(0); !errors.Is(err, ErrNoQuorum) {
		t.Errorf("notLeader(0) = %v, want ErrNoQuorum", err)
	}
}

func TestNode_Metrics(t *testing.T) {
	m, err := NewMetrics(obs.NewRegistry()) // obs enforces the 00E §11 names and allowed labels
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}
	s := startSingle(t, storage.NewMem(), func(c *Config) { c.Metrics = m })
	res := propose(t, s.n, "a")
	s.n.Stop() // the last loop iteration has updated the gauges
	for name, got := range map[string]float64{
		"is_leader": promtest.ToFloat64(m.isLeader), "applied_index": promtest.ToFloat64(m.applied),
		"commit_index": promtest.ToFloat64(m.commit), "proposals_pending": promtest.ToFloat64(m.pending),
	} {
		want := map[string]float64{"is_leader": 1, "applied_index": float64(res.Index), "commit_index": float64(res.Index),
			"proposals_pending": 0}[name]
		if got != want {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
	if promtest.ToFloat64(m.term) < 2 {
		t.Errorf("term gauge = %v", promtest.ToFloat64(m.term))
	}
	if _, err := NewMetrics(obs.NewRegistry()); err != nil {
		t.Errorf("a second registry must accept the metrics: %v", err)
	}
}

func TestNode_RejectsOversizedProposals(t *testing.T) {
	s := startSingle(t, storage.NewMem(), nil)
	_, err := s.n.Propose(context.Background(), make([]byte, 1<<20+1)) // over MaxSizePerMsg
	if !errors.Is(err, ErrProposalTooLarge) {
		t.Fatalf("Propose(1 MiB + 1) = %v, want ErrProposalTooLarge", err)
	}
	if l := s.app.list(); len(l) != 0 {
		t.Errorf("oversized proposal applied: %v", l)
	}
}

func TestConfig_RejectsNonPositiveTickInterval(t *testing.T) {
	c := Config{ID: 1, Peers: []Peer{{ID: 1, Addr: "a:1"}}, TickInterval: -time.Millisecond}.withDefaults()
	if err := c.validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("validate() = %v, want ErrInvalidConfig", err)
	}
}

// TestNode_SnapshotReportsAreNeverDropped: a flood of unreachable reports must not crowd out a
// snapshot report (raft keeps the follower paused until it gets one), and reports coalesce per
// peer instead of growing a queue.
func TestNode_SnapshotReportsAreNeverDropped(t *testing.T) {
	n, err := New(Config{ID: 1, Peers: []Peer{{ID: 1, Addr: "a:1"}, {ID: 2, Addr: "b:1"}, {ID: 3, Addr: "c:1"}}}, storage.NewMem(), &listApplier{})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Stop()
	for range 5000 {
		n.ReportUnreachable(2)
	}
	n.ReportSnapshot(2, raft.SnapshotFailure)
	n.reportMu.Lock()
	defer n.reportMu.Unlock()
	if st, ok := n.snapReports[2]; !ok || st != raft.SnapshotFailure || len(n.unreachable) != 1 {
		t.Errorf("pending reports: snapshot %v (%v), unreachable %v", st, ok, n.unreachable)
	}
}

func TestNode_UnknownPeersGetNoMetricSeries(t *testing.T) {
	m, err := NewMetrics(obs.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	n, err := New(Config{ID: 1, Peers: []Peer{{ID: 1, Addr: "a:1"}, {ID: 2, Addr: "b:1"}, {ID: 3, Addr: "c:1"}}, Metrics: m}, storage.NewMem(), &listApplier{})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Stop()
	for id := uint64(100); id < 110; id++ {
		_ = n.Step(&pb.Message{Type: pb.MsgHeartbeat.Enum(), From: proto.Uint64(id), To: proto.Uint64(1)})
		n.ReportUnreachable(id)
	}
	_ = n.Step(&pb.Message{Type: pb.MsgHeartbeat.Enum(), From: proto.Uint64(2), To: proto.Uint64(1)})
	n.ReportUnreachable(3)
	if got := promtest.CollectAndCount(m.unreachable); got != 2 {
		t.Errorf("%d peer_unreachable series, want 2 (configured peers only)", got)
	}
	if r, u := promtest.ToFloat64(m.unreachable.WithLabelValues("2")), promtest.ToFloat64(m.unreachable.WithLabelValues("3")); r != 0 || u != 1 {
		t.Errorf("peer_unreachable: peer 2 (heard from) = %v, peer 3 (reported) = %v", r, u)
	}
}

// failingSnapshotApplier fails every Snapshot call and counts them.
type failingSnapshotApplier struct {
	listApplier
	calls int
}

func (a *failingSnapshotApplier) Snapshot() ([]byte, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls++
	return nil, errors.New("too big")
}

func TestNode_FailedSnapshotIsNotRetriedEveryReady(t *testing.T) {
	tk := NewManualTicker()
	app := &failingSnapshotApplier{}
	n, err := New(Config{ID: 1, Peers: []Peer{{ID: 1, Addr: "a:1"}}, Ticker: tk, SnapshotEntries: 5}, storage.NewMem(), app)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Start(nopTransport{}); err != nil {
		t.Fatal(err)
	}
	defer n.Stop()
	for i := 0; i < 50 && !n.Status().IsLeader; i++ {
		tk.Tick()
	}
	for i := range 6 {
		propose(t, n, fmt.Sprint("e", i))
	}
	for range 30 {
		tk.Tick()
	}
	app.mu.Lock()
	defer app.mu.Unlock()
	if app.calls != 1 {
		t.Errorf("Snapshot called %d times for 6 applied entries with SnapshotEntries=5, want 1", app.calls)
	}
}
