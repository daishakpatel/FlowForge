package fsm_test

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	pb "github.com/daishakpatel/flowforge/internal/gen/proto/coordination/v1"
	"github.com/daishakpatel/flowforge/internal/raftkv/fsm"
	"github.com/daishakpatel/flowforge/internal/raftkv/node"
	"github.com/daishakpatel/flowforge/internal/raftkv/storage"
	"github.com/daishakpatel/flowforge/internal/testutil"
)

func TestMain(m *testing.M) { testutil.LeakCheck(m) }

// The adapter is what 04.02's node drives.
var _ node.Applier = (*fsm.Applier)(nil)

func TestApplier_RunsUnderARaftNode(t *testing.T) {
	f := fsm.New(fsm.Options{})
	tk := node.NewManualTicker()
	n, err := node.New(node.Config{ID: 1, Peers: []node.Peer{{ID: 1, Addr: "a:1"}}, Ticker: tk, SnapshotEntries: 3,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Entropy: bytes.NewReader(make([]byte, 8))}, storage.NewMem(), fsm.NewApplier(f))
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i := range 5 { // past SnapshotEntries: the node snapshots the FSM
		cmd, _ := proto.Marshal(&pb.Command{Meta: &pb.CommandMeta{TsUnixMs: 1}, Op: &pb.Command_IncrCounter{IncrCounter: &pb.IncrCounterRequest{Name: "epoch"}}})
		res, err := n.Propose(ctx, cmd)
		if err != nil || res.Err != nil {
			t.Fatalf("Propose: %v %v", err, res.Err)
		}
		out := &pb.CommandResult{}
		if err := proto.Unmarshal(res.Data, out); err != nil || out.GetCounter() != uint64(i)+1 { //nolint:gosec // i ≥ 0
			t.Fatalf("result %v (%v)", out, err)
		}
	}
	n.Stop() // Status is updated after the loop iteration that applied the entry: read it once the loop is done
	if f.AppliedIndex() != n.Status().Applied {
		t.Errorf("FSM applied %d, node applied %d", f.AppliedIndex(), n.Status().Applied)
	}
}

func TestApplier_DelegatesSnapshotAndRestore(t *testing.T) {
	f := populated(t, 100)
	a := fsm.NewApplier(f)
	snap, err := a.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	g := fsm.New(fsm.Options{})
	if err := fsm.NewApplier(g).Restore(snap); err != nil || g.StateHash() != f.StateHash() {
		t.Fatalf("Restore through the adapter: %v", err)
	}
	if _, err := a.Apply([]byte{}, 1); err == nil {
		t.Error("re-applying index 1 through the adapter must fail")
	}
}

type nopTransport struct{}

func (nopTransport) Send([]*raftpb.Message) {}
