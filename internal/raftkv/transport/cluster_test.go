package transport_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	"github.com/daishakpatel/flowforge/internal/raftkv/node"
	"github.com/daishakpatel/flowforge/internal/raftkv/nodetest"
	"github.com/daishakpatel/flowforge/internal/raftkv/storage"
	"github.com/daishakpatel/flowforge/internal/raftkv/transport"
	"github.com/daishakpatel/flowforge/internal/testutil"
)

// TestGRPC_ThreeNodeClusterReplicatesAndShipsSnapshots runs real nodes over the gRPC transport:
// two nodes elect a leader and commit past several snapshots, then the third starts empty and
// catches up through a chunked snapshot.
func TestGRPC_ThreeNodeClusterReplicatesAndShipsSnapshots(t *testing.T) {
	var mu sync.Mutex
	lis := map[string]*bufconn.Listener{}
	dialer := grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
		mu.Lock()
		l := lis[addr]
		mu.Unlock()
		if l == nil {
			return nil, fmt.Errorf("no listener at %s", addr)
		}
		return l.DialContext(ctx)
	})
	peers := []node.Peer{{ID: 1, Addr: "n1"}, {ID: 2, Addr: "n2"}, {ID: 3, Addr: "n3"}}
	addrs := map[uint64]string{1: "passthrough:///n1", 2: "passthrough:///n2", 3: "passthrough:///n3"}
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	nodes, tickers, apps := map[uint64]*node.Node{}, map[uint64]*node.ManualTicker{}, map[uint64]*nodetest.Recorder{}
	start := func(id uint64) {
		tickers[id], apps[id] = node.NewManualTicker(), &nodetest.Recorder{}
		n, err := node.New(node.Config{ID: id, Peers: peers, Ticker: tickers[id], Logger: quiet,
			SnapshotEntries: 10, SnapshotTrailing: 2}, storage.NewMem(), apps[id])
		if err != nil {
			t.Fatal(err)
		}
		tr, err := transport.New(transport.Config{ID: id, ClusterID: 1, Peers: addrs, DialOptions: []grpc.DialOption{dialer}, Logger: quiet}, n)
		if err != nil {
			t.Fatal(err)
		}
		srv := grpc.NewServer()
		tr.Register(srv)
		l := bufconn.Listen(4 << 20)
		mu.Lock()
		lis[fmt.Sprint("n", id)] = l
		mu.Unlock()
		go func() { _ = srv.Serve(l) }()
		if err := n.Start(tr); err != nil {
			t.Fatal(err)
		}
		nodes[id] = n
		t.Cleanup(func() { n.Stop(); srv.Stop(); tr.Close() })
	}
	tick := func() {
		for _, tk := range tickers {
			tk.Tick()
		}
	}
	leader := func() uint64 {
		for id, n := range nodes {
			if n.Status().IsLeader {
				return id
			}
		}
		return 0
	}
	start(1)
	start(2)
	testutil.WaitFor(t, func() bool { tick(); return leader() != 0 }, 20*time.Second)
	lead := leader()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for i := range 40 {
		if _, err := nodes[lead].Propose(ctx, []byte(fmt.Sprint("e", i))); err != nil {
			t.Fatalf("Propose %d: %v", i, err)
		}
	}
	start(3)
	testutil.WaitFor(t, func() bool { tick(); return len(apps[3].Entries()) == 40 }, 20*time.Second)
	if calls := apps[3].ApplyCalls(); calls >= 40 {
		t.Errorf("node 3 applied all %d entries one by one: expected a snapshot", calls)
	}
	if got, want := apps[3].Entries(), apps[lead].Entries(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("node 3 diverges from the leader")
	}
}
