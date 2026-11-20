package transport

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	coordinationv1 "github.com/daishakpatel/flowforge/internal/gen/proto/coordination/v1"
	"github.com/daishakpatel/flowforge/internal/testutil"
)

func TestMain(m *testing.M) { testutil.LeakCheck(m) }

const cluster = 7

// netw is an in-memory network of bufconn listeners keyed by address.
type netw struct {
	mu   sync.Mutex
	lis  map[string]*bufconn.Listener
	srvs []*grpc.Server
}

func newNet(t *testing.T) *netw {
	n := &netw{lis: map[string]*bufconn.Listener{}}
	t.Cleanup(func() {
		for _, s := range n.srvs {
			s.Stop()
		}
	})
	return n
}

func (n *netw) dialer() grpc.DialOption {
	return grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
		n.mu.Lock()
		l := n.lis[addr]
		n.mu.Unlock()
		if l == nil {
			return nil, &net.OpError{Op: "dial", Err: net.UnknownNetworkError(addr)}
		}
		return l.DialContext(ctx)
	})
}

// serve starts a gRPC server for t at addr and returns a stop function.
func (n *netw) serve(addr string, register func(grpc.ServiceRegistrar)) func() {
	l := bufconn.Listen(4 << 20)
	s := grpc.NewServer()
	register(s)
	n.mu.Lock()
	n.lis[addr] = l
	n.srvs = append(n.srvs, s)
	n.mu.Unlock()
	go func() { _ = s.Serve(l) }()
	return s.Stop
}

// recorder is a Handler that records everything.
type recorder struct {
	mu          sync.Mutex
	msgs        []*pb.Message
	unreachable int
	snaps       []raft.SnapshotStatus
}

func (r *recorder) Step(m *pb.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.msgs = append(r.msgs, m)
	return nil
}

func (r *recorder) ReportUnreachable(uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.unreachable++
}

func (r *recorder) ReportSnapshot(_ uint64, s raft.SnapshotStatus) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.snaps = append(r.snaps, s)
}

func (r *recorder) received() []*pb.Message {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*pb.Message(nil), r.msgs...)
}

func addr(id uint64) string { return fmt.Sprint("passthrough:///n", id) }

func newTransport(t *testing.T, nw *netw, id uint64, h Handler, queue int) *GRPC {
	t.Helper()
	tr, err := New(Config{
		ID: id, ClusterID: cluster, QueueSize: queue,
		Peers:       map[uint64]string{1: addr(1), 2: addr(2)},
		DialOptions: []grpc.DialOption{nw.dialer()},
	}, h)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tr.Close)
	return tr
}

func app(from, to, index uint64) *pb.Message {
	return &pb.Message{Type: pb.MsgApp.Enum(), From: proto.Uint64(from), To: proto.Uint64(to), Term: proto.Uint64(2),
		Index: proto.Uint64(index), Entries: []*pb.Entry{{Index: proto.Uint64(index + 1), Term: proto.Uint64(2), Data: []byte("x")}}}
}

func TestGRPC_DeliversMessagesInOrder(t *testing.T) {
	nw := newNet(t)
	ra, rb := &recorder{}, &recorder{}
	a, b := newTransport(t, nw, 1, ra, 0), newTransport(t, nw, 2, rb, 0)
	nw.serve("n2", b.Register)
	_ = a
	for i := range uint64(100) {
		a.Send([]*pb.Message{app(1, 2, i)})
	}
	testutil.WaitFor(t, func() bool { return len(rb.received()) == 100 }, 10*time.Second)
	for i, m := range rb.received() {
		if !proto.Equal(m, app(1, 2, uint64(i))) {
			t.Fatalf("message %d = %v", i, m)
		}
	}
}

func TestGRPC_ReceiverDropsForeignAndMalformedEnvelopes(t *testing.T) {
	nw := newNet(t)
	rb := &recorder{}
	b := newTransport(t, nw, 2, rb, 0)
	nw.serve("n2", b.Register)
	conn, err := grpc.NewClient(addr(2), grpc.WithTransportCredentials(insecure.NewCredentials()), nw.dialer())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := coordinationv1.NewRaftPeerClient(conn).Stream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	raw := func(m *pb.Message) []byte { b, _ := proto.Marshal(m); return b }
	env := func(clusterID, from, to uint64, payload []byte) *coordinationv1.Envelope {
		return &coordinationv1.Envelope{ClusterId: clusterID, From: from, To: to, Payload: &coordinationv1.Envelope_Message{Message: payload}}
	}
	for _, e := range []*coordinationv1.Envelope{
		env(99, 1, 2, raw(app(1, 2, 0))),       // other cluster
		env(cluster, 1, 3, raw(app(1, 2, 1))),  // other node
		env(cluster, 3, 2, raw(app(1, 2, 2))),  // from does not match the message
		env(cluster, 1, 2, []byte{0xff, 0xff}), // garbage
		env(cluster, 1, 2, raw(snapMsg(10))),   // snapshot outside chunks
		{ClusterId: cluster, From: 1, To: 2},   // empty
		env(cluster, 1, 2, raw(app(1, 2, 42))), // the only valid one, sent last on the same stream
	} {
		if err := s.Send(e); err != nil {
			t.Fatal(err)
		}
	}
	testutil.WaitFor(t, func() bool { return len(rb.received()) == 1 }, 10*time.Second)
	if got := rb.received(); len(got) != 1 || got[0].GetIndex() != 42 {
		t.Fatalf("received %v, want only the message with index 42", got)
	}
	_ = s.CloseSend()
}

func TestGRPC_StreamsSnapshotsInChunks(t *testing.T) {
	nw := newNet(t)
	ra, rb := &recorder{}, &recorder{}
	a, b := newTransport(t, nw, 1, ra, 0), newTransport(t, nw, 2, rb, 0)
	nw.serve("n2", b.Register)
	m := snapMsg(2*chunkSize + chunkSize/2)
	a.Send([]*pb.Message{m})
	testutil.WaitFor(t, func() bool { return len(rb.received()) == 1 }, 10*time.Second)
	if !proto.Equal(rb.received()[0], m) {
		t.Fatal("reassembled snapshot differs")
	}
	testutil.WaitFor(t, func() bool {
		ra.mu.Lock()
		defer ra.mu.Unlock()
		return len(ra.snaps) == 1 && ra.snaps[0] == raft.SnapshotFinish
	}, 10*time.Second)
}

// TestGRPC_FullQueueDropsAndReportsUnreachable is the slow-peer case: the peer never accepts a
// connection, so its queue fills; beyond the bound every message is dropped and reported, and a
// dropped snapshot is reported failed. Memory stays at QueueSize messages.
func TestGRPC_FullQueueDropsAndReportsUnreachable(t *testing.T) {
	nw := newNet(t)
	ra := &recorder{}
	a := newTransport(t, nw, 1, ra, 8) // nothing listens at n2
	for i := range uint64(20) {
		a.Send([]*pb.Message{app(1, 2, i)})
	}
	a.Send([]*pb.Message{snapMsg(10)})
	if q := a.peers[2].q; len(q) != cap(q) || cap(q) != 8 {
		t.Errorf("queue holds %d of %d", len(q), cap(q))
	}
	ra.mu.Lock()
	defer ra.mu.Unlock()
	if ra.unreachable < 13 || len(ra.snaps) != 1 || ra.snaps[0] != raft.SnapshotFailure {
		t.Errorf("unreachable reports %d (want ≥ 13), snapshot reports %v (want [failure])", ra.unreachable, ra.snaps)
	}
}

func TestGRPC_ReconnectsAfterPeerRestart(t *testing.T) {
	nw := newNet(t)
	ra, rb := &recorder{}, &recorder{}
	a, b := newTransport(t, nw, 1, ra, 0), newTransport(t, nw, 2, rb, 0)
	stop := nw.serve("n2", b.Register)
	a.Send([]*pb.Message{app(1, 2, 1)})
	testutil.WaitFor(t, func() bool { return len(rb.received()) == 1 }, 10*time.Second)
	stop()
	nw.serve("n2", b.Register)
	// Messages sent while the stream breaks may be lost (raft retransmits); keep sending until one
	// arrives over the new connection.
	i := uint64(2)
	testutil.WaitFor(t, func() bool {
		a.Send([]*pb.Message{app(1, 2, i)})
		i++
		return len(rb.received()) > 1
	}, 20*time.Second)
	ra.mu.Lock()
	defer ra.mu.Unlock()
	if ra.unreachable == 0 {
		t.Error("the broken stream was not reported unreachable")
	}
}
