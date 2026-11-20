// Package transport carries Raft messages between nodes (task 04.02, ADR-002). GRPC streams them
// over the RaftPeer service (proto/coordination/v1/raft.proto); Fault wraps any sender to drop,
// delay, duplicate, reorder and partition messages in tests and chaos runs (ADR-019).
package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/daishakpatel/flowforge/internal/clock"
	coordinationv1 "github.com/daishakpatel/flowforge/internal/gen/proto/coordination/v1"
)

// Handler receives what the transport delivers and reports: *node.Node implements it.
type Handler interface {
	Step(m *pb.Message) error
	ReportUnreachable(id uint64)
	ReportSnapshot(id uint64, status raft.SnapshotStatus)
}

// Config configures a GRPC transport.
type Config struct {
	ID        uint64            // this node's raft id
	ClusterID uint64            // envelopes for another cluster are dropped
	Peers     map[uint64]string // peer id → peer address; this node's own entry is ignored
	// Creds secures the outbound connections; nil means plaintext until TLS is wired (09.01).
	// The server's credentials are the caller's grpc.Server's.
	Creds       credentials.TransportCredentials
	DialOptions []grpc.DialOption // extra dial options (tests pass a bufconn dialer)
	QueueSize   int               // per-peer outbound queue, default 4096
	Clock       clock.Clock       // reconnection backoff; default clock.Real
	Logger      *slog.Logger      // default slog.Default()
}

// GRPC is the gRPC peer transport. Outbound, each peer has a bounded queue and one goroutine
// that streams its messages over one RaftPeer.Stream, reconnecting with backoff; a full queue
// drops the message and reports the peer unreachable, so memory stays bounded while a peer is
// down. Inbound, Register serves RaftPeer.Stream and hands each message to the Handler. Only the
// client-to-server direction of a stream is used.
type GRPC struct {
	cfg    Config
	h      Handler
	peers  map[uint64]*peer
	ctx    context.Context //nolint:containedctx // lifetime of the peer goroutines, cancelled by Close
	cancel context.CancelFunc
	wg     sync.WaitGroup
	once   sync.Once
}

// New creates the transport and starts its peer goroutines. Connections are made lazily.
func New(cfg Config, h Handler) (*GRPC, error) {
	if cfg.ID == 0 || cfg.ClusterID == 0 {
		return nil, errors.New("raft transport: node and cluster ids must be non-zero")
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 4096
	}
	if cfg.Clock == nil {
		cfg.Clock = clock.Real{}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	creds := cfg.Creds
	if creds == nil {
		creds = insecure.NewCredentials()
	}
	opts := append([]grpc.DialOption{grpc.WithTransportCredentials(creds)}, cfg.DialOptions...)
	t := &GRPC{cfg: cfg, h: h, peers: map[uint64]*peer{}}
	t.ctx, t.cancel = context.WithCancel(context.Background())
	for id, addr := range cfg.Peers {
		if id == cfg.ID {
			continue
		}
		conn, err := grpc.NewClient(addr, opts...)
		if err != nil {
			t.Close()
			return nil, fmt.Errorf("raft transport: peer %d at %s: %w", id, addr, err)
		}
		t.peers[id] = &peer{id: id, t: t, conn: conn, client: coordinationv1.NewRaftPeerClient(conn), q: make(chan *pb.Message, cfg.QueueSize)}
	}
	for _, p := range t.peers {
		t.wg.Add(1)
		go func() {
			defer t.wg.Done()
			p.run(t.ctx)
		}()
	}
	return t, nil
}

// Send queues each message for its peer and returns without blocking (node.Transport).
// Messages to unknown peers are dropped.
func (t *GRPC) Send(msgs []*pb.Message) {
	for _, m := range msgs {
		if p := t.peers[m.GetTo()]; p != nil {
			p.enqueue(m)
		}
	}
}

// Register serves RaftPeer on s. s must accept messages of at least 2 MiB (gRPC's default is
// 4 MiB): a MsgApp is up to MaxSizePerMsg (1 MiB) and a snapshot chunk 1 MiB plus its header.
func (t *GRPC) Register(s grpc.ServiceRegistrar) {
	coordinationv1.RegisterRaftPeerServer(s, &server{t: t})
}

// Close stops the peer goroutines and closes their connections. Queued messages are dropped.
func (t *GRPC) Close() {
	t.once.Do(func() {
		t.cancel()
		t.wg.Wait()
		for _, p := range t.peers {
			_ = p.conn.Close() // shutting down; nothing to do with the error
		}
	})
}

type server struct {
	coordinationv1.UnimplementedRaftPeerServer
	t *GRPC
}

// Stream receives one peer's envelopes in order. Envelopes for another cluster or node, and
// malformed ones, are dropped; a broken snapshot stream drops that snapshot (the sender's raft
// retries after its failure report or the next probe).
func (s *server) Stream(stream coordinationv1.RaftPeer_StreamServer) error {
	var asm assembler
	for {
		env, err := stream.Recv()
		if errors.Is(err, io.EOF) || status.Code(err) == codes.Canceled {
			return nil
		}
		if err != nil {
			return fmt.Errorf("raft transport: receive: %w", err)
		}
		m, err := s.decode(env, &asm)
		if err != nil {
			s.t.cfg.Logger.Warn("raft transport: dropped envelope", "from", env.GetFrom(), "err", err)
			continue
		}
		if m != nil {
			_ = s.t.h.Step(m) // a full inbound queue drops it; raft retransmits
		}
	}
}

// decode returns the message an envelope completes, or nil while a snapshot is still arriving.
func (s *server) decode(env *coordinationv1.Envelope, asm *assembler) (*pb.Message, error) {
	if env.GetClusterId() != s.t.cfg.ClusterID || env.GetTo() != s.t.cfg.ID {
		return nil, fmt.Errorf("envelope for cluster %d node %d", env.GetClusterId(), env.GetTo())
	}
	var m *pb.Message
	switch {
	case env.GetSnapshotChunk() != nil:
		var err error
		if m, err = asm.add(env.GetSnapshotChunk()); err != nil || m == nil {
			return nil, err
		}
	case env.GetMessage() != nil:
		m = &pb.Message{}
		if err := proto.Unmarshal(env.GetMessage(), m); err != nil {
			return nil, fmt.Errorf("decode message: %w", err)
		}
		if m.GetType() == pb.MsgSnap {
			return nil, errors.New("snapshot message outside snapshot chunks")
		}
	default:
		return nil, errors.New("empty envelope")
	}
	if m.GetTo() != s.t.cfg.ID || m.GetFrom() != env.GetFrom() {
		return nil, fmt.Errorf("message %d→%d in an envelope %d→%d", m.GetFrom(), m.GetTo(), env.GetFrom(), env.GetTo())
	}
	return m, nil
}
