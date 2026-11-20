package transport

import (
	"context"
	"fmt"
	"time"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	coordinationv1 "github.com/daishakpatel/flowforge/internal/gen/proto/coordination/v1"
)

// Reconnection backoff: doubles from backoffFirst up to backoffLimit and resets once a stream carries
// a message. No jitter: each node has at most two peers, so there is no herd to spread.
const (
	backoffFirst = 50 * time.Millisecond
	backoffLimit = 2 * time.Second
)

// peer streams the outbound messages for one peer node.
type peer struct {
	id     uint64
	t      *GRPC
	conn   *grpc.ClientConn
	client coordinationv1.RaftPeerClient
	q      chan *pb.Message
}

// enqueue never blocks: on a full queue the message is dropped and the peer reported unreachable
// (and a dropped snapshot reported failed, so raft can resend it).
func (p *peer) enqueue(m *pb.Message) {
	select {
	case p.q <- m:
	default:
		p.t.h.ReportUnreachable(p.id)
		if m.GetType() == pb.MsgSnap {
			p.t.h.ReportSnapshot(p.id, raft.SnapshotFailure)
		}
	}
}

// run keeps one stream open and drains the queue into it until ctx ends.
func (p *peer) run(ctx context.Context) {
	delay := backoffFirst
	for {
		sent, err := p.stream(ctx)
		if ctx.Err() != nil {
			return
		}
		p.t.h.ReportUnreachable(p.id)
		p.t.cfg.Logger.Debug("raft transport: peer stream failed", "peer", p.id, "err", err)
		if sent {
			delay = backoffFirst
		}
		if p.t.cfg.Clock.Sleep(ctx, delay) != nil {
			return
		}
		delay = min(2*delay, backoffLimit)
	}
}

// stream opens a stream (waiting for the connection, which gRPC redials with its own backoff)
// and sends queued messages until a send fails or ctx ends. sent reports whether any message
// went out.
func (p *peer) stream(ctx context.Context) (sent bool, err error) {
	s, err := p.client.Stream(ctx, grpc.WaitForReady(true))
	if err != nil {
		return false, fmt.Errorf("open stream: %w", err)
	}
	defer func() { _ = s.CloseSend() }() // the stream is done either way
	for {
		select {
		case <-ctx.Done():
			return sent, ctx.Err()
		case m := <-p.q:
			if err := p.send(s, m); err != nil {
				return sent, err
			}
			sent = true
		}
	}
}

// send writes one message; a MsgSnap goes as chunks and its outcome is reported to raft. A
// snapshot counts as delivered once its last chunk is written: if the follower did not install
// it, its next response makes the leader probe and resend.
func (p *peer) send(s coordinationv1.RaftPeer_StreamClient, m *pb.Message) error {
	env := func() *coordinationv1.Envelope {
		return &coordinationv1.Envelope{ClusterId: p.t.cfg.ClusterID, From: p.t.cfg.ID, To: p.id}
	}
	if m.GetType() != pb.MsgSnap {
		b, err := proto.Marshal(m)
		if err != nil {
			p.t.cfg.Logger.Warn("raft transport: unmarshalable message dropped", "err", err)
			return nil
		}
		e := env()
		e.Payload = &coordinationv1.Envelope_Message{Message: b}
		if err := s.Send(e); err != nil {
			return fmt.Errorf("send: %w", err)
		}
		return nil
	}
	chunks, err := snapshotChunks(m)
	if err != nil {
		p.t.h.ReportSnapshot(p.id, raft.SnapshotFailure)
		return nil
	}
	for _, c := range chunks {
		e := env()
		e.Payload = &coordinationv1.Envelope_SnapshotChunk{SnapshotChunk: c}
		if err := s.Send(e); err != nil {
			p.t.h.ReportSnapshot(p.id, raft.SnapshotFailure)
			return fmt.Errorf("send snapshot: %w", err)
		}
	}
	p.t.h.ReportSnapshot(p.id, raft.SnapshotFinish)
	return nil
}
