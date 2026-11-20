package transport

import (
	"errors"
	"fmt"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	coordinationv1 "github.com/daishakpatel/flowforge/internal/gen/proto/coordination/v1"
	"github.com/daishakpatel/flowforge/internal/raftkv/storage"
)

// chunkSize is the most snapshot data one SnapshotChunk carries (raft.proto: ≤ 1 MiB).
const chunkSize = 1 << 20

// errBadChunk is wrapped by every reassembly failure; the receiver drops the partial snapshot.
var errBadChunk = errors.New("bad snapshot chunk")

// snapshotChunks splits a MsgSnap into chunks per raft.proto: the first carries the message with
// its snapshot data emptied, then the data follows in order. The chunks share m's data (no copy);
// m itself is not modified.
func snapshotChunks(m *pb.Message) ([]*coordinationv1.SnapshotChunk, error) {
	data := m.GetSnapshot().GetData()
	hb, err := proto.Marshal(withoutData(m))
	if err != nil {
		return nil, fmt.Errorf("marshal snapshot message: %w", err)
	}
	total := uint64(len(data))
	chunks := []*coordinationv1.SnapshotChunk{}
	for off := 0; ; off += chunkSize {
		end := min(off+chunkSize, len(data))
		c := &coordinationv1.SnapshotChunk{Offset: uint64(off), Data: data[off:end], TotalSize: total, Last: end == len(data)}
		if off == 0 {
			c.Message = hb
		}
		chunks = append(chunks, c)
		if c.Last {
			return chunks, nil
		}
	}
}

// withoutData returns a shallow copy of m whose snapshot has m's metadata and no data, so the
// header is marshalled without copying the (up to 64 MiB) payload.
func withoutData(m *pb.Message) *pb.Message {
	src := m.ProtoReflect()
	dst := src.New()
	src.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		dst.Set(fd, v) // shared values: dst is only marshalled
		return true
	})
	head := dst.Interface().(*pb.Message) //nolint:forcetypeassert // New keeps the type
	head.Snapshot = &pb.Snapshot{Metadata: m.GetSnapshot().GetMetadata()}
	return head
}

// assembler rebuilds one MsgSnap from its chunks on a receiving stream. Chunks must arrive in
// order on one stream; anything else is errBadChunk and resets it.
type assembler struct {
	msg   *pb.Message
	buf   []byte
	total uint64
}

// add takes the next chunk and returns the whole message after the last one.
func (a *assembler) add(c *coordinationv1.SnapshotChunk) (*pb.Message, error) {
	m, err := a.next(c)
	if err != nil || m != nil {
		*a = assembler{}
	}
	return m, err
}

func (a *assembler) next(c *coordinationv1.SnapshotChunk) (*pb.Message, error) {
	if len(c.GetMessage()) > 0 {
		if err := a.begin(c); err != nil {
			return nil, err
		}
	}
	switch {
	case a.msg == nil:
		return nil, fmt.Errorf("%w: chunk at offset %d without a first chunk", errBadChunk, c.GetOffset())
	case c.GetOffset() != uint64(len(a.buf)) || c.GetTotalSize() != a.total:
		return nil, fmt.Errorf("%w: chunk at offset %d, expected %d", errBadChunk, c.GetOffset(), len(a.buf))
	case len(c.GetData()) > chunkSize || uint64(len(a.buf)+len(c.GetData())) > a.total:
		return nil, fmt.Errorf("%w: chunk overruns the snapshot", errBadChunk)
	}
	a.buf = append(a.buf, c.GetData()...)
	if !c.GetLast() {
		return nil, nil
	}
	if uint64(len(a.buf)) != a.total {
		return nil, fmt.Errorf("%w: got %d of %d bytes", errBadChunk, len(a.buf), a.total)
	}
	a.msg.Snapshot.Data = a.buf
	return a.msg, nil
}

func (a *assembler) begin(c *coordinationv1.SnapshotChunk) error {
	switch {
	case a.msg != nil:
		return fmt.Errorf("%w: new snapshot before the last one finished", errBadChunk)
	case c.GetOffset() != 0:
		return fmt.Errorf("%w: message in a chunk at offset %d", errBadChunk, c.GetOffset())
	case c.GetTotalSize() > storage.MaxSnapshotBytes:
		return fmt.Errorf("%w: %d bytes exceeds the snapshot limit", errBadChunk, c.GetTotalSize())
	}
	m := &pb.Message{}
	if err := proto.Unmarshal(c.GetMessage(), m); err != nil || m.GetType() != pb.MsgSnap || m.GetSnapshot() == nil {
		return fmt.Errorf("%w: first chunk does not hold a MsgSnap", errBadChunk)
	}
	a.msg, a.total, a.buf = m, c.GetTotalSize(), make([]byte, 0, c.GetTotalSize())
	return nil
}
