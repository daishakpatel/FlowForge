package transport

import (
	"bytes"
	"errors"
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	coordinationv1 "github.com/daishakpatel/flowforge/internal/gen/proto/coordination/v1"
)

func snapMsg(size int) *pb.Message {
	data := bytes.Repeat([]byte{0xab}, size)
	for i := range data {
		data[i] = byte(i % 251)
	}
	return &pb.Message{
		Type: pb.MsgSnap.Enum(), From: proto.Uint64(1), To: proto.Uint64(2), Term: proto.Uint64(3),
		Snapshot: &pb.Snapshot{Data: data, Metadata: &pb.SnapshotMetadata{Index: proto.Uint64(50), Term: proto.Uint64(3)}},
	}
}

func TestSnapshotChunks_RoundTrip(t *testing.T) {
	for _, size := range []int{0, 1, chunkSize, chunkSize + 1, 2*chunkSize + 17} {
		m := snapMsg(size)
		chunks, err := snapshotChunks(m)
		if err != nil {
			t.Fatalf("size %d: %v", size, err)
		}
		if want := max(1, (size+chunkSize-1)/chunkSize); len(chunks) != want {
			t.Errorf("size %d: %d chunks, want %d", size, len(chunks), want)
		}
		var a assembler
		var got *pb.Message
		for i, c := range chunks {
			if len(c.GetData()) > chunkSize {
				t.Fatalf("chunk %d has %d bytes", i, len(c.GetData()))
			}
			out, err := a.add(c)
			if err != nil {
				t.Fatalf("size %d chunk %d: %v", size, i, err)
			}
			if (out != nil) != (i == len(chunks)-1) {
				t.Fatalf("size %d: message completed at chunk %d of %d", size, i, len(chunks))
			}
			got = out
		}
		if !proto.Equal(got, m) {
			t.Errorf("size %d: reassembled message differs", size)
		}
		if len(m.GetSnapshot().GetData()) != size {
			t.Errorf("size %d: snapshotChunks modified the original message", size)
		}
	}
}

func TestAssembler_RejectsBrokenStreams(t *testing.T) {
	chunks, err := snapshotChunks(snapMsg(2*chunkSize + 5))
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string][]*coordinationv1.SnapshotChunk{
		"starts mid-way": {chunks[1]},
		"skips a chunk":  {chunks[0], chunks[2]},
		"repeats one":    {chunks[0], chunks[0]},
		"oversized":      {{Message: chunks[0].GetMessage(), TotalSize: 1 << 40}},
		"short total":    {{Message: chunks[0].GetMessage(), Data: []byte("abc"), TotalSize: 2, Last: true}},
		"bad message":    {{Message: []byte{0xff, 0xff}, TotalSize: 0, Last: true}},
	}
	for name, seq := range cases {
		t.Run(name, func(t *testing.T) {
			var a assembler
			var last error
			for _, c := range seq {
				if _, last = a.add(c); last != nil {
					break
				}
			}
			if !errors.Is(last, errBadChunk) {
				t.Errorf("err = %v, want errBadChunk", last)
			}
		})
	}
}
