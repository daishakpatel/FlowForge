package server

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/daishakpatel/flowforge/internal/gen/proto/coordination/v1"
	"github.com/daishakpatel/flowforge/internal/raftkv/node"
)

// TestNodeErr maps node errors to statuses; when the caller's ctx has ended its own error wins
// over the server-side timeout it caused (the client could not tell them apart otherwise).
func TestNodeErr(t *testing.T) {
	live := context.Background()
	done, cancel := context.WithCancel(context.Background())
	cancel()
	cases := []struct {
		name string
		ctx  context.Context
		err  error
		want codes.Code
	}{
		{"not leader", live, &node.NotLeaderError{Leader: 2}, codes.FailedPrecondition},
		{"no quorum", live, node.ErrNoQuorum, codes.Unavailable},
		{"too large", live, node.ErrProposalTooLarge, codes.ResourceExhausted},
		{"server timeout", live, node.ErrProposalTimeout, codes.Unavailable},
		{"read index timeout", live, node.ErrReadIndexTimeout, codes.Unavailable},
		{"dropped", live, node.ErrProposalDropped, codes.Unavailable},
		{"stopped", live, node.ErrNodeStopped, codes.Unavailable},
		{"caller gave up", done, node.ErrProposalTimeout, codes.Canceled},
		{"unknown", live, errors.New("boom"), codes.Internal},
	}
	for _, c := range cases {
		if got := status.Code(nodeErr(c.ctx, c.err)); got != c.want {
			t.Errorf("%s: code %s, want %s", c.name, got, c.want)
		}
	}
}

func TestResultErr(t *testing.T) {
	cases := map[pb.ResultCode]codes.Code{
		pb.ResultCode_RESULT_CODE_VERSION_MISMATCH: codes.Aborted,
		pb.ResultCode_RESULT_CODE_KEY_NOT_FOUND:    codes.Aborted,
		pb.ResultCode_RESULT_CODE_LEASE_NOT_FOUND:  codes.NotFound,
		pb.ResultCode_RESULT_CODE_LEASE_EXPIRED:    codes.NotFound,
		pb.ResultCode_RESULT_CODE_TXN_TOO_LARGE:    codes.InvalidArgument,
		pb.ResultCode_RESULT_CODE_INVALID_ARGUMENT: codes.InvalidArgument,
		pb.ResultCode_RESULT_CODE_QUOTA_EXCEEDED:   codes.ResourceExhausted,
		pb.ResultCode_RESULT_CODE_UNSPECIFIED:      codes.Internal,
	}
	for rc, want := range cases {
		if got := status.Code(resultErr(&pb.CommandResult{Code: rc})); got != want {
			t.Errorf("%s: code %s, want %s", rc, got, want)
		}
	}
}
