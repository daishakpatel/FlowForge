package server

import (
	"context"
	"crypto/subtle"
	"errors"
	"strings"

	"google.golang.org/grpc/metadata"

	"github.com/daishakpatel/flowforge/internal/apperrors"
	pb "github.com/daishakpatel/flowforge/internal/gen/proto/coordination/v1"
	"github.com/daishakpatel/flowforge/internal/raftkv/node"
)

// adminService is operator tooling (04.06): leadership transfer and on-demand snapshot and
// compaction. Every call needs "authorization: Bearer <AdminToken>"; with no token configured the
// service refuses everything. Until mTLS (09.01) the token travels in plaintext, so the admin
// port must stay on a private network.
type adminService struct {
	pb.UnimplementedAdminServer
	s *Server
}

// authorize checks the bearer token in constant time.
func (s *Server) authorize(ctx context.Context) error {
	if s.cfg.AdminToken == "" {
		return newErr(apperrors.CodePermissionDenied, "admin service disabled (FF_RAFT_ADMIN_TOKEN unset)", nil)
	}
	md, _ := metadata.FromIncomingContext(ctx)
	for _, v := range md.Get("authorization") {
		token, ok := strings.CutPrefix(v, "Bearer ")
		if ok && subtle.ConstantTimeCompare([]byte(token), []byte(s.cfg.AdminToken)) == 1 {
			return nil
		}
	}
	return newErr(apperrors.CodeUnauthenticated, "missing or invalid admin token", nil)
}

func (a adminService) TransferLeader(ctx context.Context, req *pb.TransferLeaderRequest) (*pb.TransferLeaderResponse, error) {
	if err := a.s.authorize(ctx); err != nil {
		return nil, err
	}
	if err := a.s.node.TransferLeadership(ctx, req.GetTo()); err != nil {
		if errors.Is(err, node.ErrInvalidConfig) {
			return nil, invalid("transfer target is not another voter")
		}
		return nil, nodeErr(ctx, err)
	}
	// Right after stepping down the node knows no leader until the new one's first message; the
	// target is then the best answer (0 when the node chose it and hasn't heard from it yet).
	lead := a.s.node.Status().Leader
	if lead == 0 {
		lead = req.GetTo()
	}
	return &pb.TransferLeaderResponse{LeaderId: lead}, nil
}

func (a adminService) SnapshotNow(ctx context.Context, _ *pb.SnapshotNowRequest) (*pb.SnapshotNowResponse, error) {
	if err := a.s.authorize(ctx); err != nil {
		return nil, err
	}
	idx, err := a.s.node.SnapshotNow(ctx)
	if err != nil {
		return nil, nodeErr(ctx, err)
	}
	return &pb.SnapshotNowResponse{SnapshotIndex: idx}, nil
}

func (a adminService) Compact(ctx context.Context, _ *pb.CompactRequest) (*pb.CompactResponse, error) {
	if err := a.s.authorize(ctx); err != nil {
		return nil, err
	}
	first, err := a.s.node.CompactNow(ctx)
	if err != nil {
		return nil, nodeErr(ctx, err)
	}
	return &pb.CompactResponse{FirstIndex: first}, nil
}
