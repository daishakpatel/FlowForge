// Package server serves the Raft KV's KV and Cluster gRPC services (task 04.04, FR-RAFT-04,
// FR-RAFT-06, ADR-005, ADR-006) on top of a node.Node and its fsm.FSM.
//
//   - Writes run on the leader only: it stamps the command with its clock (ts_unix_ms, ADR-005),
//     proposes it and answers once the entry has applied. A follower answers NOT_LEADER with the
//     leader's id. A write carrying (client_id, request_id) is deduplicated by the FSM, so a
//     retry after a timeout is applied at most once.
//   - LINEARIZABLE reads (the default) run on the leader after a ReadIndex; lease expiry is
//     judged with the leader's clock read after the ReadIndex. SERIALIZABLE reads answer from
//     any node's applied state, flagged stale.
//   - The sweeper (RunSweeper) proposes LeaseExpire for leases the leader's clock says expired,
//     so their keys are physically deleted.
//
// See docs/architecture/raft-kv-api.md.
package server

import (
	"errors"
	"io"
	"log/slog"
	"time"

	"google.golang.org/grpc"

	"github.com/daishakpatel/flowforge/internal/clock"
	pb "github.com/daishakpatel/flowforge/internal/gen/proto/coordination/v1"
	"github.com/daishakpatel/flowforge/internal/raftkv/fsm"
	"github.com/daishakpatel/flowforge/internal/raftkv/node"
)

// Config configures a Server. Zero values take the defaults noted.
type Config struct {
	NodeID  uint64       // this node's raft id
	Members []*pb.Member // the static members, reported by Cluster.Status
	// ProposalTimeout bounds a write's wait for apply and a read's ReadIndex, inside the
	// client's own deadline (default 3s, FF_RAFT_PROPOSAL_TIMEOUT).
	ProposalTimeout time.Duration
	SweepInterval   time.Duration // default 500ms
	SweepBatch      int           // LeaseExpire proposals per sweep, default 64
	Clock           clock.Clock   // stamps commands and judges expiry; default clock.Real
	Logger          *slog.Logger  // default: discard
	Metrics         *Metrics      // nil records nothing
	// AdminToken authorises the Admin service (FF_RAFT_ADMIN_TOKEN, 04.06); empty disables it.
	AdminToken string
}

// Server implements the KV, Cluster and Admin services for one node. Its methods are safe for
// concurrent use.
type Server struct {
	cfg     Config
	node    *node.Node
	fsm     *fsm.FSM
	started time.Time // Cluster.Status's uptime
}

// New returns a server for n, whose Applier must be fsm.NewApplier(f).
func New(cfg Config, n *node.Node, f *fsm.FSM) (*Server, error) {
	if n == nil || f == nil || cfg.NodeID == 0 {
		return nil, errors.New("raft kv server: node, fsm and node id are required")
	}
	if cfg.ProposalTimeout <= 0 {
		cfg.ProposalTimeout = 3 * time.Second
	}
	if cfg.SweepInterval <= 0 {
		cfg.SweepInterval = 500 * time.Millisecond
	}
	if cfg.SweepBatch <= 0 {
		cfg.SweepBatch = 64
	}
	if cfg.Clock == nil {
		cfg.Clock = clock.Real{}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Server{cfg: cfg, node: n, fsm: f, started: cfg.Clock.Now()}, nil
}

// Register serves KV, Cluster and Admin on r.
func (s *Server) Register(r grpc.ServiceRegistrar) {
	pb.RegisterKVServer(r, kvService{s: s})
	pb.RegisterClusterServer(r, clusterService{s: s})
	pb.RegisterAdminServer(r, adminService{s: s})
}

func (s *Server) now() int64 { return s.cfg.Clock.Now().UnixMilli() }

type kvService struct {
	pb.UnimplementedKVServer
	s *Server
}

type clusterService struct {
	pb.UnimplementedClusterServer
	s *Server
}
