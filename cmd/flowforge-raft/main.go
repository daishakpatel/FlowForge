// Command flowforge-raft runs one node of the Raft KV (task 04.04, FR-RAFT-04, ADR-002,
// ADR-006): bbolt storage under FF_RAFT_DATA_DIR, the deterministic FSM, the Raft node, the gRPC
// peer transport on FF_RAFT_PEER_ADDR, the KV and Cluster services on FF_RAFT_CLIENT_ADDR, and
// /metrics, /healthz and /readyz on FF_RAFT_HTTP_ADDR. Configuration: 00E §6.
//
// On SIGTERM or SIGINT it shuts down gracefully: a leader first hands leadership to the most
// up-to-date follower (best effort, bounded), so clients see one redirect rather than an
// election; then it stops serving clients (draining in-flight requests), stops the node and
// closes the storage.
package main

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/daishakpatel/flowforge/internal/config"
	pb "github.com/daishakpatel/flowforge/internal/gen/proto/coordination/v1"
	"github.com/daishakpatel/flowforge/internal/logging"
	"github.com/daishakpatel/flowforge/internal/obs"
	"github.com/daishakpatel/flowforge/internal/raftkv/fsm"
	"github.com/daishakpatel/flowforge/internal/raftkv/node"
	"github.com/daishakpatel/flowforge/internal/raftkv/server"
	"github.com/daishakpatel/flowforge/internal/raftkv/storage"
	"github.com/daishakpatel/flowforge/internal/raftkv/transport"
	"github.com/daishakpatel/flowforge/internal/version"
)

// Shutdown bounds: the leadership handoff and the drain of in-flight client requests.
const (
	transferTimeout = 5 * time.Second
	drainTimeout    = 10 * time.Second
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, config.Options{}, os.Stderr, (&net.ListenConfig{}).Listen)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "flowforge-raft:", err)
		os.Exit(1)
	}
}

type listenFunc func(ctx context.Context, network, addr string) (net.Listener, error)

// run is main without process exit, so tests can drive it with their own environment and
// listeners. It returns nil once ctx is cancelled and the node has shut down.
func run(ctx context.Context, opts config.Options, logOut io.Writer, listen listenFunc) error {
	cfg, err := config.Load[config.Raft](opts)
	if err != nil {
		return err
	}
	logger := logging.New(logging.Config{
		Level: cfg.LogLevel, Format: cfg.LogFormat, Service: "flowforge-raft", Version: version.Version(), Writer: logOut,
	})
	logger.Info("starting", "config", cfg.Redacted())
	peers, err := node.ParsePeers(cfg.Peers)
	if err != nil {
		return err
	}
	ids := make([]uint64, len(peers))
	for i, p := range peers {
		ids[i] = p.ID
	}
	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}
	st, err := storage.OpenBolt(filepath.Join(cfg.DataDir, "raft.db"), storage.Identity{ClusterID: clusterID(ids), NodeID: cfg.NodeID})
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }() // after the node stopped; a close error changes nothing then
	s, err := wire(cfg, peers, clusterID(ids), st, logger)
	if err != nil {
		return err
	}
	return s.serve(ctx, cfg, listen)
}

// clusterID derives the raft cluster id from the voter set, so every node of one static cluster
// agrees on it without another setting: envelopes and data directories of a cluster with other
// voters are refused (04.02, 04.01).
func clusterID(voters []uint64) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte("flowforge-raft")) // hash.Hash writes never fail
	for _, id := range slices.Sorted(slices.Values(voters)) {
		_, _ = h.Write([]byte(strconv.FormatUint(id, 10) + ","))
	}
	return max(h.Sum64(), 1)
}

// stack is one wired node, not yet serving.
type stack struct {
	logger *slog.Logger
	node   *node.Node
	srv    *server.Server
	tr     *transport.GRPC
	reg    *obs.Registry
	// stopping fails /readyz from the start of shutdown, so traffic moves away while it drains.
	stopping atomic.Bool
}

func wire(cfg config.Raft, peers []node.Peer, cluster uint64, st storage.Storage, logger *slog.Logger) (*stack, error) {
	reg := obs.NewRegistry()
	nm, err := node.NewMetrics(reg)
	if err != nil {
		return nil, err
	}
	sm, err := server.NewMetrics(reg)
	if err != nil {
		return nil, err
	}
	f := fsm.New(fsm.Options{})
	n, err := node.New(node.Config{ID: cfg.NodeID, Peers: peers, Logger: logger, Metrics: nm}, st, fsm.NewApplier(f))
	if err != nil {
		return nil, err
	}
	members := make([]*pb.Member, len(peers))
	addrs := map[uint64]string{}
	for i, p := range peers {
		members[i] = &pb.Member{Id: p.ID, PeerAddr: p.Addr}
		if p.ID == cfg.NodeID {
			members[i].ClientAddr = cfg.ClientAddr // other nodes' client addresses aren't configured
		}
		addrs[p.ID] = p.Addr
	}
	srv, err := server.New(server.Config{
		NodeID: cfg.NodeID, Members: members, ProposalTimeout: cfg.ProposalTimeout, Logger: logger, Metrics: sm,
	}, n, f)
	if err != nil {
		return nil, err
	}
	tr, err := transport.New(transport.Config{ID: cfg.NodeID, ClusterID: cluster, Peers: addrs, Logger: logger}, n)
	if err != nil {
		return nil, err
	}
	return &stack{logger: logger, node: n, srv: srv, tr: tr, reg: reg}, nil
}

// keepalive: clients ping every 10 s (client.keepaliveParams); a dead client is dropped after 3 s.
var (
	serverKeepalive = grpc.KeepaliveParams(keepalive.ServerParameters{Time: 10 * time.Second, Timeout: 3 * time.Second})
	keepalivePolicy = grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 5 * time.Second, PermitWithoutStream: true})
)

// serve starts everything, waits for ctx, then shuts down in order (package doc).
func (s *stack) serve(ctx context.Context, cfg config.Raft, listen listenFunc) error {
	peerLis, err := listen(ctx, "tcp", cfg.PeerAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.PeerAddr, err)
	}
	clientLis, err := listen(ctx, "tcp", cfg.ClientAddr)
	if err != nil {
		_ = peerLis.Close()
		return fmt.Errorf("listen on %s: %w", cfg.ClientAddr, err)
	}
	httpLis, err := listen(ctx, "tcp", cfg.HTTPAddr)
	if err != nil {
		_, _ = peerLis.Close(), clientLis.Close()
		return fmt.Errorf("listen on %s: %w", cfg.HTTPAddr, err)
	}
	peerSrv := grpc.NewServer(serverKeepalive, keepalivePolicy)
	s.tr.Register(peerSrv)
	clientSrv := grpc.NewServer(grpc.ChainUnaryInterceptor(s.srv.UnaryInterceptor()), serverKeepalive, keepalivePolicy)
	s.srv.Register(clientSrv)
	httpSrv := obs.NewServer("", s.reg, obs.ReadinessCheck{Name: "raft", Check: func(context.Context) error {
		if s.stopping.Load() {
			return errors.New("shutting down")
		}
		if s.node.Status().Leader == 0 {
			return errors.New("no raft leader")
		}
		return nil
	}})

	var wg sync.WaitGroup
	errc := make(chan error, 3)
	wg.Go(func() { errc <- serveErr("peer grpc", peerSrv.Serve(peerLis)) })
	wg.Go(func() { errc <- serveErr("client grpc", clientSrv.Serve(clientLis)) })
	wg.Go(func() { errc <- serveErr("http", httpSrv.Serve(httpLis)) })
	if err := s.node.Start(s.tr); err != nil {
		peerSrv.Stop()
		clientSrv.Stop()
		_ = httpSrv.Close()
		wg.Wait()
		s.tr.Close()
		return err
	}
	sweepCtx, stopSweep := context.WithCancel(context.WithoutCancel(ctx))
	wg.Go(func() { s.srv.RunSweeper(sweepCtx) })
	s.logger.Info("serving", "client_addr", cfg.ClientAddr, "peer_addr", cfg.PeerAddr, "http_addr", cfg.HTTPAddr)

	var runErr error
	select {
	case <-ctx.Done():
	case <-s.node.Done():
		runErr = fmt.Errorf("raft node stopped: %w", s.node.Err())
	case runErr = <-errc:
	}
	s.shutdown(context.WithoutCancel(ctx), clientSrv)
	stopSweep()
	s.node.Stop()
	s.tr.Close()
	peerSrv.Stop()
	hctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), drainTimeout)
	defer cancel()
	_ = httpSrv.Shutdown(hctx) // the operational server holds nothing worth waiting for
	wg.Wait()
	s.logger.Info("stopped", logging.Err(runErr))
	return runErr
}

// shutdown hands leadership off (when leader) while still serving, so requests in the window
// are answered NOT_LEADER with the new leader's id, then drains the client server.
func (s *stack) shutdown(ctx context.Context, clientSrv *grpc.Server) {
	s.stopping.Store(true)
	if s.node.Status().IsLeader {
		tctx, cancel := context.WithTimeout(ctx, transferTimeout)
		err := s.node.TransferLeadership(tctx, 0)
		cancel()
		if err != nil {
			s.logger.Warn("leadership transfer failed; the cluster will elect a new leader", logging.Err(err))
		} else {
			s.logger.Info("leadership transferred", "leader", s.node.Status().Leader)
		}
	}
	drained := make(chan struct{})
	go func() {
		clientSrv.GracefulStop()
		close(drained)
	}()
	select {
	case <-drained:
	case <-time.After(drainTimeout):
		clientSrv.Stop()
		<-drained
	}
}

func serveErr(name string, err error) error {
	if err == nil || errors.Is(err, grpc.ErrServerStopped) || errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return fmt.Errorf("%s server: %w", name, err)
}
