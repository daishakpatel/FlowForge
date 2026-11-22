// Package client is the typed Go client of the Raft KV (task 04.04, FR-RAFT-04, ADR-006), also
// exported as pkg/client/coordination.
//
// It keeps one gRPC connection per endpoint, remembers the leader, follows NOT_LEADER hints, and
// retries only where a retry is safe (retry.go):
//
//   - NOT_LEADER: the node did not run the request, so it is redirected at once.
//   - UNAVAILABLE (no quorum, a timeout, a dead node): reads are retried, and so are writes,
//     because every write carries this client's (client_id, request_id) and the server
//     deduplicates it: a retry of a write that did apply returns the original result.
//
// Every other error is returned as it is, typed (errors.go): a conflict is never retried.
// After the attempts or the deadline run out, a write's error wraps ErrUnavailable and its
// outcome is unknown: it may have applied (at most once).
package client

import (
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"github.com/daishakpatel/flowforge/internal/clock"
	pb "github.com/daishakpatel/flowforge/internal/gen/proto/coordination/v1"
	"github.com/daishakpatel/flowforge/internal/ids"
)

// Config configures a Client. Zero values take the defaults noted.
type Config struct {
	Endpoints []string // the nodes' client addresses (FF_RAFT_ENDPOINTS); at least one
	// ClientID identifies this client's writes for deduplication; default a new "cli_" ULID.
	// Two live clients must never share one.
	ClientID    string
	MaxAttempts int // per call, default 5
	// BaseBackoff and MaxBackoff (defaults 300ms and 3s) shape the waits between attempts
	// (retry.go); scale them with the cluster's election timeout.
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
	// Timeout is a call's overall deadline when its ctx has none, default 10s.
	Timeout time.Duration
	// DialOptions replace the default (plaintext until TLS is wired in 09.01, with keepalive).
	DialOptions []grpc.DialOption
	Clock       clock.Clock // backoff sleeps; default clock.Real
	Rand        *rand.Rand  // backoff jitter; default seeded from crypto/rand
}

// keepaliveParams detect a dead leader's connection within ~13 s even without requests.
var keepaliveParams = keepalive.ClientParameters{Time: 10 * time.Second, Timeout: 3 * time.Second, PermitWithoutStream: true}

// connectParams reconnect to a restarted node within seconds: gRPC's default backoff grows to
// 120 s, and calls on a connection in backoff fail at once, so a node that came back would look
// down for minutes.
var connectParams = grpc.ConnectParams{
	Backoff:           backoff.Config{BaseDelay: 100 * time.Millisecond, Multiplier: 1.6, Jitter: 0.2, MaxDelay: time.Second},
	MinConnectTimeout: time.Second,
}

// Client is safe for concurrent use. Close it when done.
type Client struct {
	cfg   Config
	conns []*grpc.ClientConn
	kv    []pb.KVClient
	cl    []pb.ClusterClient
	seq   atomic.Uint64
	nonce string // random per Client: request ids never repeat across Clients sharing a ClientID

	mu     sync.Mutex // guards the leader cache
	leader int        // endpoint index of the believed leader, -1 unknown
	next   int        // next endpoint to try when the leader is unknown
	byID   map[uint64]int

	rngMu sync.Mutex
	rng   *rand.Rand
}

// New returns a client for cfg's endpoints. Connections are made lazily.
func New(cfg Config) (*Client, error) {
	if len(cfg.Endpoints) == 0 {
		return nil, errors.New("coordination client: no endpoints")
	}
	if cfg.MaxAttempts < 0 || cfg.BaseBackoff < 0 || cfg.MaxBackoff < 0 || cfg.Timeout < 0 {
		return nil, errors.New("coordination client: negative attempts, backoff or timeout")
	}
	cfg = withDefaults(cfg)
	var nonce [8]byte
	_, _ = crand.Read(nonce[:]) // crypto/rand.Read never fails (Go ≥ 1.24)
	c := &Client{cfg: cfg, leader: -1, byID: map[uint64]int{}, rng: cfg.Rand, nonce: hex.EncodeToString(nonce[:])}
	for _, ep := range cfg.Endpoints {
		conn, err := grpc.NewClient(ep, cfg.DialOptions...)
		if err != nil {
			_ = c.Close() // reporting the dial error instead
			return nil, fmt.Errorf("coordination client: endpoint %s: %w", ep, err)
		}
		c.conns = append(c.conns, conn)
		c.kv = append(c.kv, pb.NewKVClient(conn))
		c.cl = append(c.cl, pb.NewClusterClient(conn))
	}
	return c, nil
}

func withDefaults(cfg Config) Config {
	if cfg.ClientID == "" {
		cfg.ClientID = ids.New("cli")
	}
	if cfg.MaxAttempts == 0 {
		cfg.MaxAttempts = 5
	}
	if cfg.BaseBackoff == 0 {
		cfg.BaseBackoff = 300 * time.Millisecond
	}
	if cfg.MaxBackoff == 0 {
		cfg.MaxBackoff = 3 * time.Second
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.DialOptions == nil {
		cfg.DialOptions = []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithKeepaliveParams(keepaliveParams),
			grpc.WithConnectParams(connectParams),
		}
	}
	if cfg.Clock == nil {
		cfg.Clock = clock.Real{}
	}
	if cfg.Rand == nil {
		var seed [16]byte
		_, _ = crand.Read(seed[:])                                                                                   // crypto/rand.Read never fails (Go ≥ 1.24)
		cfg.Rand = rand.New(rand.NewPCG(binary.LittleEndian.Uint64(seed[:8]), binary.LittleEndian.Uint64(seed[8:]))) //nolint:gosec // G404: jitter only
	}
	return cfg
}

// Close closes every connection.
func (c *Client) Close() error {
	var errs []error
	for _, conn := range c.conns {
		errs = append(errs, conn.Close())
	}
	return errors.Join(errs...)
}

// meta identifies one write. It is made once per call, so every retry of the call carries the
// same request id and the server applies it at most once. The request id is this Client's random
// nonce plus a counter, so a restarted process reusing a fixed ClientID never repeats one (the
// server would answer a repeat with the earlier write's result).
func (c *Client) meta() *pb.RequestMeta {
	return &pb.RequestMeta{ClientId: c.cfg.ClientID, RequestId: c.nonce + "-" + strconv.FormatUint(c.seq.Add(1), 10)}
}

// Option adjusts one call. Each option applies to the calls its doc names; others ignore it.
type Option func(*options)

type options struct {
	expected    *uint64
	lease       uint64
	consistency pb.Consistency
	limit       uint32
	start       string
}

// WithExpectedVersion makes Put and Delete apply only at this version (0: the key must not exist).
func WithExpectedVersion(v uint64) Option { return func(o *options) { o.expected = &v } }

// WithLease binds a Put's key to a lease.
func WithLease(id uint64) Option { return func(o *options) { o.lease = id } }

// Serializable makes Get and List read the serving node's state, which may be stale, instead of
// a linearizable read on the leader.
func Serializable() Option {
	return func(o *options) { o.consistency = pb.Consistency_CONSISTENCY_SERIALIZABLE }
}

// WithLimit caps a List page (the server's default and maximum is 1000).
func WithLimit(n uint32) Option { return func(o *options) { o.limit = n } }

// WithStartKey continues a List from a previous page's next_key.
func WithStartKey(k string) Option { return func(o *options) { o.start = k } }

// leaderOnly reports whether a read must be served by the leader (only those teach the client
// which endpoint leads).
func (o options) leaderOnly() bool { return o.consistency != pb.Consistency_CONSISTENCY_SERIALIZABLE }

func collect(opts []Option) options {
	var o options
	for _, f := range opts {
		f(&o)
	}
	return o
}

// Get reads key. ErrNotFound when it is absent or bound to an expired lease.
func (c *Client) Get(ctx context.Context, key string, opts ...Option) (*pb.GetResponse, error) {
	o := collect(opts)
	req := &pb.GetRequest{Key: key, Consistency: o.consistency}
	return call(ctx, c, o.leaderOnly(), func(ctx context.Context, i int) (*pb.GetResponse, error) { return c.kv[i].Get(ctx, req) })
}

// List returns one page of the keys under prefix, in key order (WithLimit, WithStartKey).
func (c *Client) List(ctx context.Context, prefix string, opts ...Option) (*pb.ListResponse, error) {
	o := collect(opts)
	req := &pb.ListRequest{Prefix: prefix, Limit: o.limit, Consistency: o.consistency, StartKey: o.start}
	return call(ctx, c, o.leaderOnly(), func(ctx context.Context, i int) (*pb.ListResponse, error) { return c.kv[i].List(ctx, req) })
}

// Put writes key (WithExpectedVersion, WithLease) and returns it as written.
func (c *Client) Put(ctx context.Context, key string, value []byte, opts ...Option) (*pb.KeyValue, error) {
	o := collect(opts)
	req := &pb.PutRequest{Meta: c.meta(), Key: key, Value: value, ExpectedVersion: o.expected, LeaseId: o.lease}
	res, err := call(ctx, c, true, func(ctx context.Context, i int) (*pb.PutResponse, error) { return c.kv[i].Put(ctx, req) })
	return res.GetKv(), err
}

// Delete removes key (WithExpectedVersion) and reports whether it existed.
func (c *Client) Delete(ctx context.Context, key string, opts ...Option) (bool, error) {
	o := collect(opts)
	req := &pb.DeleteRequest{Meta: c.meta(), Key: key, ExpectedVersion: o.expected}
	res, err := call(ctx, c, true, func(ctx context.Context, i int) (*pb.DeleteResponse, error) { return c.kv[i].Delete(ctx, req) })
	return res.GetDeleted(), err
}

// CAS writes value only if key's version is expected (0: must not exist). A mismatch is not an
// error: Succeeded is false and Kv holds the current key (unset when absent).
func (c *Client) CAS(ctx context.Context, key string, expected uint64, value []byte, opts ...Option) (*pb.CASResponse, error) {
	o := collect(opts)
	req := &pb.CASRequest{Meta: c.meta(), Key: key, ExpectedVersion: expected, Action: &pb.CASRequest_Value{Value: value}, LeaseId: o.lease}
	return call(ctx, c, true, func(ctx context.Context, i int) (*pb.CASResponse, error) { return c.kv[i].CAS(ctx, req) })
}

// CASDelete deletes key only if its version is expected (> 0).
func (c *Client) CASDelete(ctx context.Context, key string, expected uint64) (*pb.CASResponse, error) {
	req := &pb.CASRequest{Meta: c.meta(), Key: key, ExpectedVersion: expected, Action: &pb.CASRequest_Delete{Delete: &pb.CASDelete{}}}
	return call(ctx, c, true, func(ctx context.Context, i int) (*pb.CASResponse, error) { return c.kv[i].CAS(ctx, req) })
}

// Txn runs success if every compare holds, else failure (≤ 8 compares, ≤ 8 ops in all).
func (c *Client) Txn(ctx context.Context, compare []*pb.Compare, success, failure []*pb.Op) (*pb.TxnResponse, error) {
	req := &pb.TxnRequest{Meta: c.meta(), Compare: compare, Success: success, Failure: failure}
	return call(ctx, c, true, func(ctx context.Context, i int) (*pb.TxnResponse, error) { return c.kv[i].Txn(ctx, req) })
}

// LeaseGrant creates a lease of ttl (1 ms to 24 h, whole milliseconds) for owner.
func (c *Client) LeaseGrant(ctx context.Context, ttl time.Duration, owner string) (*pb.LeaseGrantResponse, error) {
	req := &pb.LeaseGrantRequest{Meta: c.meta(), TtlMs: uint64(max(ttl.Milliseconds(), 0)), Owner: owner} //nolint:gosec // clamped ≥ 0
	return call(ctx, c, true, func(ctx context.Context, i int) (*pb.LeaseGrantResponse, error) { return c.kv[i].LeaseGrant(ctx, req) })
}

// LeaseKeepAlive extends a live lease. ErrLeaseExpired once the leader's clock says it expired
// (re-grant it); ErrLeaseNotFound when it is gone or the token is wrong.
func (c *Client) LeaseKeepAlive(ctx context.Context, id, fencingToken uint64) (*pb.LeaseKeepAliveResponse, error) {
	req := &pb.LeaseKeepAliveRequest{Meta: c.meta(), LeaseId: id, FencingToken: fencingToken}
	return call(ctx, c, true, func(ctx context.Context, i int) (*pb.LeaseKeepAliveResponse, error) {
		return c.kv[i].LeaseKeepAlive(ctx, req)
	})
}

// LeaseRevoke ends a lease and deletes its keys.
func (c *Client) LeaseRevoke(ctx context.Context, id uint64) error {
	req := &pb.LeaseRevokeRequest{Meta: c.meta(), LeaseId: id}
	_, err := call(ctx, c, true, func(ctx context.Context, i int) (*pb.LeaseRevokeResponse, error) {
		return c.kv[i].LeaseRevoke(ctx, req)
	})
	return err
}

// IncrCounter increments a named counter and returns its new value.
func (c *Client) IncrCounter(ctx context.Context, name string) (uint64, error) {
	req := &pb.IncrCounterRequest{Meta: c.meta(), Name: name}
	res, err := call(ctx, c, true, func(ctx context.Context, i int) (*pb.IncrCounterResponse, error) {
		return c.kv[i].IncrCounter(ctx, req)
	})
	return res.GetValue(), err
}

// Status returns the cluster status as the node the client believes is the leader reports it.
func (c *Client) Status(ctx context.Context) (*pb.StatusResponse, error) {
	return call(ctx, c, false, func(ctx context.Context, i int) (*pb.StatusResponse, error) {
		return c.cl[i].Status(ctx, &pb.StatusRequest{})
	})
}
