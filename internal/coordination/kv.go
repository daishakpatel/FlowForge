// Package coordination builds FlowForge's coordination primitives on the Raft KV client (task
// 04.05, FR-RAFT-06, ADR-003, ADR-005): scheduler leader election with fencing epochs (Elector,
// Gate), worker liveness leases (WorkerRegistry) and fencing helpers (Fence). It is client-side
// only. Leadership here only says who should act; Postgres's epoch assertion is what guarantees
// a deposed leader cannot commit (ADR-003). See docs/architecture/coordination.md.
package coordination

import (
	"context"
	"time"

	pb "github.com/daishakpatel/flowforge/internal/gen/proto/coordination/v1"
	"github.com/daishakpatel/flowforge/internal/raftkv/client"
)

// KV is the part of the Raft KV the library uses, with explicit parameters so a fake can
// implement it (fake.KV). Errors follow internal/raftkv/client: match them with errors.Is against
// client.ErrNotFound, client.ErrLeaseExpired, client.ErrLeaseNotFound, client.ErrConflict and
// client.ErrUnavailable. Reads are linearizable.
type KV interface {
	// Get returns key, or client.ErrNotFound when it is absent or bound to an expired lease.
	Get(ctx context.Context, key string) (*pb.KeyValue, error)
	// List returns one page of the keys under prefix from start, and the next page's start ("": no more).
	List(ctx context.Context, prefix, start string) ([]*pb.KeyValue, string, error)
	// Put writes key, bound to leaseID when it is non-zero.
	Put(ctx context.Context, key string, value []byte, leaseID uint64) (*pb.KeyValue, error)
	// CAS writes key only at version expected (0: must not exist). On a mismatch it returns false
	// and the current key (nil when absent).
	CAS(ctx context.Context, key string, expected uint64, value []byte, leaseID uint64) (bool, *pb.KeyValue, error)
	// LeaseGrant creates a lease and returns its id and fencing token.
	LeaseGrant(ctx context.Context, ttl time.Duration, owner string) (id, token uint64, err error)
	// LeaseKeepAlive extends a lease from the server's clock.
	LeaseKeepAlive(ctx context.Context, id, token uint64) error
	// LeaseRevoke ends a lease and deletes its keys.
	LeaseRevoke(ctx context.Context, id uint64) error
	// IncrCounter increments a named counter and returns its new value.
	IncrCounter(ctx context.Context, name string) (uint64, error)
}

// NewKV adapts the Raft KV client to KV.
func NewKV(c *client.Client) KV { return clientKV{c: c} }

type clientKV struct{ c *client.Client }

func (k clientKV) Get(ctx context.Context, key string) (*pb.KeyValue, error) {
	res, err := k.c.Get(ctx, key)
	return res.GetKv(), err
}

func (k clientKV) List(ctx context.Context, prefix, start string) ([]*pb.KeyValue, string, error) {
	res, err := k.c.List(ctx, prefix, client.WithStartKey(start))
	return res.GetKvs(), res.GetNextKey(), err
}

func (k clientKV) Put(ctx context.Context, key string, value []byte, leaseID uint64) (*pb.KeyValue, error) {
	return k.c.Put(ctx, key, value, client.WithLease(leaseID))
}

func (k clientKV) CAS(ctx context.Context, key string, expected uint64, value []byte, leaseID uint64) (bool, *pb.KeyValue, error) {
	res, err := k.c.CAS(ctx, key, expected, value, client.WithLease(leaseID))
	return res.GetSucceeded(), res.GetKv(), err
}

func (k clientKV) LeaseGrant(ctx context.Context, ttl time.Duration, owner string) (uint64, uint64, error) {
	res, err := k.c.LeaseGrant(ctx, ttl, owner)
	return res.GetLeaseId(), res.GetFencingToken(), err
}

func (k clientKV) LeaseKeepAlive(ctx context.Context, id, token uint64) error {
	_, err := k.c.LeaseKeepAlive(ctx, id, token)
	return err
}

func (k clientKV) LeaseRevoke(ctx context.Context, id uint64) error { return k.c.LeaseRevoke(ctx, id) }

func (k clientKV) IncrCounter(ctx context.Context, name string) (uint64, error) {
	return k.c.IncrCounter(ctx, name)
}
