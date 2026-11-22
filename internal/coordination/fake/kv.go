// Package fake is an in-memory coordination.KV for tests (task 04.05). One Store is the
// "cluster"; each Client is one process's connection to it, which tests can partition away.
//
// It keeps the Raft KV's semantics that coordination relies on, judged on the Store's clock (a
// fake clock in tests), as the server judges them on the leader's (ADR-005):
//   - expires_at = now + ttl at grant; keep-alive sets max(expires_at, now + ttl) (00E §5.2);
//   - a key bound to an expired lease reads as absent, and writes treat it as absent (04.04's
//     expire on touch); keep-alive of an expired lease is LEASE_EXPIRED, of an unknown one or
//     with a wrong token LEASE_NOT_FOUND;
//   - per-key versions start at 1; CAS with expected 0 means "must not exist";
//   - a partitioned client's calls fail with client.ErrUnavailable and change nothing.
//
// Errors wrap internal/raftkv/client's kinds, so errors.Is works as with the real client.
package fake

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/daishakpatel/flowforge/internal/clock"
	pb "github.com/daishakpatel/flowforge/internal/gen/proto/coordination/v1"
	"github.com/daishakpatel/flowforge/internal/raftkv/client"
)

type entry struct {
	value   []byte
	version uint64
	lease   uint64
}

type lease struct {
	ttl       time.Duration
	expiresAt time.Time
	token     uint64
}

// Store is the shared state. Safe for concurrent use.
type Store struct {
	clk clock.Clock

	mu          sync.Mutex
	keys        map[string]*entry
	leases      map[uint64]*lease
	counters    map[string]uint64
	nextLease   uint64
	fencing     uint64
	partitioned map[string]bool
}

// NewStore returns an empty store judging leases on clk.
func NewStore(clk clock.Clock) *Store {
	return &Store{clk: clk, keys: map[string]*entry{}, leases: map[uint64]*lease{}, counters: map[string]uint64{}, partitioned: map[string]bool{}}
}

// Client returns a connection named name.
func (s *Store) Client(name string) *KV { return &KV{s: s, name: name} }

// Partition cuts (or, with false, restores) every client named name.
func (s *Store) Partition(name string, cut bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.partitioned[name] = cut
}

// KV is one client of a Store; it implements coordination.KV.
type KV struct {
	s    *Store
	name string
}

func errorf(kind error, format string, args ...any) error {
	return fmt.Errorf("fake kv: "+format+": %w", append(args, kind)...)
}

// do runs f under the store's lock unless the client is partitioned.
func (k *KV) do(f func(now time.Time) error) error {
	k.s.mu.Lock()
	defer k.s.mu.Unlock()
	if k.s.partitioned[k.name] {
		return errorf(client.ErrUnavailable, "%s is partitioned", k.name)
	}
	return f(k.s.clk.Now())
}

// live returns key's entry, or nil when absent or on an expired lease.
func (s *Store) live(key string, now time.Time) *entry {
	e := s.keys[key]
	if e == nil || (e.lease != 0 && !now.Before(s.leases[e.lease].expiresAt)) {
		return nil
	}
	return e
}

// touch drops key's expired lease (and its keys) before a write, as 04.04's server does.
func (s *Store) touch(key string, now time.Time) {
	if e := s.keys[key]; e != nil && e.lease != 0 && s.live(key, now) == nil {
		s.drop(e.lease)
	}
}

func (s *Store) drop(id uint64) {
	for k, e := range s.keys {
		if e.lease == id {
			delete(s.keys, k)
		}
	}
	delete(s.leases, id)
}

func kv(key string, e *entry) *pb.KeyValue {
	if e == nil {
		return nil
	}
	return &pb.KeyValue{Key: key, Value: slices.Clone(e.value), Version: e.version, LeaseId: e.lease}
}

func (s *Store) write(key string, value []byte, leaseID uint64, now time.Time) (*pb.KeyValue, error) {
	if leaseID != 0 {
		l := s.leases[leaseID]
		switch {
		case l == nil:
			return nil, errorf(client.ErrLeaseNotFound, "lease %d", leaseID)
		case !now.Before(l.expiresAt):
			return nil, errorf(client.ErrLeaseExpired, "lease %d", leaseID)
		}
	}
	e := s.keys[key]
	if e == nil {
		e = &entry{}
		s.keys[key] = e
	}
	e.value, e.lease = slices.Clone(value), leaseID
	e.version++
	return kv(key, e), nil
}

// Get implements coordination.KV.
func (k *KV) Get(_ context.Context, key string) (*pb.KeyValue, error) {
	var out *pb.KeyValue
	err := k.do(func(now time.Time) error {
		if e := k.s.live(key, now); e != nil {
			out = kv(key, e)
			return nil
		}
		return errorf(client.ErrNotFound, "key %q", key)
	})
	return out, err
}

// List implements coordination.KV: one page holding every live key under prefix from start.
func (k *KV) List(_ context.Context, prefix, start string) ([]*pb.KeyValue, string, error) {
	var out []*pb.KeyValue
	err := k.do(func(now time.Time) error {
		keys := make([]string, 0, len(k.s.keys))
		for key := range k.s.keys {
			if strings.HasPrefix(key, prefix) && key >= start && k.s.live(key, now) != nil {
				keys = append(keys, key)
			}
		}
		slices.Sort(keys)
		for _, key := range keys {
			out = append(out, kv(key, k.s.keys[key]))
		}
		return nil
	})
	return out, "", err
}

// Put implements coordination.KV.
func (k *KV) Put(_ context.Context, key string, value []byte, leaseID uint64) (*pb.KeyValue, error) {
	var out *pb.KeyValue
	err := k.do(func(now time.Time) error {
		k.s.touch(key, now)
		var err error
		out, err = k.s.write(key, value, leaseID, now)
		return err
	})
	return out, err
}

// CAS implements coordination.KV.
func (k *KV) CAS(_ context.Context, key string, expected uint64, value []byte, leaseID uint64) (bool, *pb.KeyValue, error) {
	var ok bool
	var out *pb.KeyValue
	err := k.do(func(now time.Time) error {
		k.s.touch(key, now)
		cur := k.s.live(key, now)
		if (cur == nil && expected != 0) || (cur != nil && cur.version != expected) {
			out = kv(key, cur)
			return nil
		}
		var err error
		out, err = k.s.write(key, value, leaseID, now)
		ok = err == nil
		return err
	})
	return ok, out, err
}

// LeaseGrant implements coordination.KV.
func (k *KV) LeaseGrant(_ context.Context, ttl time.Duration, _ string) (uint64, uint64, error) {
	var id, token uint64
	err := k.do(func(now time.Time) error {
		if ttl <= 0 {
			return errorf(client.ErrInvalidArgument, "ttl %s", ttl)
		}
		k.s.nextLease++
		k.s.fencing++
		id, token = k.s.nextLease, k.s.fencing
		k.s.leases[id] = &lease{ttl: ttl, expiresAt: now.Add(ttl), token: token}
		return nil
	})
	return id, token, err
}

// LeaseKeepAlive implements coordination.KV.
func (k *KV) LeaseKeepAlive(_ context.Context, id, token uint64) error {
	return k.do(func(now time.Time) error {
		l := k.s.leases[id]
		switch {
		case l == nil || l.token != token:
			return errorf(client.ErrLeaseNotFound, "lease %d", id)
		case !now.Before(l.expiresAt):
			return errorf(client.ErrLeaseExpired, "lease %d", id)
		}
		if exp := now.Add(l.ttl); exp.After(l.expiresAt) { // from now, never backwards (00E §5.2)
			l.expiresAt = exp
		}
		return nil
	})
}

// LeaseRevoke implements coordination.KV.
func (k *KV) LeaseRevoke(_ context.Context, id uint64) error {
	return k.do(func(time.Time) error {
		if k.s.leases[id] == nil {
			return errorf(client.ErrLeaseNotFound, "lease %d", id)
		}
		k.s.drop(id)
		return nil
	})
}

// IncrCounter implements coordination.KV.
func (k *KV) IncrCounter(_ context.Context, name string) (uint64, error) {
	var v uint64
	err := k.do(func(time.Time) error {
		k.s.counters[name]++
		v = k.s.counters[name]
		return nil
	})
	return v, err
}
