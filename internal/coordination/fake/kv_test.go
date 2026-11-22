package fake_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/coordination"
	"github.com/daishakpatel/flowforge/internal/coordination/fake"
	"github.com/daishakpatel/flowforge/internal/raftkv/client"
)

var _ coordination.KV = (*fake.KV)(nil)

// TestKV_LeaseTiming: the fake judges leases on its clock as the server does (ADR-005): a key on
// an expired lease reads as absent and is absent for CAS, keep-alive sets
// max(expires_at, now + ttl), and a keep-alive after expiry is LEASE_EXPIRED.
func TestKV_LeaseTiming(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewFake(time.UnixMilli(0))
	kv := fake.NewStore(clk).Client("a")
	id, token, err := kv.LeaseGrant(ctx, time.Second, "a")
	if err != nil || id == 0 || token == 0 {
		t.Fatalf("LeaseGrant = %d, %d, %v", id, token, err)
	}
	if ok, _, err := kv.CAS(ctx, "k", 0, []byte("v"), id); !ok || err != nil {
		t.Fatalf("CAS on lease = %v, %v", ok, err)
	}
	clk.Advance(900 * time.Millisecond)
	if err := kv.LeaseKeepAlive(ctx, id, token); err != nil {
		t.Fatal(err)
	}
	clk.Advance(950 * time.Millisecond) // t = 1850 < 900+1000 = 1900
	if _, err := kv.Get(ctx, "k"); err != nil {
		t.Fatalf("Get before expiry: %v", err)
	}
	clk.Advance(50 * time.Millisecond) // t = 1900: expired
	if _, err := kv.Get(ctx, "k"); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("Get after expiry = %v", err)
	}
	if err := kv.LeaseKeepAlive(ctx, id, token); !errors.Is(err, client.ErrLeaseExpired) {
		t.Fatalf("keep-alive after expiry = %v", err)
	}
	if err := kv.LeaseKeepAlive(ctx, 99, 1); !errors.Is(err, client.ErrLeaseNotFound) {
		t.Fatalf("keep-alive of an unknown lease = %v", err)
	}
	if err := kv.LeaseKeepAlive(ctx, id, token+1); !errors.Is(err, client.ErrLeaseNotFound) {
		t.Fatalf("keep-alive with a wrong token = %v", err)
	}
	if _, err := kv.Put(ctx, "x", nil, id); !errors.Is(err, client.ErrLeaseExpired) {
		t.Fatalf("Put on an expired lease = %v", err)
	}
	// Touching the expired key drops its lease, as the server's expire-on-touch does.
	if ok, _, err := kv.CAS(ctx, "k", 0, []byte("w"), 0); !ok || err != nil {
		t.Fatalf("CAS must-not-exist on an expired key = %v, %v", ok, err)
	}
	if err := kv.LeaseKeepAlive(ctx, id, token); !errors.Is(err, client.ErrLeaseNotFound) {
		t.Fatalf("keep-alive of a lease expired on touch = %v", err)
	}
}

func TestKV_CASCountersListRevoke(t *testing.T) {
	ctx := context.Background()
	kv := fake.NewStore(clock.NewFake(time.UnixMilli(0))).Client("a")
	if ok, _, err := kv.CAS(ctx, "k", 0, []byte("1"), 0); !ok || err != nil {
		t.Fatal(ok, err)
	}
	if ok, cur, _ := kv.CAS(ctx, "k", 0, []byte("2"), 0); ok || cur.GetVersion() != 1 {
		t.Fatalf("stale CAS = %v, %v", ok, cur)
	}
	for want := uint64(1); want <= 2; want++ {
		if v, err := kv.IncrCounter(ctx, "n"); v != want || err != nil {
			t.Fatalf("IncrCounter = %d, %v", v, err)
		}
	}
	id, _, _ := kv.LeaseGrant(ctx, time.Hour, "a")
	for _, k := range []string{"w/a", "w/b", "w/c"} {
		if _, err := kv.Put(ctx, k, []byte(k), id); err != nil {
			t.Fatal(err)
		}
	}
	page, next, err := kv.List(ctx, "w/", "")
	if err != nil || len(page) != 3 || next != "" {
		t.Fatalf("List = %v, %q, %v", page, next, err)
	}
	if page, _, _ = kv.List(ctx, "w/", "w/b"); len(page) != 2 {
		t.Fatalf("List from w/b = %v", page)
	}
	if err := kv.LeaseRevoke(ctx, id); err != nil {
		t.Fatal(err)
	}
	if page, _, _ = kv.List(ctx, "w/", ""); len(page) != 0 {
		t.Fatalf("keys left after revoke: %v", page)
	}
	if err := kv.LeaseRevoke(ctx, id); !errors.Is(err, client.ErrLeaseNotFound) {
		t.Fatalf("second revoke = %v", err)
	}
}

func TestKV_PartitionMakesAClientUnavailable(t *testing.T) {
	ctx := context.Background()
	s := fake.NewStore(clock.NewFake(time.UnixMilli(0)))
	a, b := s.Client("a"), s.Client("b")
	s.Partition("a", true)
	if _, err := a.IncrCounter(ctx, "n"); !errors.Is(err, client.ErrUnavailable) {
		t.Fatalf("partitioned client = %v", err)
	}
	if _, err := b.IncrCounter(ctx, "n"); err != nil {
		t.Fatalf("other client = %v", err)
	}
	s.Partition("a", false)
	if v, err := a.IncrCounter(ctx, "n"); v != 2 || err != nil {
		t.Fatalf("healed client = %d, %v", v, err)
	}
}
