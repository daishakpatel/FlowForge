package coordination_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	promtest "github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/coordination"
	"github.com/daishakpatel/flowforge/internal/coordination/fake"
	pb "github.com/daishakpatel/flowforge/internal/gen/proto/coordination/v1"
	"github.com/daishakpatel/flowforge/internal/obs"
	"github.com/daishakpatel/flowforge/internal/raftkv/client"
)

const workerTTL = 15 * time.Second

type registryEnv struct {
	clk   *clock.Fake
	store *fake.Store
	reg   *coordination.WorkerRegistry
	m     *coordination.RegistryMetrics
}

func newRegistry(t *testing.T) registryEnv {
	t.Helper()
	clk := clock.NewFake(time.UnixMilli(1_000_000))
	store := fake.NewStore(clk)
	m, err := coordination.NewRegistryMetrics(obs.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	reg, err := coordination.NewWorkerRegistry(store.Client("api"), coordination.RegistryConfig{TTL: workerTTL, Clock: clk, Metrics: m})
	if err != nil {
		t.Fatal(err)
	}
	return registryEnv{clk: clk, store: store, reg: reg, m: m}
}

func worker(id, key string) coordination.WorkerInfo {
	return coordination.WorkerInfo{ID: id, KeyID: key, Capabilities: []string{"shell"}, Queues: []string{"default"}, Capacity: 4, Version: "v1"}
}

func ids(ws []coordination.WorkerInfo) []string {
	var out []string
	for _, w := range ws {
		out = append(out, w.ID)
	}
	return out
}

func TestRegistry_RegisterRenewExpireLifecycle(t *testing.T) {
	env := newRegistry(t)
	ctx := context.Background()
	h, err := env.reg.Register(ctx, worker("w1", "k1"))
	if err != nil || h.WorkerID != "w1" || h.LeaseID == 0 || h.Token == 0 {
		t.Fatalf("Register = %+v, %v", h, err)
	}
	alive, err := env.reg.List(ctx)
	if err != nil || !slices.Equal(ids(alive), []string{"w1"}) || alive[0].RegisteredMs != 1_000_000 || alive[0].Capacity != 4 || alive[0].KeyID != "k1" {
		t.Fatalf("List = %+v, %v", alive, err)
	}
	// Renewals keep it alive well past one TTL.
	for range 3 {
		env.clk.Advance(10 * time.Second)
		if h, err = env.reg.Renew(ctx, h); err != nil {
			t.Fatal(err)
		}
	}
	if alive, _ = env.reg.List(ctx); len(alive) != 1 {
		t.Fatalf("renewed worker gone: %v", alive)
	}
	// Without renewals it expires one TTL after the last renewal (at 30 s): at 45 s.
	env.clk.Advance(workerTTL - time.Millisecond)
	if alive, _ = env.reg.List(ctx); len(alive) != 1 {
		t.Fatalf("worker gone before its expiry: %v", alive)
	}
	env.clk.Advance(time.Millisecond)
	if alive, _ = env.reg.List(ctx); len(alive) != 0 {
		t.Fatalf("expired worker listed: %v", alive)
	}
	if exp, err := env.reg.Expired(ctx, []string{"w1"}); err != nil || !slices.Equal(exp, []string{"w1"}) {
		t.Fatalf("Expired = %v, %v", exp, err)
	}
	// Renewing after expiry re-registers transparently, under a new lease.
	again, err := env.reg.Renew(ctx, h)
	if err != nil || again.LeaseID == h.LeaseID {
		t.Fatalf("Renew after expiry = %+v, %v", again, err)
	}
	if alive, _ = env.reg.List(ctx); !slices.Equal(ids(alive), []string{"w1"}) {
		t.Fatalf("not re-registered: %v", alive)
	}
	if n := promtest.ToFloat64(env.m.RenewalFailures); n != 0 {
		t.Fatalf("re-registration counted as %v renewal failures", n)
	}
}

// TestRegistry_HandleRebuildsFromTheKV: any API instance can renew a worker another instance
// registered, from the key alone.
func TestRegistry_HandleRebuildsFromTheKV(t *testing.T) {
	env := newRegistry(t)
	ctx := context.Background()
	h, err := env.reg.Register(ctx, worker("w1", "k1"))
	if err != nil {
		t.Fatal(err)
	}
	other, err := coordination.NewWorkerRegistry(env.store.Client("api-2"), coordination.RegistryConfig{TTL: workerTTL, Clock: env.clk})
	if err != nil {
		t.Fatal(err)
	}
	got, err := other.Handle(ctx, "w1")
	if err != nil || got.LeaseID != h.LeaseID || got.Token != h.Token || got.Info.KeyID != "k1" {
		t.Fatalf("Handle = %+v, %v; want lease %d token %d", got, err, h.LeaseID, h.Token)
	}
	env.clk.Advance(10 * time.Second)
	if _, err := other.Renew(ctx, got); err != nil {
		t.Fatal(err)
	}
	env.clk.Advance(10 * time.Second) // 20 s after registering: alive only thanks to the renewal
	if alive, _ := env.reg.List(ctx); len(alive) != 1 {
		t.Fatal("renewal through a rebuilt handle did not extend the lease")
	}
	if _, err := other.Handle(ctx, "nobody"); !errors.Is(err, coordination.ErrNotRegistered) {
		t.Fatalf("Handle of an unknown worker = %v", err)
	}
}

func TestRegistry_ConflictAndReRegistration(t *testing.T) {
	env := newRegistry(t)
	ctx := context.Background()
	first, err := env.reg.Register(ctx, worker("w1", "k1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.reg.Register(ctx, worker("w1", "k2")); !errors.Is(err, coordination.ErrWorkerConflict) {
		t.Fatalf("Register by another key = %v", err)
	}
	// The same key re-registering (a restarted worker) takes over under a new lease and frees
	// the old one.
	second, err := env.reg.Register(ctx, worker("w1", "k1"))
	if err != nil || second.LeaseID == first.LeaseID {
		t.Fatalf("re-Register = %+v, %v", second, err)
	}
	if err := env.store.Client("admin").LeaseKeepAlive(ctx, first.LeaseID, first.Token); err == nil {
		t.Fatal("the old lease survived the re-registration")
	}
}

func TestRegistry_DeregisterAndExpired(t *testing.T) {
	env := newRegistry(t)
	ctx := context.Background()
	h1, _ := env.reg.Register(ctx, worker("w1", "k1"))
	if _, err := env.reg.Register(ctx, worker("w2", "k1")); err != nil {
		t.Fatal(err)
	}
	if err := env.reg.Deregister(ctx, h1); err != nil {
		t.Fatal(err)
	}
	if err := env.reg.Deregister(ctx, h1); err != nil {
		t.Fatalf("second Deregister = %v", err)
	}
	if alive, _ := env.reg.List(ctx); !slices.Equal(ids(alive), []string{"w2"}) {
		t.Fatalf("List after Deregister = %v", ids(alive))
	}
	exp, err := env.reg.Expired(ctx, []string{"w3", "w2", "w1", "w3"})
	if err != nil || !slices.Equal(exp, []string{"w1", "w3"}) {
		t.Fatalf("Expired = %v, %v; want [w1 w3]", exp, err)
	}
}

func TestRegistry_RenewalFailureIsCountedAndReturned(t *testing.T) {
	env := newRegistry(t)
	ctx := context.Background()
	h, _ := env.reg.Register(ctx, worker("w1", "k1"))
	env.store.Partition("api", true)
	if _, err := env.reg.Renew(ctx, h); err == nil {
		t.Fatal("Renew through a partition succeeded")
	}
	if n := promtest.ToFloat64(env.m.RenewalFailures); n != 1 {
		t.Fatalf("renewal failures = %v, want 1", n)
	}
}

func TestNewWorkerRegistry_ValidatesConfig(t *testing.T) {
	kv := fake.NewStore(clock.NewFake(time.Unix(0, 0))).Client("a")
	if _, err := coordination.NewWorkerRegistry(kv, coordination.RegistryConfig{}); err == nil {
		t.Fatal("registry without a TTL")
	}
	if _, err := coordination.NewWorkerRegistry(nil, coordination.RegistryConfig{TTL: time.Second}); err == nil {
		t.Fatal("registry without a kv")
	}
}

// flakyKV injects failures: grants fail while failGrants is set; casLost applies a CAS but
// reports an error, as a lost response would.
type flakyKV struct {
	coordination.KV
	failGrants bool
	casLost    bool
}

func (k *flakyKV) LeaseGrant(ctx context.Context, ttl time.Duration, owner string) (uint64, uint64, error) {
	if k.failGrants {
		return 0, 0, client.ErrUnavailable
	}
	return k.KV.LeaseGrant(ctx, ttl, owner)
}

func (k *flakyKV) CAS(ctx context.Context, key string, expected uint64, value []byte, lease uint64) (bool, *pb.KeyValue, error) {
	ok, kv, err := k.KV.CAS(ctx, key, expected, value, lease)
	if k.casLost {
		return false, nil, client.ErrUnavailable
	}
	return ok, kv, err
}

func newFlakyRegistry(t *testing.T) (registryEnv, *flakyKV) {
	t.Helper()
	env := newRegistry(t)
	kv := &flakyKV{KV: env.store.Client("api")}
	reg, err := coordination.NewWorkerRegistry(kv, coordination.RegistryConfig{TTL: workerTTL, Clock: env.clk, Metrics: env.m})
	if err != nil {
		t.Fatal(err)
	}
	env.reg = reg
	return env, kv
}

// A failed re-registration returns the caller's handle unchanged and counts the failure, so the
// next renewal still knows the worker (code review).
func TestRegistry_FailedReRegistrationKeepsTheHandle(t *testing.T) {
	env, kv := newFlakyRegistry(t)
	ctx := context.Background()
	h, err := env.reg.Register(ctx, worker("w1", "k1"))
	if err != nil {
		t.Fatal(err)
	}
	env.clk.Advance(workerTTL)
	kv.failGrants = true
	got, err := env.reg.Renew(ctx, h)
	if err == nil || got.WorkerID != "w1" || got.LeaseID != h.LeaseID || got.Info.KeyID != "k1" {
		t.Fatalf("Renew = %+v, %v; want the original handle and an error", got, err)
	}
	if n := promtest.ToFloat64(env.m.RenewalFailures); n != 1 {
		t.Fatalf("renewal failures = %v", n)
	}
	kv.failGrants = false
	if got, err = env.reg.Renew(ctx, got); err != nil || got.LeaseID == h.LeaseID {
		t.Fatalf("retry = %+v, %v", got, err)
	}
	if alive, _ := env.reg.List(ctx); !slices.Equal(ids(alive), []string{"w1"}) {
		t.Fatalf("List = %v", ids(alive))
	}
}

// A stale handle (another API instance re-registered the worker since) adopts the live lease
// instead of re-registering and revoking it (code review: two instances would ping-pong).
func TestRegistry_StaleHandleAdoptsTheLiveLease(t *testing.T) {
	env := newRegistry(t)
	ctx := context.Background()
	x, err := env.reg.Register(ctx, worker("w1", "k1"))
	if err != nil {
		t.Fatal(err)
	}
	y, err := env.reg.Register(ctx, worker("w1", "k1")) // another instance; x's lease is revoked
	if err != nil {
		t.Fatal(err)
	}
	got, err := env.reg.Renew(ctx, x)
	if err != nil || got.LeaseID != y.LeaseID {
		t.Fatalf("Renew(stale) = %+v, %v; want lease %d", got, err, y.LeaseID)
	}
	if _, err := env.reg.Renew(ctx, y); err != nil {
		t.Fatalf("the live lease was revoked: %v", err)
	}
}

// A CAS whose response is lost but which applied must not be undone by revoking its lease.
func TestRegistry_UnknownCASOutcomeKeepsTheWorker(t *testing.T) {
	env, kv := newFlakyRegistry(t)
	ctx := context.Background()
	kv.casLost = true
	h, err := env.reg.Register(ctx, worker("w1", "k1"))
	kv.casLost = false
	if err != nil || h.LeaseID == 0 {
		t.Fatalf("Register with a lost CAS response = %+v, %v", h, err)
	}
	if alive, _ := env.reg.List(ctx); !slices.Equal(ids(alive), []string{"w1"}) {
		t.Fatalf("worker deleted by the cleanup: %v", ids(alive))
	}
}

// A renewal racing a graceful Deregister must not bring the worker back (code review).
func TestRegistry_RenewAfterDeregisterDoesNotResurrect(t *testing.T) {
	env := newRegistry(t)
	ctx := context.Background()
	h, _ := env.reg.Register(ctx, worker("w1", "k1"))
	if err := env.reg.Deregister(ctx, h); err != nil {
		t.Fatal(err)
	}
	if _, err := env.reg.Renew(ctx, h); !errors.Is(err, coordination.ErrNotRegistered) {
		t.Fatalf("Renew after Deregister = %v, want ErrNotRegistered", err)
	}
	if alive, _ := env.reg.List(ctx); len(alive) != 0 {
		t.Fatalf("deregistered worker came back: %v", ids(alive))
	}
}

func TestRegistry_RejectsBadWorkerIDs(t *testing.T) {
	env := newRegistry(t)
	for _, id := range []string{"", "a/b", strings.Repeat("x", 257)} {
		if _, err := env.reg.Register(context.Background(), worker(id, "k1")); !errors.Is(err, coordination.ErrInvalidWorkerID) {
			t.Errorf("Register(%q) = %v", id, err)
		}
	}
}
