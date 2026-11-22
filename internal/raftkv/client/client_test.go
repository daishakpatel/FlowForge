package client_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/daishakpatel/flowforge/internal/clock"
	pb "github.com/daishakpatel/flowforge/internal/gen/proto/coordination/v1"
	"github.com/daishakpatel/flowforge/internal/raftkv/client"
	"github.com/daishakpatel/flowforge/internal/raftkv/kvtest"
	"github.com/daishakpatel/flowforge/internal/testutil"
)

func TestMain(m *testing.M) { testutil.LeakCheck(m) }

const leaderWait = 5 * time.Second

// tick and election timeout of kvtest clusters: raft randomizes the timeout in [10, 20) ticks.
const (
	tick               = 10 * time.Millisecond
	maxElectionTimeout = 20 * tick
)

func tctx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// newClient dials every member, with the endpoints ordered so the first is a follower: the
// first request has to follow a NOT_LEADER hint.
func newClient(t *testing.T, c *kvtest.Cluster, lead uint64, mod func(*client.Config)) *client.Client {
	t.Helper()
	leadEP := c.Endpoints()[lead-1]
	eps := slices.DeleteFunc(c.Endpoints(), func(ep string) bool { return ep == leadEP })
	eps = append(eps, leadEP)
	// Backoff scaled to kvtest's 10 ms tick, as the defaults are to the 100 ms production tick:
	// four waits of at least 30+60+120+240 ms outlast a 200 ms election.
	cfg := client.Config{Endpoints: eps, BaseBackoff: 60 * time.Millisecond, MaxBackoff: 600 * time.Millisecond, Rand: rand.New(rand.NewPCG(1, 2))} //nolint:gosec // G404: seeded jitter keeps the test reproducible
	if mod != nil {
		mod(&cfg)
	}
	cl, err := client.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cl.Close() })
	return cl
}

func notLeaderAnswers(c *kvtest.Cluster) float64 {
	var n float64
	for _, id := range c.IDs() {
		if m := c.Member(id); m != nil {
			for _, method := range []string{"Put", "Get", "List", "IncrCounter", "LeaseGrant"} {
				n += promtest.ToFloat64(m.Metrics.Requests.WithLabelValues(method, "FailedPrecondition"))
			}
		}
	}
	return n
}

func TestClient_TypedOperations(t *testing.T) {
	c := kvtest.New(t, kvtest.Options{})
	cl := newClient(t, c, c.WaitLeader(leaderWait), nil)
	ctx := tctx(t)

	kv, err := cl.Put(ctx, "a", []byte("1"))
	if err != nil || kv.GetVersion() != 1 {
		t.Fatalf("Put = %v, %v", kv, err)
	}
	got, err := cl.Get(ctx, "a")
	if err != nil || string(got.GetKv().GetValue()) != "1" || got.GetStale() {
		t.Fatalf("Get = %v, %v", got, err)
	}
	if got, err := cl.Get(ctx, "a", client.Serializable()); err != nil || !got.GetStale() {
		t.Fatalf("serializable Get = %v, %v", got, err)
	}
	if _, err := cl.Put(ctx, "a", []byte("2"), client.WithExpectedVersion(1)); err != nil {
		t.Fatalf("Put at version: %v", err)
	}
	cas, err := cl.CAS(ctx, "a", 2, []byte("3"))
	if err != nil || !cas.GetSucceeded() {
		t.Fatalf("CAS = %v, %v", cas, err)
	}
	if cas, err := cl.CAS(ctx, "a", 2, []byte("x")); err != nil || cas.GetSucceeded() || cas.GetKv().GetVersion() != 3 {
		t.Fatalf("stale CAS = %v, %v", cas, err)
	}
	txn, err := cl.Txn(ctx,
		[]*pb.Compare{{Key: "a", Target: pb.Compare_TARGET_VERSION, Result: pb.Compare_RESULT_EQUAL, Operand: &pb.Compare_Version{Version: 3}}},
		[]*pb.Op{{Op: &pb.Op_Put{Put: &pb.PutOp{Key: "b", Value: []byte("b")}}}}, nil)
	if err != nil || !txn.GetSucceeded() {
		t.Fatalf("Txn = %v, %v", txn, err)
	}
	list, err := cl.List(ctx, "", client.WithLimit(1))
	if err != nil || len(list.GetKvs()) != 1 || !list.GetMore() {
		t.Fatalf("List = %v, %v", list, err)
	}
	if list, err = cl.List(ctx, "", client.WithStartKey(list.GetNextKey())); err != nil || len(list.GetKvs()) != 1 || list.GetKvs()[0].GetKey() != "b" {
		t.Fatalf("List next page = %v, %v", list, err)
	}
	if deleted, err := cl.Delete(ctx, "b"); err != nil || !deleted {
		t.Fatalf("Delete = %v, %v", deleted, err)
	}
	if cas, err := cl.CASDelete(ctx, "a", 3); err != nil || !cas.GetSucceeded() {
		t.Fatalf("CASDelete = %v, %v", cas, err)
	}

	g, err := cl.LeaseGrant(ctx, 30*time.Second, "w1")
	if err != nil || g.GetLeaseId() == 0 {
		t.Fatalf("LeaseGrant = %v, %v", g, err)
	}
	if _, err := cl.Put(ctx, "w/1", []byte("x"), client.WithLease(g.GetLeaseId())); err != nil {
		t.Fatalf("Put on lease: %v", err)
	}
	if _, err := cl.LeaseKeepAlive(ctx, g.GetLeaseId(), g.GetFencingToken()); err != nil {
		t.Fatalf("LeaseKeepAlive: %v", err)
	}
	if err := cl.LeaseRevoke(ctx, g.GetLeaseId()); err != nil {
		t.Fatalf("LeaseRevoke: %v", err)
	}
	if _, err := cl.Get(ctx, "w/1"); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("Get after revoke = %v", err)
	}
	for want := uint64(1); want <= 3; want++ {
		if v, err := cl.IncrCounter(ctx, "n"); err != nil || v != want {
			t.Fatalf("IncrCounter = %d, %v; want %d", v, err, want)
		}
	}
	st, err := cl.Status(ctx)
	if err != nil || st.GetLeaderId() == 0 || st.GetNodeId() != st.GetLeaderId() {
		t.Fatalf("Status = %v, %v", st, err)
	}
	// One redirect at most: the first request learned the leader, the rest went straight to it.
	if n := notLeaderAnswers(c); n != 1 {
		t.Fatalf("NOT_LEADER answers = %v, want 1", n)
	}
}

func TestClient_TypedErrors(t *testing.T) {
	c := kvtest.New(t, kvtest.Options{})
	cl := newClient(t, c, c.WaitLeader(leaderWait), nil)
	ctx := tctx(t)
	if _, err := cl.Get(ctx, "missing"); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("Get missing = %v", err)
	}
	if _, err := cl.Put(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	_, err := cl.Put(ctx, "k", []byte("w"), client.WithExpectedVersion(4))
	var ce *client.Error
	if !errors.Is(err, client.ErrConflict) || !errors.As(err, &ce) || ce.CurrentVersion != 1 {
		t.Fatalf("Put conflict = %v", err)
	}
	if _, err := cl.Put(ctx, "k", []byte("w"), client.WithLease(77)); !errors.Is(err, client.ErrLeaseNotFound) {
		t.Fatalf("Put on unknown lease = %v", err)
	}
	if _, err := cl.Put(ctx, "", []byte("w")); !errors.Is(err, client.ErrInvalidArgument) {
		t.Fatalf("Put empty key = %v", err)
	}
	g, err := cl.LeaseGrant(ctx, time.Millisecond, "w")
	if err != nil {
		t.Fatal(err)
	}
	testutil.WaitFor(t, func() bool {
		_, err = cl.LeaseKeepAlive(ctx, g.GetLeaseId(), g.GetFencingToken())
		return errors.Is(err, client.ErrLeaseExpired) || errors.Is(err, client.ErrLeaseNotFound)
	}, leaderWait)
	if _, err := cl.LeaseGrant(ctx, 0, "w"); !errors.Is(err, client.ErrInvalidArgument) {
		t.Fatalf("LeaseGrant ttl 0 = %v", err)
	}
}

// TestClient_RetriesWritesAcrossFailoverWithoutDuplicates kills the leader in the middle of a
// stream of counter increments. Every increment succeeds through the retry policy, and the
// counter equals the number of increments: a retried write is applied once (FSM dedupe).
func TestClient_RetriesWritesAcrossFailoverWithoutDuplicates(t *testing.T) {
	c := kvtest.New(t, kvtest.Options{})
	lead := c.WaitLeader(leaderWait)
	cl := newClient(t, c, lead, nil)
	ctx := tctx(t)
	const n = 200
	clk := clock.Real{}
	var worst time.Duration
	for i := 1; i <= n; i++ {
		if i == n/2 {
			c.Kill(lead)
		}
		start := clk.Now()
		v, err := cl.IncrCounter(ctx, "n")
		if err != nil {
			t.Fatalf("increment %d: %v", i, err)
		}
		if v != uint64(i) {
			t.Fatalf("increment %d returned %d: a write was applied twice or lost", i, v)
		}
		worst = max(worst, clk.Now().Sub(start))
	}
	// The write that spans the kill waits for an election (≤ maxElectionTimeout after the old
	// leader's last heartbeat) plus the client's backoff.
	t.Logf("worst write latency across the leader kill: %s (max election timeout %s)", worst, maxElectionTimeout)
	if worst > 2*maxElectionTimeout+time.Second {
		t.Fatalf("failover took %s", worst)
	}
}

// TestClient_LinearizableReadAfterWriteAcrossLeaderChanges: a linearizable read after an
// acknowledged write always sees it, through leader kills and restarts.
func TestClient_LinearizableReadAfterWriteAcrossLeaderChanges(t *testing.T) {
	iterations := 10_000
	if testing.Short() {
		iterations = 1_000
	}
	c := kvtest.New(t, kvtest.Options{})
	// More attempts than the default 5: this test is about read-after-write, and on a loaded
	// machine (-race on one CPU, or the whole suite in parallel) an election can outlast five
	// waits, which ends the call with ErrUnavailable as designed (TestClient_GivesUpWhenNoNodeAnswers).
	cl := newClient(t, c, c.WaitLeader(leaderWait), func(cfg *client.Config) { cfg.MaxAttempts = 10 })
	killed := uint64(0)
	changes := 0
	for i := range iterations {
		if i > 0 && i%(iterations/4) == 0 {
			if killed != 0 {
				c.Start(killed)
			}
			killed = c.WaitLeader(leaderWait)
			c.Kill(killed)
			changes++
		}
		// A deadline per iteration: the 10 000 iterations together take longer than one call's
		// budget on a loaded machine (make test-integration runs every package with -race).
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		v := []byte(strconv.Itoa(i))
		if _, err := cl.Put(ctx, "k", v); err != nil {
			cancel()
			t.Fatalf("iteration %d: Put: %v", i, err)
		}
		got, err := cl.Get(ctx, "k")
		cancel()
		if err != nil || string(got.GetKv().GetValue()) != string(v) {
			t.Fatalf("iteration %d: Get = %v, %v; want %s", i, got, err, v)
		}
	}
	t.Logf("%d write/read pairs across %d leader kills", iterations, changes)
}

func TestClient_PermanentErrorsAreNotRetried(t *testing.T) {
	c := kvtest.New(t, kvtest.Options{})
	lead := c.WaitLeader(leaderWait)
	cl := newClient(t, c, lead, nil)
	ctx := tctx(t)
	if _, err := cl.Put(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Put(ctx, "k", []byte("w"), client.WithExpectedVersion(9)); !errors.Is(err, client.ErrConflict) {
		t.Fatal(err)
	}
	if n := promtest.ToFloat64(c.Member(lead).Metrics.Requests.WithLabelValues("Put", "Aborted")); n != 1 {
		t.Fatalf("conflicting Put sent %v times, want 1", n)
	}
}

// TestClient_GivesUpWhenNoNodeAnswers: with every member down the client stops after
// MaxAttempts with ErrUnavailable, well within its deadline.
func TestClient_GivesUpWhenNoNodeAnswers(t *testing.T) {
	c := kvtest.New(t, kvtest.Options{})
	lead := c.WaitLeader(leaderWait)
	cl := newClient(t, c, lead, func(cfg *client.Config) {
		cfg.MaxAttempts, cfg.BaseBackoff, cfg.MaxBackoff = 3, 5*time.Millisecond, 10*time.Millisecond
	})
	for _, id := range c.IDs() {
		c.Kill(id)
	}
	start := clock.Real{}.Now()
	_, err := cl.Put(tctx(t), "k", []byte("v"))
	if !errors.Is(err, client.ErrUnavailable) {
		t.Fatalf("Put = %v, want ErrUnavailable", err)
	}
	if d := (clock.Real{}).Now().Sub(start); d > 5*time.Second {
		t.Fatalf("gave up after %s", d)
	}
}

func TestClient_ConcurrentWritersGetUniqueRequestIDs(t *testing.T) {
	c := kvtest.New(t, kvtest.Options{})
	cl := newClient(t, c, c.WaitLeader(leaderWait), nil)
	ctx := tctx(t)
	var wg sync.WaitGroup
	const writers, each = 8, 25
	for w := range writers {
		wg.Go(func() {
			for range each {
				if _, err := cl.IncrCounter(ctx, "n"); err != nil {
					t.Errorf("writer %d: %v", w, err)
					return
				}
			}
		})
	}
	wg.Wait()
	// Distinct request ids: no increment was mistaken for another's retry.
	if v, err := cl.IncrCounter(ctx, "n"); err != nil || v != writers*each+1 {
		t.Fatalf("counter = %d, %v; want %d", v, err, writers*each+1)
	}
}

func TestNew_ValidatesConfig(t *testing.T) {
	if _, err := client.New(client.Config{}); err == nil {
		t.Fatal("New with no endpoints succeeded")
	}
	if _, err := client.New(client.Config{Endpoints: []string{"a:1"}, MaxAttempts: -1}); err == nil {
		t.Fatal("New with negative attempts succeeded")
	}
	cl, err := client.New(client.Config{Endpoints: []string{"a:1"}, ClientID: "me"})
	if err != nil {
		t.Fatal(err)
	}
	if err := cl.Close(); err != nil {
		t.Fatal(fmt.Errorf("close: %w", err))
	}
}

// TestClient_SharedClientIDNeverReusesRequestIDs: two clients (or one restarted process) with the
// same fixed ClientID must not have their writes deduplicated against each other's (code review).
func TestClient_SharedClientIDNeverReusesRequestIDs(t *testing.T) {
	c := kvtest.New(t, kvtest.Options{})
	lead := c.WaitLeader(leaderWait)
	same := func(cfg *client.Config) { cfg.ClientID = "host-1" }
	ctx := tctx(t)
	if _, err := newClient(t, c, lead, same).Put(ctx, "first", []byte("1")); err != nil {
		t.Fatal(err)
	}
	b := newClient(t, c, lead, same)
	if _, err := b.Put(ctx, "second", []byte("2")); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Get(ctx, "second"); err != nil {
		t.Fatalf("the restarted client's first write was not applied: %v", err)
	}
}

// TestClient_RetryAfterALostResponseAppliesOnce: the first IncrCounter reaches the server and
// applies, but its response is lost; the client retries with the same request id and the server
// returns the original result instead of incrementing again.
func TestClient_RetryAfterALostResponseAppliesOnce(t *testing.T) {
	c := kvtest.New(t, kvtest.Options{})
	lead := c.WaitLeader(leaderWait)
	var lost atomic.Bool
	dropFirst := grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		err := invoker(ctx, method, req, reply, cc, opts...)
		if err == nil && strings.HasSuffix(method, "/IncrCounter") && lost.CompareAndSwap(false, true) {
			return status.Error(codes.Unavailable, "response lost") // applied, but the client never hears
		}
		return err
	})
	cl := newClient(t, c, lead, func(cfg *client.Config) {
		cfg.DialOptions = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()), dropFirst}
	})
	ctx := tctx(t)
	if v, err := cl.IncrCounter(ctx, "n"); err != nil || v != 1 {
		t.Fatalf("IncrCounter after a lost response = %d, %v; want 1 (applied once)", v, err)
	}
	if !lost.Load() {
		t.Fatal("no response was dropped")
	}
	if v, err := cl.IncrCounter(ctx, "n"); err != nil || v != 2 {
		t.Fatalf("next IncrCounter = %d, %v; want 2", v, err)
	}
}

// TestClient_LeaseExpiredIsTyped: a keep-alive the leader's clock says is too late is
// ErrLeaseExpired, not a plain not-found (the holder must re-grant).
func TestClient_LeaseExpiredIsTyped(t *testing.T) {
	clk := clock.NewFake(time.UnixMilli(1_000_000))
	c := kvtest.New(t, kvtest.Options{Size: 1, Clock: clk, SweepInterval: time.Hour})
	cl := newClient(t, c, c.WaitLeader(leaderWait), nil)
	ctx := tctx(t)
	g, err := cl.LeaseGrant(ctx, time.Second, "w")
	if err != nil {
		t.Fatal(err)
	}
	clk.Advance(2 * time.Second)
	_, err = cl.LeaseKeepAlive(ctx, g.GetLeaseId(), g.GetFencingToken())
	var e *client.Error
	if !errors.Is(err, client.ErrLeaseExpired) || errors.Is(err, client.ErrNotFound) || !errors.As(err, &e) || e.ResultCode != "LEASE_EXPIRED" {
		t.Fatalf("keep-alive after expiry = %v, want ErrLeaseExpired", err)
	}
}
