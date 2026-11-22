package coordination_test

import (
	"context"

	promtest "github.com/prometheus/client_golang/prometheus/testutil"

	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/daishakpatel/flowforge/internal/obs"

	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/coordination"
	"github.com/daishakpatel/flowforge/internal/coordination/fake"
	"github.com/daishakpatel/flowforge/internal/scheduler"
	"github.com/daishakpatel/flowforge/internal/testutil"
)

// The acceptance criterion's compile-time assertion: Gate is the scheduler's leadership gate.
var _ scheduler.Gate = (*coordination.Gate)(nil)

func TestMain(m *testing.M) { testutil.LeakCheck(m) }

// Timing of the fake-clock tests: renew every 1 s, a 3 s lease, local expiry 1 s before it.
const (
	ttl    = 3 * time.Second
	renew  = time.Second
	margin = time.Second
	step   = 100 * time.Millisecond
)

// recordingFence records activations and fails the first failures of them.
type recordingFence struct {
	mu       sync.Mutex
	failures int
	calls    []int64
}

func (f *recordingFence) Activate(_ context.Context, epoch int64, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, epoch)
	if f.failures > 0 {
		f.failures--
		return errors.New("postgres unavailable")
	}
	return nil
}

func (f *recordingFence) activations() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int64(nil), f.calls...)
}

// world is a fake cluster on a fake clock. Every elector waits on exactly one clock timer while
// at rest, so settle can wait until all of them have reacted to the last Advance.
type world struct {
	t        *testing.T
	clk      *clock.Fake
	store    *fake.Store
	electors map[string]*coordination.Elector
	stops    map[string]func()
	metrics  map[string]*coordination.ElectorMetrics // optional, per elector
}

func newWorld(t *testing.T) *world {
	clk := clock.NewFake(time.UnixMilli(1_000_000))
	return &world{t: t, clk: clk, store: fake.NewStore(clk), electors: map[string]*coordination.Elector{}, stops: map[string]func(){}}
}

// start runs an elector named name until the test ends (or stop is called).
func (w *world) start(name string, fence coordination.EpochFence, seed uint64) *coordination.Elector {
	w.t.Helper()
	e, err := coordination.NewElector(coordination.ElectorConfig{
		NodeID: name, TTL: ttl, Renew: renew, SafetyMargin: margin, Clock: w.clk, Metrics: w.metrics[name],
		Rand: rand.New(rand.NewPCG(seed, 1)), Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), //nolint:gosec // G404: seeded for reproducible tests
	}, w.store.Client(name), fence)
	if err != nil {
		w.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = e.Run(ctx)
	}()
	stopped := false
	w.stops[name] = func() {
		if !stopped {
			stopped = true
			cancel()
			<-done
		}
	}
	w.t.Cleanup(w.stops[name])
	w.electors[name] = e
	w.settle()
	return e
}

// stop ends an elector's Run and forgets it.
func (w *world) stop(name string) {
	w.stops[name]()
	delete(w.electors, name)
}

func (w *world) settle() { w.clk.BlockUntilWaiters(len(w.electors)) }

// advance moves time by d in steps, checking after each that at most one node leads.
func (w *world) advance(d time.Duration) {
	w.t.Helper()
	for passed := time.Duration(0); passed < d; passed += step {
		w.clk.Advance(step)
		w.settle()
		w.oneLeaderAtMost()
	}
}

func (w *world) oneLeaderAtMost() {
	w.t.Helper()
	var leaders []string
	for n, e := range w.electors {
		if e.Gate().IsLeader() {
			leaders = append(leaders, n)
		}
	}
	if len(leaders) > 1 {
		w.t.Fatalf("two leaders at %s: %v", w.clk.Now().Format(time.StampMilli), leaders)
	}
}

func (w *world) leader() string {
	for n, e := range w.electors {
		if e.Gate().IsLeader() {
			return n
		}
	}
	return ""
}

func closed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestElector_SingleCandidateLeads(t *testing.T) {
	w := newWorld(t)
	fence := &recordingFence{}
	a := w.start("a", fence, 1)
	g := a.Gate()
	if !g.IsLeader() || g.Epoch() != 1 || closed(g.Done()) {
		t.Fatalf("leader=%v epoch=%d done=%v", g.IsLeader(), g.Epoch(), closed(g.Done()))
	}
	if got := fence.activations(); len(got) != 1 || got[0] != 1 {
		t.Fatalf("fence activations %v, want [1]", got)
	}
	info, err := a.Current(context.Background())
	if err != nil || info.NodeID != "a" || info.Epoch != 1 || info.SinceMs != 1_000_000 {
		t.Fatalf("Current = %+v, %v", info, err)
	}
	if err := g.Wait(context.Background()); err != nil {
		t.Fatalf("Wait while leader: %v", err)
	}
	if f, ok := coordination.FenceFromLeader(g); !ok || f.Epoch != 1 {
		t.Fatalf("FenceFromLeader = %+v, %v", f, ok)
	}
	w.advance(10 * ttl) // renewals keep it leading
	if !g.IsLeader() || g.Epoch() != 1 {
		t.Fatal("lost leadership while renewing")
	}
}

func TestElector_SecondCandidateLosesTheCAS(t *testing.T) {
	w := newWorld(t)
	w.start("a", &recordingFence{}, 1)
	b := w.start("b", &recordingFence{}, 2)
	w.advance(5 * ttl)
	if b.Gate().IsLeader() || !closed(b.Gate().Done()) || w.leader() != "a" {
		t.Fatalf("leader %q; b leads: %v", w.leader(), b.Gate().IsLeader())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := b.Gate().Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("follower Wait = %v", err)
	}
}

// TestElector_StepsDownBeforeTheServerExpires: a leader that can't renew is not the leader any more
// while its lease still exists on the server (the margin), and a follower takes over only after the
// server-side expiry, with a strictly higher epoch.
func TestElector_StepsDownBeforeTheServerExpires(t *testing.T) {
	w := newWorld(t)
	a := w.start("a", &recordingFence{}, 1)
	b := w.start("b", &recordingFence{}, 2)
	observer := w.store.Client("observer")
	w.advance(5 * renew) // a few renewals: the deadline now comes from a keep-alive, not the grant
	w.store.Partition("a", true)
	var lostAt time.Time
	for range int(3 * ttl / step) {
		w.clk.Advance(step)
		w.settle()
		if !a.Gate().IsLeader() && lostAt.IsZero() {
			lostAt = w.clk.Now()
			if _, err := observer.Get(context.Background(), coordination.LeaderKey); err != nil {
				t.Fatalf("a stepped down after the server expired its lease: %v", err)
			}
			if !closed(a.Gate().Done()) {
				t.Fatal("Done not closed when leadership was lost")
			}
		}
		if b.Gate().IsLeader() {
			break
		}
	}
	if lostAt.IsZero() || !b.Gate().IsLeader() {
		t.Fatalf("a lost at %v; b leads: %v", lostAt, b.Gate().IsLeader())
	}
	if b.Gate().Epoch() <= a.Gate().Epoch() {
		t.Fatalf("new epoch %d not above the old %d", b.Gate().Epoch(), a.Gate().Epoch())
	}
	t.Logf("a stepped down at %s; b took over at %s", lostAt.Format(time.StampMilli), w.clk.Now().Format(time.StampMilli))
}

// TestElector_RandomizedRounds: 5 electors, 200 leadership changes forced by partitioning the
// leader at a random point of its term. At every step at most one node leads, and each new
// leader's epoch is strictly above every earlier one.
func TestElector_RandomizedRounds(t *testing.T) {
	rounds := 200
	if testing.Short() {
		rounds = 40
	}
	const seed = 405
	rng := rand.New(rand.NewPCG(seed, 0)) //nolint:gosec // G404: seeded for a reproducible test
	t.Logf("seed %d", seed)
	w := newWorld(t)
	for i := range 5 {
		w.start(fmt.Sprintf("n%d", i), &recordingFence{}, uint64(i)+10) //nolint:gosec // G115: small loop index
	}
	var lastEpoch int64
	for round := range rounds {
		lead := w.leader()
		for lead == "" {
			w.advance(step)
			lead = w.leader()
		}
		epoch := w.electors[lead].Gate().Epoch()
		if epoch <= lastEpoch {
			t.Fatalf("round %d: leader %s has epoch %d, not above %d", round, lead, epoch, lastEpoch)
		}
		lastEpoch = epoch
		w.advance(time.Duration(rng.Int64N(int64(2 * ttl))))
		w.store.Partition(lead, true)
		for w.leader() == "" || w.leader() == lead {
			w.advance(step)
		}
		w.store.Partition(lead, false)
	}
}

func TestElector_FenceFailureIsNotAnnouncedAndRetried(t *testing.T) {
	w := newWorld(t)
	fence := &recordingFence{failures: 2}
	a := w.start("a", fence, 1)
	if a.Gate().IsLeader() {
		t.Fatal("announced leadership although the fence failed")
	}
	for range int(10 * renew / step) {
		if a.Gate().IsLeader() {
			break
		}
		if n := len(fence.activations()); n < 3 && a.Gate().IsLeader() {
			t.Fatalf("leader after %d activations, all of them failed", n)
		}
		w.advance(step)
	}
	got := fence.activations()
	if !a.Gate().IsLeader() || len(got) != 3 || a.Gate().Epoch() != got[2] || got[2] <= got[1] {
		t.Fatalf("activations %v, leader %v at epoch %d", got, a.Gate().IsLeader(), a.Gate().Epoch())
	}
}

func TestElector_ShortRenewalFailuresKeepLeadership(t *testing.T) {
	w := newWorld(t)
	a := w.start("a", &recordingFence{}, 1)
	done := a.Gate().Done()
	w.store.Partition("a", true)
	w.advance(renew + 500*time.Millisecond) // < ttl - margin since the last renewal
	w.store.Partition("a", false)
	w.advance(3 * renew)
	if !a.Gate().IsLeader() || closed(done) || a.Gate().Epoch() != 1 {
		t.Fatalf("leader %v, done %v, epoch %d", a.Gate().IsLeader(), closed(done), a.Gate().Epoch())
	}
}

// TestElector_LostLeaseEndsTheTermAtOnce: a lease the server no longer has (revoked here) ends the
// term at the next renewal, before the local deadline.
func TestElector_LostLeaseEndsTheTermAtOnce(t *testing.T) {
	w := newWorld(t)
	a := w.start("a", &recordingFence{}, 1)
	done := a.Gate().Done()
	admin := w.store.Client("admin")
	key, err := admin.Get(context.Background(), coordination.LeaderKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := admin.LeaseRevoke(context.Background(), key.GetLeaseId()); err != nil {
		t.Fatal(err)
	}
	w.advance(renew)
	if !closed(done) {
		t.Fatal("term 1 still running one renewal after its lease was revoked")
	}
}

func TestElector_ShutdownStepsDownAndFreesTheKey(t *testing.T) {
	w := newWorld(t)
	a := w.start("a", &recordingFence{}, 1)
	done := a.Gate().Done()
	w.stop("a")
	if a.Gate().IsLeader() || !closed(done) {
		t.Fatal("still leader after Run returned")
	}
	if _, err := w.store.Client("admin").Get(context.Background(), coordination.LeaderKey); err == nil {
		t.Fatal("leader key still held after shutdown")
	}
	b := w.start("b", &recordingFence{}, 2)
	if !b.Gate().IsLeader() {
		t.Fatal("successor did not take over at once")
	}
}

func TestGate_WaitReturnsOnLeadership(t *testing.T) {
	w := newWorld(t)
	w.start("a", &recordingFence{}, 1)
	b := w.start("b", &recordingFence{}, 2)
	waited := make(chan error, 1)
	go func() { waited <- b.Gate().Wait(context.Background()) }()
	w.stop("a")
	for !b.Gate().IsLeader() {
		w.advance(step)
	}
	if err := <-waited; err != nil {
		t.Fatal(err)
	}
}

func TestNewElector_ValidatesConfig(t *testing.T) {
	kv := fake.NewStore(clock.NewFake(time.Unix(0, 0))).Client("a")
	for _, cfg := range []coordination.ElectorConfig{
		{TTL: ttl, Renew: renew},                        // no node id
		{NodeID: "a", TTL: 0, Renew: renew},             // no ttl
		{NodeID: "a", TTL: ttl, Renew: 3 * time.Second}, // renew ≥ ttl - margin
	} {
		if _, err := coordination.NewElector(cfg, kv, &recordingFence{}); err == nil {
			t.Errorf("NewElector(%+v) succeeded", cfg)
		}
	}
	if _, err := coordination.NewElector(coordination.ElectorConfig{NodeID: "a", TTL: ttl, Renew: renew}, kv, nil); err == nil {
		t.Error("NewElector without a fence succeeded")
	}
}

func TestElector_Metrics(t *testing.T) {
	w := newWorld(t)
	w.metrics = map[string]*coordination.ElectorMetrics{}
	for _, n := range []string{"a", "b"} {
		m, err := coordination.NewElectorMetrics(obs.NewRegistry())
		if err != nil {
			t.Fatal(err)
		}
		w.metrics[n] = m
	}
	w.start("a", &recordingFence{}, 1)
	w.start("b", &recordingFence{}, 2)
	a, b := w.metrics["a"], w.metrics["b"]
	if promtest.ToFloat64(a.IsLeader) != 1 || promtest.ToFloat64(a.Epoch) != 1 || promtest.ToFloat64(a.LeaderChanges) != 1 {
		t.Fatal("leader metrics not set when a became leader")
	}
	w.store.Partition("a", true)
	for promtest.ToFloat64(b.IsLeader) != 1 {
		w.advance(step)
	}
	if promtest.ToFloat64(a.IsLeader) != 0 || promtest.ToFloat64(a.LeaderChanges) != 2 || promtest.ToFloat64(b.Epoch) != 2 {
		t.Fatalf("after the handoff: a leader %v changes %v; b epoch %v", promtest.ToFloat64(a.IsLeader),
			promtest.ToFloat64(a.LeaderChanges), promtest.ToFloat64(b.Epoch))
	}
}

// stallingKV blocks keep-alives while stalled, as a hung connection or a GC pause would.
type stallingKV struct {
	coordination.KV
	mu      sync.Mutex
	stalled chan struct{} // non-nil: keep-alives wait for it to close
}

func (k *stallingKV) stall() {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.stalled = make(chan struct{})
}

func (k *stallingKV) release() {
	k.mu.Lock()
	defer k.mu.Unlock()
	close(k.stalled)
	k.stalled = nil
}

func (k *stallingKV) LeaseKeepAlive(ctx context.Context, id, token uint64) error {
	k.mu.Lock()
	ch := k.stalled
	k.mu.Unlock()
	if ch != nil {
		<-ch
		return errors.New("connection reset")
	}
	return k.KV.LeaseKeepAlive(ctx, id, token)
}

// TestGate_DeadlineHoldsWhileTheElectorIsStuck: IsLeader turns false at the local deadline even
// when the elector's goroutine is stuck in a keep-alive and can't end the term itself.
func TestGate_DeadlineHoldsWhileTheElectorIsStuck(t *testing.T) {
	clk := clock.NewFake(time.UnixMilli(1_000_000))
	kv := &stallingKV{KV: fake.NewStore(clk).Client("a")}
	e, err := coordination.NewElector(coordination.ElectorConfig{NodeID: "a", TTL: ttl, Renew: renew, SafetyMargin: margin, Clock: clk}, kv, &recordingFence{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = e.Run(ctx)
	}()
	defer func() {
		cancel()
		kv.release()
		<-done
	}()
	clk.BlockUntilWaiters(1)
	if !e.Gate().IsLeader() {
		t.Fatal("not leading")
	}
	kv.stall()
	clk.Advance(renew) // the elector wakes and blocks in a keep-alive
	clk.Advance(ttl - margin)
	if e.Gate().IsLeader() {
		t.Fatal("still leader past the local deadline while the elector is stuck")
	}
}
