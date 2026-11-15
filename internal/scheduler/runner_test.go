package scheduler

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/testutil"
)

// fakeGate is a Gate the test flips by hand. Every gain starts a new term with epoch+1.
type fakeGate struct {
	mu     sync.Mutex
	leader bool
	epoch  int64
	done   chan struct{} // current term; closed while not leader
	gained chan struct{} // closed while leader
}

func newFakeGate() *fakeGate {
	done := make(chan struct{})
	close(done)
	return &fakeGate{done: done, gained: make(chan struct{})}
}

func (g *fakeGate) set(leader bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	switch {
	case leader && !g.leader:
		g.leader, g.epoch, g.done = true, g.epoch+1, make(chan struct{})
		close(g.gained)
	case !leader && g.leader:
		g.leader, g.gained = false, make(chan struct{})
		close(g.done)
	}
}

func (g *fakeGate) IsLeader() bool { g.mu.Lock(); defer g.mu.Unlock(); return g.leader }
func (g *fakeGate) Epoch() int64   { g.mu.Lock(); defer g.mu.Unlock(); return g.epoch }
func (g *fakeGate) Done() <-chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.done
}

func (g *fakeGate) Wait(ctx context.Context) error {
	for {
		g.mu.Lock()
		leader, gained := g.leader, g.gained
		g.mu.Unlock()
		if leader {
			return nil
		}
		select {
		case <-gained:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// tickLoop ticks on a fake clock and records any tick taken after its term ended.
type tickLoop struct {
	clock                      *clock.Fake
	starts, stops, ticks, late atomic.Int64
	lastEpoch                  atomic.Int64
}

func (l *tickLoop) Name() string { return "tick" }

func (l *tickLoop) Run(ctx context.Context, g Gate) error {
	l.starts.Add(1)
	l.lastEpoch.Store(g.Epoch())
	done := g.Done()
	t := l.clock.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			l.stops.Add(1)
			return nil
		case <-t.C():
			select {
			case <-done:
				l.late.Add(1) // a decision after Done closed would be a Gate contract breach
			default:
				l.ticks.Add(1)
			}
		}
	}
}

func TestRunner_FollowsLeadership(t *testing.T) {
	t.Parallel()
	fc := clock.NewFake(time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC))
	g, loop := newFakeGate(), &tickLoop{clock: fc}
	r := NewRunner(g)
	r.RegisterLoop(loop)
	ctx, cancel := context.WithCancel(t.Context())
	res := make(chan error, 1)
	go func() { res <- r.Run(ctx) }()

	if loop.starts.Load() != 0 {
		t.Fatal("loop started without leadership")
	}
	g.set(true)
	fc.BlockUntilWaiters(1) // the loop's ticker
	if got := loop.lastEpoch.Load(); got != 1 {
		t.Fatalf("loop saw epoch %d, want 1", got)
	}
	fc.Advance(time.Second)
	testutil.WaitFor(t, func() bool { return loop.ticks.Load() == 1 }, 5*time.Second)

	// Loss cancels the loop without the clock moving at all, so well within one tick.
	g.set(false)
	testutil.WaitFor(t, func() bool { return loop.stops.Load() == 1 }, 5*time.Second)
	fc.Advance(time.Second)
	if fc.Waiters() != 0 || loop.ticks.Load() != 1 || loop.late.Load() != 0 {
		t.Fatalf("after loss: waiters=%d ticks=%d late=%d", fc.Waiters(), loop.ticks.Load(), loop.late.Load())
	}

	g.set(true)
	fc.BlockUntilWaiters(1)
	if loop.starts.Load() != 2 || loop.lastEpoch.Load() != 2 {
		t.Fatalf("after regain: starts=%d epoch=%d", loop.starts.Load(), loop.lastEpoch.Load())
	}
	cancel()
	if err := <-res; err != nil {
		t.Fatalf("Run = %v, want nil on shutdown", err)
	}
	if loop.stops.Load() != 2 || loop.late.Load() != 0 {
		t.Fatalf("stops=%d late=%d", loop.stops.Load(), loop.late.Load())
	}
}

type failLoop struct{ err error }

func (failLoop) Name() string { return "fail" }
func (l failLoop) Run(context.Context, Gate) error {
	if l.err == nil {
		panic("boom")
	}
	return l.err
}

// blockLoop runs until cancelled and records that it was.
type blockLoop struct{ stopped atomic.Bool }

func (*blockLoop) Name() string { return "block" }
func (l *blockLoop) Run(ctx context.Context, _ Gate) error {
	<-ctx.Done()
	l.stopped.Store(true)
	return nil
}

func TestRunner_LoopFailureStopsAll(t *testing.T) {
	t.Parallel()
	errStale := errors.New("stale")
	for name, tc := range map[string]struct {
		loop Loop
		want string
	}{
		"error": {failLoop{err: errStale}, "loop fail: stale"},
		"panic": {failLoop{}, "loop fail panicked: boom"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			other := &blockLoop{}
			r := NewRunner(AlwaysLeader{})
			r.RegisterLoop(other)
			r.RegisterLoop(tc.loop)
			err := r.Run(t.Context())
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Run = %v, want %q", err, tc.want)
			}
			if name == "error" && !errors.Is(err, errStale) {
				t.Errorf("Run error does not wrap the loop's error: %v", err)
			}
			if !other.stopped.Load() {
				t.Error("the other loop was still running when Run returned")
			}
		})
	}
}

func TestAlwaysLeader(t *testing.T) {
	t.Parallel()
	var g Gate = AlwaysLeader{}
	if !g.IsLeader() || g.Epoch() != 0 || g.Done() != nil || g.Wait(t.Context()) != nil {
		t.Fatal("AlwaysLeader must be leader at epoch 0 with a never-closing Done")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if !errors.Is(g.Wait(ctx), context.Canceled) {
		t.Fatal("Wait must return ctx.Err() once ctx is done")
	}
}
