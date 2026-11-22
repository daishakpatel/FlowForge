package coordination

import (
	"context"
	"sync"
	"time"

	"github.com/daishakpatel/flowforge/internal/clock"
)

// Gate is the leadership state an Elector keeps; it implements scheduler.Gate (the compile-time
// assertion is in elector_test.go, so this package doesn't import the scheduler).
//
// Local conservative expiry: IsLeader is false from the term's deadline on, which the Elector
// sets to TTL − SafetyMargin after the start of the last successful grant or keep-alive. The
// server measures the lease from a timestamp it took after that start, so this instance stops
// believing it leads before the server can expire the lease and let another node win. It is
// judged on every call, not when the Elector's goroutine next runs; Done closes when that
// goroutine notices. Safety still never rests on clocks: Postgres asserts the epoch (ADR-003).
// Safe for concurrent use.
type Gate struct {
	clk clock.Clock

	mu       sync.Mutex
	leading  bool
	epoch    int64
	deadline time.Time
	done     chan struct{} // closed when the current term ends; closed while not leading
	elected  chan struct{} // closed when the next term starts (Wait)
}

func newGate(clk clock.Clock) *Gate {
	done := make(chan struct{})
	close(done)
	return &Gate{clk: clk, done: done, elected: make(chan struct{})}
}

// IsLeader reports whether this instance leads now: a term is running and its local deadline
// has not passed.
func (g *Gate) IsLeader() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.leading && g.clk.Now().Before(g.deadline)
}

// Epoch is the fencing epoch of the current term, or of the last one while not leading (0 before
// the first).
func (g *Gate) Epoch() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.epoch
}

// Term returns the current term's epoch and whether this instance leads, read together: a check
// of IsLeader followed by Epoch could straddle a change of term.
func (g *Gate) Term() (int64, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.epoch, g.leading && g.clk.Now().Before(g.deadline)
}

// Done returns a channel closed when the current term ends; while not leading, a closed channel.
// Each term has a new channel.
func (g *Gate) Done() <-chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.done
}

// Wait blocks until this instance leads, or returns ctx.Err().
func (g *Gate) Wait(ctx context.Context) error {
	for {
		g.mu.Lock()
		leading := g.leading && g.clk.Now().Before(g.deadline)
		next := g.elected
		if g.leading && !leading {
			next = g.done // past the deadline, the term ends soon: elected is already closed
		}
		g.mu.Unlock()
		if leading {
			return nil
		}
		select {
		case <-next:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// begin starts a term at epoch, leading until deadline unless extended.
func (g *Gate) begin(epoch int64, deadline time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.leading, g.epoch, g.deadline = true, epoch, deadline
	g.done = make(chan struct{})
	close(g.elected)
}

// extend moves the current term's deadline after a successful keep-alive.
func (g *Gate) extend(deadline time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.leading {
		g.deadline = deadline
	}
}

// end ends the current term, if any, and reports whether one was running.
func (g *Gate) end() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.leading {
		return false
	}
	g.leading = false
	close(g.done)
	g.elected = make(chan struct{})
	return true
}
