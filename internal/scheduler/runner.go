package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/logging"
	"github.com/daishakpatel/flowforge/internal/persistence"
)

// Loop is one leader-only background job: the scheduler loop here, retries and recovery (03.04),
// the outbox relay (03.07), cron (03.08).
//
// Run works until ctx is done and then returns nil. ctx is cancelled when the leadership term
// ends, so a loop needs no Gate polling of its own to stop; it gets the Gate for Epoch, which
// its transactions assert. Run returns an error only when the loop must not continue (for
// example a stale epoch); transient failures are logged and retried on the next tick.
type Loop interface {
	Name() string
	Run(ctx context.Context, gate Gate) error
}

// Runner runs every registered Loop while the Gate says leader, cancels them as soon as the
// term ends, and starts them again on the next term.
type Runner struct {
	gate  Gate
	loops []Loop
}

// NewRunner returns a Runner with no loops, gated by g.
func NewRunner(g Gate) *Runner { return &Runner{gate: g} }

// RegisterLoop adds l; it is the extension point later tasks use (03.04, 03.07, 03.08). Call it
// before Run.
func (r *Runner) RegisterLoop(l Loop) { r.loops = append(r.loops, l) }

// Run returns nil when ctx ends, after every loop has returned. If a loop fails, Run stops the
// others and returns that error: the process exits and restarts rather than keep scheduling in
// a state it cannot trust (00C §10.1 fail safe).
func (r *Runner) Run(ctx context.Context) error {
	logger := logging.FromContext(ctx)
	for {
		if err := r.gate.Wait(ctx); err != nil {
			return nil
		}
		done, epoch := r.gate.Done(), r.gate.Epoch() // Done first: if the term ends now, done is closed
		logger.InfoContext(ctx, "leadership acquired; starting loops", "epoch", epoch, "loops", len(r.loops))
		err := r.term(ctx, done, logger)
		if err != nil || ctx.Err() != nil {
			return err
		}
		logger.WarnContext(ctx, "leadership lost; loops stopped", "epoch", epoch)
	}
}

// term runs all loops until done closes, ctx ends or a loop fails, and returns once every loop
// has returned.
func (r *Runner) term(ctx context.Context, done <-chan struct{}, logger *slog.Logger) error {
	tctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errc := make(chan error, len(r.loops))
	var wg sync.WaitGroup
	for _, l := range r.loops {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := runLoop(tctx, l, r.gate); err != nil {
				errc <- err
			}
		}()
	}
	all := make(chan struct{})
	go func() { wg.Wait(); close(all) }()

	var err error
	select {
	case <-done:
	case <-ctx.Done():
	case err = <-errc:
		logger.ErrorContext(ctx, "scheduler loop failed; stopping all loops", logging.Err(err))
	case <-all: // every loop returned nil while still leader; nothing left to run
		select {
		case <-done:
		case <-ctx.Done():
		}
	}
	cancel()
	<-all
	return err
}

// runLoop runs l, turning a panic into an error so one bad loop stops the term cleanly.
func runLoop(ctx context.Context, l Loop, g Gate) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("loop %s panicked: %v\n%s", l.Name(), p, debug.Stack())
		}
	}()
	if err := l.Run(ctx, g); err != nil {
		return fmt.Errorf("loop %s: %w", l.Name(), err)
	}
	return nil
}

// maxLoopBackoff caps how long a loop waits after repeated failures (03.04 degraded behavior).
const maxLoopBackoff = 30 * time.Second

// ErrorBackoff is how long a loop waits after its failures-th consecutive failed iteration: the
// tick doubled per failure, capped at 30 s. A database outage therefore costs a few queries a
// minute, and the loop resumes within 30 s of recovery.
func ErrorBackoff(failures int, tick time.Duration) time.Duration {
	d := tick << min(failures, 10)
	if d <= 0 || d > maxLoopBackoff {
		return maxLoopBackoff
	}
	return d
}

// RunTicking runs once every tick, or sooner when wake fires, until ctx ends (returning nil).
// An error wrapping persistence.ErrStaleEpoch is returned: this scheduler must stop deciding.
// Any other error is logged, counted in flowforge_loop_errors_total{loop=name} and followed by
// ErrorBackoff instead of the tick; the loop never gives up on a transient failure such as a
// database outage. once must make every decision inside a transaction (03.04).
func RunTicking(ctx context.Context, name string, c clock.Clock, tick time.Duration, wake Waker,
	errs *prometheus.CounterVec, once func(context.Context) error) error {
	failures := 0
	for {
		var woken <-chan struct{}
		if wake != nil && failures == 0 {
			woken = wake.Wait() // before the iteration, so a notification during it is not missed
		}
		err := once(ctx)
		switch {
		case errors.Is(err, persistence.ErrStaleEpoch):
			logging.FromContext(ctx).ErrorContext(ctx, "scheduler epoch is stale; stopping",
				"loop", name, "reason", "STALE_EPOCH", logging.Err(err))
			return err
		case err != nil && ctx.Err() == nil:
			failures++
			if errs != nil {
				errs.WithLabelValues(name).Inc()
			}
			logging.FromContext(ctx).WarnContext(ctx, "loop iteration failed; backing off",
				"loop", name, "failures", failures, logging.Err(err))
		default:
			failures = 0
		}
		wait := tick
		if failures > 0 {
			wait = ErrorBackoff(failures, tick)
		}
		t := c.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil
		case <-t.C():
		case <-woken:
			t.Stop()
		}
	}
}
