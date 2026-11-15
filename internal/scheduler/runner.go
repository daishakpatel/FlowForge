package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"

	"github.com/daishakpatel/flowforge/internal/logging"
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
