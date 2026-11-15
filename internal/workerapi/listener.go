package workerapi

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/logging"
	"github.com/daishakpatel/flowforge/internal/persistence"
)

// TaskQueuedChannel is the Postgres NOTIFY channel announcing that tasks became QUEUED. The
// scheduler (03.02) sends it in the transaction that queues them, so it arrives at commit.
const TaskQueuedChannel = persistence.TaskQueuedChannel

const reconnectDelay = time.Second

// Waker turns NOTIFY on one channel into a broadcast: ff_task_queued for long-polling PollTask
// calls, ff_scheduler_wake for the scheduler loop. It owns one dedicated connection, held by Run;
// waiters never touch the pool. Safe for concurrent use.
type Waker struct {
	connect func(ctx context.Context) (*pgx.Conn, error)
	clock   clock.Clock
	channel string

	mu sync.Mutex
	ch chan struct{} // closed and replaced on every notification
}

// NewWaker returns a Waker that LISTENs on TaskQueuedChannel on connections from connect.
func NewWaker(connect func(ctx context.Context) (*pgx.Conn, error), c clock.Clock) *Waker {
	return NewChannelWaker(connect, c, TaskQueuedChannel)
}

// NewChannelWaker returns a Waker that LISTENs on channel (quoted as an identifier).
func NewChannelWaker(connect func(ctx context.Context) (*pgx.Conn, error), c clock.Clock, channel string) *Waker {
	return &Waker{connect: connect, clock: c, channel: channel, ch: make(chan struct{})}
}

// Wait returns a channel closed at the next notification. Take it before checking for work, so
// a notification that arrives during the check still wakes the caller.
func (w *Waker) Wait() <-chan struct{} {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.ch
}

func (w *Waker) broadcast() {
	w.mu.Lock()
	defer w.mu.Unlock()
	close(w.ch)
	w.ch = make(chan struct{})
}

// Run listens until ctx ends, reconnecting a second after any failure; it returns nil when ctx
// is done. While disconnected, pollers fall back to re-checking every second.
func (w *Waker) Run(ctx context.Context) error {
	for {
		err := w.listen(ctx)
		if ctx.Err() != nil {
			return nil
		}
		logging.FromContext(ctx).WarnContext(ctx, "notification listener disconnected; retrying", "channel", w.channel, logging.Err(err))
		if err := w.clock.Sleep(ctx, reconnectDelay); err != nil {
			return nil
		}
	}
}

func (w *Waker) listen(ctx context.Context) error {
	conn, err := w.connect(ctx)
	if err != nil {
		return fmt.Errorf("connect listener: %w", err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }() // closing a dead listener can only fail
	// LISTEN takes no parameters and cannot be expressed in sqlc, so the channel is quoted here.
	if _, err := conn.Exec(ctx, "LISTEN "+pgx.Identifier{w.channel}.Sanitize()); err != nil {
		return fmt.Errorf("listen %s: %w", w.channel, err)
	}
	w.broadcast() // notifications may have been missed while disconnected
	for {
		if _, err := conn.WaitForNotification(ctx); err != nil {
			return fmt.Errorf("wait for notification: %w", err)
		}
		w.broadcast()
	}
}
