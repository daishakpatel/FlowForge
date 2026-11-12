package workerapi

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/logging"
)

// TaskQueuedChannel is the Postgres NOTIFY channel announcing that tasks became QUEUED. Whoever
// queues tasks (the scheduler, 03.02) runs NOTIFY ff_task_queued after committing.
const TaskQueuedChannel = "ff_task_queued"

const reconnectDelay = time.Second

// Waker turns NOTIFY ff_task_queued into a broadcast for long-polling PollTask calls. It owns
// one dedicated connection, held by Run; waiters never touch the pool. Safe for concurrent use.
type Waker struct {
	connect func(ctx context.Context) (*pgx.Conn, error)
	clock   clock.Clock

	mu sync.Mutex
	ch chan struct{} // closed and replaced on every notification
}

// NewWaker returns a Waker that LISTENs on connections from connect.
func NewWaker(connect func(ctx context.Context) (*pgx.Conn, error), c clock.Clock) *Waker {
	return &Waker{connect: connect, clock: c, ch: make(chan struct{})}
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
		logging.FromContext(ctx).WarnContext(ctx, "task-queued listener disconnected; retrying", logging.Err(err))
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
	// A constant statement: LISTEN takes no parameters and cannot be expressed in sqlc.
	if _, err := conn.Exec(ctx, "LISTEN "+TaskQueuedChannel); err != nil {
		return fmt.Errorf("listen %s: %w", TaskQueuedChannel, err)
	}
	w.broadcast() // notifications may have been missed while disconnected
	for {
		if _, err := conn.WaitForNotification(ctx); err != nil {
			return fmt.Errorf("wait for notification: %w", err)
		}
		w.broadcast()
	}
}
