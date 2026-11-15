package runtime

import (
	"context"

	workerv1 "github.com/daishakpatel/flowforge/internal/gen/proto/worker/v1"
)

// pollLoop long-polls for at most the free capacity and starts an attempt per task, until ctx
// ends. With no free capacity it waits for an attempt to end; it never asks for more than
// Capacity minus the running attempts, and only this loop adds attempts, so the worker never
// runs more than Capacity at once.
func (w *Worker) pollLoop(ctx context.Context) {
	failures := 0
	for ctx.Err() == nil {
		free := w.cfg.Capacity - w.activeCount()
		if free <= 0 {
			select {
			case <-w.freed:
			case <-ctx.Done():
			}
			continue
		}
		if !w.ensureRegistered(ctx, failures) {
			failures++
			continue
		}
		tasks, err := w.poll(ctx, free) // returns tasks even when ctx ended meanwhile; start them
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			w.noteCallError(err)
			w.d.Logger.Warn("poll failed", "err", err)
			_ = w.d.Clock.Sleep(ctx, w.backoff.delay(failures)) // ctx ending ends the loop
			failures++
			continue
		}
		failures = 0
		for _, t := range tasks {
			w.start(ctx, t)
		}
	}
}

// poll makes one PollTask call. Shutdown does not cancel it: the API may already have claimed
// tasks for this worker and committed, and cancelling would drop the response carrying them,
// orphaning those attempts until recovery. The call is bounded by PollWait + RPCTimeout instead,
// and the tasks it returns are started and get the shutdown grace.
func (w *Worker) poll(ctx context.Context, free int) ([]*workerv1.Task, error) {
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), w.cfg.PollWait+w.cfg.RPCTimeout)
	defer cancel()
	resp, err := w.d.Client.PollTask(rctx, &workerv1.PollTaskRequest{
		WorkerId: w.cfg.ID, FreeCapacity: int32(free), WaitMs: int32(w.cfg.PollWait.Milliseconds()), //nolint:gosec // G115: free ≤ capacity ≤ 1000; wait ≤ 30 s
	})
	if err != nil {
		return nil, err
	}
	return resp.GetTasks(), nil
}
