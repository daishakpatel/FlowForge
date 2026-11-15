package runtime

import (
	"context"
	"time"

	workerv1 "github.com/daishakpatel/flowforge/internal/gen/proto/worker/v1"
)

// heartbeatLoop heartbeats every interval until ctx ends. It runs in its own goroutine, keeps
// going while handlers block and through the shutdown grace, and shares no lock with handlers.
func (w *Worker) heartbeatLoop(ctx context.Context) {
	every := time.Duration(w.beatEvery.Load())
	t := w.d.Clock.NewTicker(every)
	defer t.Stop()
	failures := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C():
		}
		if now := time.Duration(w.beatEvery.Load()); now != every { // re-registration changed it
			every = now
			t.Reset(every)
		}
		if !w.ensureRegistered(ctx, failures) {
			w.d.Metrics.leaseRenewalFailed() // no heartbeat this tick: the lease was not renewed
			failures++
			continue
		}
		if !w.beat(ctx) {
			failures++
			continue
		}
		failures = 0
	}
}

// beat sends one heartbeat for the running attempts and applies the answer: cancel ids cancel
// their attempt (reported CANCELLED), stale ids cancel it and drop its result. It reports
// whether the heartbeat succeeded.
func (w *Worker) beat(ctx context.Context) bool {
	rctx, cancel := context.WithTimeout(ctx, w.cfg.RPCTimeout)
	defer cancel()
	resp, err := w.d.Client.Heartbeat(rctx, &workerv1.HeartbeatRequest{WorkerId: w.cfg.ID, Running: w.snapshot()})
	if err != nil {
		if ctx.Err() == nil {
			w.d.Metrics.leaseRenewalFailed()
			w.noteCallError(err)
			w.d.Logger.Warn("heartbeat failed; running attempts continue", "err", err)
		}
		return false
	}
	for _, id := range resp.GetCancelAttemptIds() {
		w.cancelAttempt(id, errServerCancel)
	}
	for _, id := range resp.GetStaleAttemptIds() {
		w.cancelAttempt(id, errStale)
	}
	return true
}
