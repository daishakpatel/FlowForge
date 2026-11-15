package runtime

import "time"

// drain waits for running attempts after polling has stopped: up to ShutdownGrace for them to
// finish, then cancels the rest (reported WORKER_SHUTDOWN, retryable) and waits up to
// reportGrace for their reports, then cuts reporting off. It returns once every attempt
// goroutine has ended, so nothing outlives Run.
func (w *Worker) drain() {
	done := make(chan struct{})
	go func() { w.attempts.Wait(); close(done) }()
	if w.waitFor(done, w.cfg.ShutdownGrace) {
		return
	}
	w.d.Logger.Warn("shutdown grace over; cancelling running attempts", "running", w.activeCount())
	w.cancelAll(errShutdown)
	if w.waitFor(done, reportGrace) {
		return
	}
	w.d.Logger.Error("attempt reports still pending; abandoning them", "running", w.activeCount())
	w.stop(errReportCutoff)
	<-done
}

// waitFor reports whether done closed within d.
func (w *Worker) waitFor(done <-chan struct{}, d time.Duration) bool {
	t := w.d.Clock.NewTimer(d)
	defer t.Stop()
	select {
	case <-done:
		return true
	case <-t.C():
		return false
	}
}
