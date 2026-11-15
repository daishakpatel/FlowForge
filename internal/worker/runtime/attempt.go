package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	workerv1 "github.com/daishakpatel/flowforge/internal/gen/proto/worker/v1"

	"github.com/daishakpatel/flowforge/internal/worker/handlers"
)

// attempt is one running task attempt.
type attempt struct {
	task   *workerv1.Task
	cancel context.CancelCauseFunc // cause: errServerCancel, errStale or errShutdown
	logs   *logSink
}

// start runs t in its own goroutine. Only the poll loop calls it. The attempt keeps ctx's values
// but not its cancellation: ctx is the poll loop's, which ends at shutdown, and a running
// attempt gets the shutdown grace instead (drain cancels it through w.running).
func (w *Worker) start(ctx context.Context, t *workerv1.Task) {
	actx, cancel := context.WithCancelCause(context.WithoutCancel(ctx))
	a := &attempt{task: t, cancel: cancel, logs: newLogSink(w.cfg.LogLines)}
	w.mu.Lock()
	w.running[t.GetAttemptId()] = a
	n := len(w.running)
	w.mu.Unlock()
	w.d.Metrics.setActive(n)
	w.attempts.Add(1)
	go func() {
		defer w.attempts.Done()
		defer w.end(a)
		w.run(actx, a)
	}()
}

// end removes a from the running set and wakes the poll loop.
func (w *Worker) end(a *attempt) {
	a.cancel(nil)
	w.mu.Lock()
	delete(w.running, a.task.GetAttemptId())
	n := len(w.running)
	w.mu.Unlock()
	w.d.Metrics.setActive(n)
	select {
	case w.freed <- struct{}{}:
	default: // already signalled
	}
}

// run executes the handler under a context that ends deadlineMargin before the server's
// deadline (courtesy only: the server's timeout decision is final, ADR-021), then ships the logs
// and reports the outcome. A stale attempt's result is dropped unreported.
func (w *Worker) run(parent context.Context, a *attempt) {
	t := a.task
	ctx, stop := context.WithDeadline(parent, t.GetDeadline().AsTime().Add(-deadlineMargin))
	defer stop()
	tc := handlers.TaskContext{
		TaskID: t.GetTaskId(), AttemptID: t.GetAttemptId(), AttemptNumber: int(t.GetAttemptNumber()),
		IdempotencyKey: t.GetIdempotencyKey(), Input: t.GetInputJson(), Deadline: t.GetDeadline().AsTime(),
		Logger: slog.New(&logHandler{sink: a.logs}), RunID: t.GetCorrelationId(), Traceparent: t.GetTraceparent(),
	}
	var result json.RawMessage
	var err error
	if h, ok := w.d.Registry.Get(t.GetTaskType()); ok {
		result, err = handlers.Execute(ctx, h, tc)
	} else {
		err = handlers.NonRetryable(handlers.CodeUnknownType, fmt.Errorf("no handler for task type %q", t.GetTaskType()))
	}
	if ctx.Err() != nil && errors.Is(context.Cause(ctx), errStale) {
		w.d.Logger.Warn("attempt is stale; result dropped", "attempt_id", t.GetAttemptId(), "task_id", t.GetTaskId())
		w.d.Metrics.outcome("stale")
		return
	}
	rctx, done := w.reportContext(ctx, a)
	defer done()
	w.shipLogs(rctx, a)
	w.reportOutcome(ctx, rctx, a, result, err)
}

// reportContext is ctx without its cancellation (a cancelled attempt still reports how it
// ended), bounded by the attempt's deadline and the worker's shutdown cut-off.
func (w *Worker) reportContext(ctx context.Context, a *attempt) (context.Context, context.CancelFunc) {
	rctx, cancel := context.WithDeadline(context.WithoutCancel(ctx), a.task.GetDeadline().AsTime())
	unhook := context.AfterFunc(w.reportCtx, cancel)
	return rctx, func() { unhook(); cancel() }
}

// reportOutcome turns the handler's return into CompleteTask or FailTask. ctx is the attempt's
// context (its cause tells why it ended); rctx bounds the report.
func (w *Worker) reportOutcome(ctx, rctx context.Context, a *attempt, result json.RawMessage, err error) {
	id, token := a.task.GetAttemptId(), a.task.GetFencingToken()
	cause := context.Cause(ctx)
	fail := func(outcome, code string, retryable bool, msg string) {
		w.report(rctx, a, outcome, func(cctx context.Context) error {
			_, err := w.d.Client.FailTask(cctx, &workerv1.FailTaskRequest{
				AttemptId: id, FencingToken: token, ErrorCode: code, Message: msg, Retryable: retryable,
			})
			return err
		})
	}
	switch {
	case err == nil && len(result) > 0 && !json.Valid(result):
		fail("failed", "INVALID_RESULT", false, "handler returned invalid JSON")
	case err == nil:
		w.report(rctx, a, "succeeded", func(cctx context.Context) error {
			_, err := w.d.Client.CompleteTask(cctx, &workerv1.CompleteTaskRequest{AttemptId: id, FencingToken: token, ResultJson: result})
			return err
		})
	case errors.Is(cause, errServerCancel):
		fail("cancelled", handlers.CodeCancelled, false, "cancelled by the server")
	case errors.Is(cause, errShutdown):
		fail("shutdown", handlers.CodeWorkerShutdown, true, "worker shut down before the handler finished")
	default:
		code, retryable := handlers.Classify(err)
		fail("failed", code, retryable, err.Error())
	}
}

// report makes one fenced report call, repeating it with backoff while the API is unreachable,
// until ctx ends (the attempt's deadline or the shutdown cut-off). STALE_ATTEMPT is final and
// never repeated: another attempt owns the task (ADR-003).
func (w *Worker) report(ctx context.Context, a *attempt, outcome string, call func(context.Context) error) {
	logger := w.d.Logger.With("attempt_id", a.task.GetAttemptId(), "task_id", a.task.GetTaskId())
	for n := 0; ; n++ {
		rctx, done := context.WithTimeout(ctx, w.cfg.RPCTimeout)
		err := call(rctx)
		done()
		switch {
		case err == nil:
			w.d.Metrics.outcome(outcome)
			return
		case stale(err):
			logger.Warn("report rejected: attempt is stale; result dropped")
			a.cancel(errStale)
			w.d.Metrics.outcome("stale")
			return
		case !transient(err):
			logger.Error("report rejected", "outcome", outcome, "err", err)
			w.d.Metrics.outcome("report_failed")
			return
		}
		w.noteCallError(err)
		if w.d.Clock.Sleep(ctx, w.backoff.delay(n)) != nil {
			logger.Error("gave up reporting: deadline or shutdown reached", "outcome", outcome, "err", err)
			w.d.Metrics.outcome("report_failed")
			return
		}
	}
}
