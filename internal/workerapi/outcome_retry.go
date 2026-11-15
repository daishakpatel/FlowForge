package workerapi

import (
	"context"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daishakpatel/flowforge/internal/apperrors"
	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/events"
	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/testutil/failpoint"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
	"github.com/daishakpatel/flowforge/internal/workflow/statemachine"
)

// CodeWorkerLost is the task error code when the last allowed attempt was abandoned because its
// worker was lost (03.04 decision table).
const CodeWorkerLost = "WORKER_LOST"

// RetryOutcomeHandler is the default OutcomeHandler from task 03.04 (FR-WF-07, FR-WF-08): it
// retries failed, timed-out and abandoned attempts according to the task's retry_policy.
//
//	Outcome    retryable / code                    attempts left  task becomes
//	Failed     retryable, code ∉ non_retryable     yes            RETRY_WAIT
//	Failed     not retryable, or code listed       —              FAILED
//	Failed     retryable                           no             FAILED
//	TimedOut   (always retryable)                  yes / no       RETRY_WAIT / TIMED_OUT
//	Abandoned  (always retryable)                  yes / no       RETRY_WAIT / FAILED (WORKER_LOST)
//
// Abandoned attempts count toward max_attempts: attempt_count rises at every claim. A retry
// sets available_at = clock.Now() + statemachine.NextRetryDelay, the time base that promotion
// and claiming compare with. Any outcome but success of a task whose run has a cancel request
// makes the task CANCELLED instead (03.05). Safe for concurrent use.
type RetryOutcomeHandler struct {
	Tasks persistence.TaskRepo
	Runs  persistence.RunRepo
	Clock clock.Clock

	mu  sync.Mutex
	rng *rand.Rand // jitter; nil means none
}

var _ OutcomeHandler = (*RetryOutcomeHandler)(nil)

// NewRetryOutcomeHandler returns a RetryOutcomeHandler drawing jitter from rng (seeded in
// tests; nil disables jitter).
func NewRetryOutcomeHandler(tasks persistence.TaskRepo, runs persistence.RunRepo, c clock.Clock, rng *rand.Rand) *RetryOutcomeHandler {
	return &RetryOutcomeHandler{Tasks: tasks, Runs: runs, Clock: c, rng: rng}
}

// retryDecision is what a finished attempt means for its task.
type retryDecision struct {
	to   model.TaskStatus // RETRY_WAIT or a terminal status
	code string           // error_code on the task.failed / task.timed_out event
}

// decide applies the decision table to an attempt that ended Failed, TimedOut or Abandoned.
// attemptCount is the task's attempt_count, including the attempt that just ended.
func decide(p model.RetryPolicy, attemptCount int, kind AttemptKind, code string, retryable bool) retryDecision {
	left := attemptCount < max(p.MaxAttempts, 1)
	switch kind {
	case AttemptTimedOut:
		if left {
			return retryDecision{to: model.TaskRetryWait}
		}
		return retryDecision{to: model.TaskTimedOut, code: code}
	case AttemptAbandoned:
		if left {
			return retryDecision{to: model.TaskRetryWait}
		}
		return retryDecision{to: model.TaskFailed, code: CodeWorkerLost}
	default: // AttemptFailed
		if !retryable || slices.Contains(p.NonRetryableErrors, code) || !left {
			return retryDecision{to: model.TaskFailed, code: code}
		}
		return retryDecision{to: model.TaskRetryWait}
	}
}

// OnAttemptFinished implements OutcomeHandler.
func (h *RetryOutcomeHandler) OnAttemptFinished(ctx context.Context, tx pgx.Tx, o AttemptOutcome) error {
	move := func(to model.TaskStatus, mutate func(*persistence.TaskMutation), ev *events.Event) error {
		return h.Tasks.Transition(ctx, tx, persistence.TaskTransitionParams{
			TaskID: o.Task.ID, From: model.TaskRunning, To: to, Mutate: mutate, Event: ev,
		})
	}
	switch o.Kind {
	case AttemptSucceeded:
		return move(model.TaskSucceeded, func(m *persistence.TaskMutation) { m.Result = o.Result }, nil)
	case AttemptCancelled:
		return move(model.TaskCancelled, nil, nil)
	}
	run, err := h.Runs.Get(ctx, tx, o.Task.RunID)
	if err != nil {
		return err
	}
	if run.CancelRequested { // the run is being cancelled: no retry, no failure (03.05)
		return move(model.TaskCancelled, nil, nil)
	}
	d := decide(o.Task.RetryPolicy, o.Task.AttemptCount, o.Kind, o.ErrorCode, o.Retryable)
	if d.to != model.TaskRetryWait {
		ev := events.NewTaskEvent(statemachine.TaskTransitionEvent(model.TaskRunning, d.to), events.TaskPayload{
			RunID: o.Task.RunID, TaskID: o.Task.ID, TaskKey: o.Task.Key, Status: d.to, PreviousStatus: model.TaskRunning,
			AttemptCount: o.Task.AttemptCount, ErrorCode: d.code,
		})
		return move(d.to, nil, &ev)
	}
	delay := h.delay(o.Task.RetryPolicy, o.Attempt.AttemptNumber)
	at := h.Clock.Now().Add(delay)
	return move(model.TaskRetryWait, func(m *persistence.TaskMutation) { m.RetryDelay, m.AvailableAt = &delay, &at }, nil)
}

func (h *RetryOutcomeHandler) delay(p model.RetryPolicy, attempt int) time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	return statemachine.NextRetryDelay(p, attempt, h.rng)
}

// AttemptEnder ends RUNNING attempts on the server's own decision: a passed deadline
// (scheduler.TimeoutLoop) or a lost worker (recovery.Loop). Like a worker's report, it moves the
// attempt with the conditional, fenced UPDATE and hands the outcome to Outcomes in the same
// transaction, so attempt and task move together or not at all (ADR-003, ADR-020).
type AttemptEnder struct {
	Attempts persistence.AttemptRepo
	Tasks    persistence.TaskRepo
	Outcomes OutcomeHandler
}

// End moves a (read as RUNNING, with its fencing token) to kind, TIMED_OUT or ABANDONED, with
// code, then runs the OutcomeHandler, in tx. It reports false, with no error, when the attempt
// had already ended: a worker's report or another decision won, and ending it again is a no-op.
func (e AttemptEnder) End(ctx context.Context, tx pgx.Tx, a persistence.AttemptView, kind AttemptKind, code string) (bool, error) {
	retryable := true // the server ending an attempt says nothing against retrying the task
	err := e.Attempts.Transition(ctx, tx, persistence.AttemptTransitionParams{
		AttemptID: a.ID, From: model.AttemptRunning, To: model.AttemptStatus(kind), FencingToken: a.FencingToken,
		Mutate: func(m *persistence.AttemptMutation) { m.ErrorCode, m.Retryable = code, &retryable },
	})
	if apperrors.CodeOf(err) == apperrors.CodeStaleAttempt {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	task, err := e.Tasks.Get(ctx, tx, a.TaskID)
	if err != nil {
		return false, err
	}
	if err := failpoint.Inject(failpoint.OutcomeAfterAttemptEnded); err != nil {
		return false, err
	}
	a.Status, a.ErrorCode, a.Retryable = model.AttemptStatus(kind), code, &retryable
	return true, e.Outcomes.OnAttemptFinished(ctx, tx, AttemptOutcome{
		Attempt: a, Task: task, Kind: kind, ErrorCode: code, Retryable: true,
	})
}
