package scheduler

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/daishakpatel/flowforge/internal/apperrors"
	"github.com/daishakpatel/flowforge/internal/events"
	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/workerapi"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
	"github.com/daishakpatel/flowforge/internal/workflow/statemachine"
)

// ReasonWorkflowTimeout is the run.failed reason when a run's workflow deadline passed (03.05).
const ReasonWorkflowTimeout = "WORKFLOW_TIMEOUT"

// cancelWaiting moves every head task that has not started (PENDING, READY, QUEUED, RETRY_WAIT)
// to CANCELLED through move, and returns how many head tasks are still not terminal: RUNNING
// ones, which only their worker or a forced cancel ends, and any a concurrent change (a claim)
// moved first, which the next pass handles.
func cancelWaiting(heads []*persistence.TaskView, move func(*persistence.TaskView) error) (int, error) {
	left := 0
	for _, t := range heads {
		if t.Status != model.TaskRunning && !statemachine.IsTerminalTask(t.Status) {
			if err := move(t); err != nil {
				return 0, err
			}
		}
		if !statemachine.IsTerminalTask(t.Status) {
			left++
		}
	}
	return left, nil
}

// cancelRun is processRun for a cancel-requested run (03.05): tasks that have not started are
// CANCELLED; RUNNING ones are cancelled by their workers, told through heartbeats
// (workerapi.RunCancelSource), or forced by TimeoutLoop when the cancel grace runs out. Once
// every head task is terminal the run becomes CANCELLED (run.cancelled), from any status.
func (d *decision) cancelRun(ctx context.Context) (result, error) {
	left, err := cancelWaiting(d.heads, func(t *persistence.TaskView) error {
		_, err := d.moveTask(ctx, t, model.TaskCancelled, nil, nil)
		return err
	})
	if err != nil || left > 0 {
		return d.res, err
	}
	moved, err := d.moveRun(ctx, model.RunCancelled, nil)
	if moved {
		d.res.completed = model.RunCancelled
	}
	return d.res, err
}

// endOverdueRun ends a RUNNING or PAUSED run whose deadline has passed, in tx: its RUNNING
// attempts are force-cancelled (fenced, so a late worker report gets STALE_ATTEMPT), its other
// unfinished tasks CANCELLED, and the run becomes CANCELLED when a cancel was requested (the
// grace ran out) or FAILED with reason WORKFLOW_TIMEOUT. A run that changed meanwhile is left
// to the next pass.
func (l *TimeoutLoop) endOverdueRun(ctx context.Context, tx pgx.Tx, runID string, epoch int64) error {
	run, err := l.Sched.LockRun(ctx, tx, runID)
	if err != nil {
		return err
	}
	now := l.Clock.Now()
	if (run.Status != model.RunRunning && run.Status != model.RunPaused) || run.DeadlineAt == nil || run.DeadlineAt.After(now) {
		return nil
	}
	rows, err := l.Sched.HeadTasks(ctx, tx, runID, maxTasksPerRun)
	if err != nil {
		return err
	}
	heads := make([]*persistence.TaskView, len(rows))
	for i := range rows {
		heads[i] = &rows[i]
		if rows[i].Status == model.TaskRunning {
			if err := l.forceCancel(ctx, tx, heads[i]); err != nil {
				return err
			}
		}
	}
	left, err := cancelWaiting(heads, func(t *persistence.TaskView) error {
		err := l.Ender.Tasks.Transition(ctx, tx, persistence.TaskTransitionParams{TaskID: t.ID, From: t.Status, To: model.TaskCancelled})
		if err == nil {
			t.Status = model.TaskCancelled
		} else if apperrors.CodeOf(err) == apperrors.CodeInvalidState {
			err = nil // moved meanwhile (a claim); the next pass sees it
		}
		return err
	})
	if err != nil || left > 0 {
		return err
	}
	p := persistence.RunTransitionParams{RunID: runID, From: run.Status, To: model.RunCancelled, Version: run.Version, LeaderEpoch: &epoch}
	if !run.CancelRequested {
		ev := events.NewRunEvent(statemachine.EventRunFailed, events.RunPayload{
			RunID: run.ID, DefinitionID: run.DefinitionID, Status: model.RunFailed, PreviousStatus: run.Status,
			Reason: ReasonWorkflowTimeout,
		})
		p.To, p.Event = model.RunFailed, &ev
	}
	return l.Runs.Transition(ctx, tx, p)
}

// forceCancel ends t's RUNNING attempt as CANCELLED through the AttemptEnder; the outcome
// handler then cancels the task. A task with no RUNNING attempt is left as it is.
func (l *TimeoutLoop) forceCancel(ctx context.Context, tx pgx.Tx, t *persistence.TaskView) error {
	attempts, err := l.Ender.Attempts.ListByTask(ctx, tx, t.ID)
	if err != nil {
		return err
	}
	for _, a := range attempts {
		if a.Status != model.AttemptRunning {
			continue
		}
		ended, err := l.Ender.End(ctx, tx, a, workerapi.AttemptCancelled, workerapi.CodeCancelled)
		if err != nil {
			return fmt.Errorf("force-cancel attempt %s: %w", a.ID, err)
		}
		if ended {
			t.Status = model.TaskCancelled
		}
	}
	return nil
}
