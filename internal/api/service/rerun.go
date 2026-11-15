package service

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daishakpatel/flowforge/internal/apperrors"
	"github.com/daishakpatel/flowforge/internal/events"
	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
	"github.com/daishakpatel/flowforge/internal/workflow/statemachine"
)

// Rerun limits and defaults (task 03.06).
const (
	MaxRerunReasonRunes = 1000
	MaxRerunTasks       = 500 // the definition task limit
	RetryReason         = "retry"
)

// RerunParams is the input of RunService.Rerun.
type RerunParams struct {
	RunID string
	// TaskKeys are the head tasks to rerun; each must be FAILED, TIMED_OUT or CANCELLED. Empty
	// means every FAILED or TIMED_OUT head task (what retry does).
	TaskKeys []string
	Reason   string // required; recorded in run.resumed
	Actor    string // who asked: an API key id, or the approval executor (06.06)
}

// Rerun remediates a FAILED or NEEDS_ATTENTION run (ADR-008, FR-WF-10, task 03.06) and returns
// the ids of the task rows it created, in creation order.
//
// In one transaction it inserts a new PENDING row for each target task and for every descendant
// head task that did not succeed, each superseding the row it replaces (supersedes_task_id).
// The originals stay as they are, so history is immutable. Descendants that SUCCEEDED are kept.
// The run moves back to RUNNING (completed_at cleared, the workflow deadline restarted) with a
// run.resumed event listing the superseded and new rows and the reason, and the scheduler is
// woken; it then treats the new rows like any other head tasks.
//
// Errors: a run in another status, or being cancelled, is INVALID_STATE, so a second identical
// request after the first committed is rejected; an unknown task key is NOT_FOUND; a target
// that did not fail, or a row to replace that is not finished, is INVALID_STATE. Concurrent
// reruns are serialized by the run version and the unique (run_id, task_key, supersedes_task_id)
// constraint: one commits, the other gets CONFLICT and creates nothing.
func (s *RunService) Rerun(ctx context.Context, p RerunParams) ([]string, error) {
	if err := validateRerun(p); err != nil {
		return nil, err
	}
	var created []string
	err := s.Tx.WithTx(ctx, pgx.TxOptions{}, func(tx pgx.Tx) error {
		run, err := s.RunRepo.Get(ctx, tx, p.RunID)
		if err != nil {
			return err
		}
		if (run.Status != model.RunFailed && run.Status != model.RunNeedsAttention) || run.CancelRequested {
			return apperrors.InvalidState(fmt.Sprintf("run %s is %s%s; only a FAILED or NEEDS_ATTENTION run can be rerun", run.ID, run.Status, cancelNote(run)))
		}
		rows, err := s.TaskRepo.ListByRun(ctx, tx, run.ID)
		if err != nil {
			return err
		}
		plan, err := planRerun(heads(rows), p.TaskKeys)
		if err != nil {
			return err
		}
		superseded := make([]string, len(plan))
		for i, t := range plan {
			superseded[i] = t.ID
		}
		if created, err = s.Reruns.InsertReruns(ctx, tx, run.ID, superseded); err != nil {
			return err
		}
		return s.reopen(ctx, tx, run, p, superseded, created)
	})
	if err != nil {
		return nil, err
	}
	return created, nil
}

// Retry reruns every FAILED or TIMED_OUT head task of a run (POST …/retry); reason defaults to
// RetryReason.
func (s *RunService) Retry(ctx context.Context, runID, reason, actor string) ([]string, error) {
	if strings.TrimSpace(reason) == "" {
		reason = RetryReason
	}
	return s.Rerun(ctx, RerunParams{RunID: runID, Reason: reason, Actor: actor})
}

// HeadTasks returns a run's head task rows (those no rerun has superseded) in creation order, or
// NOT_FOUND for an unknown run. Tasks returns every generation.
func (s *RunService) HeadTasks(ctx context.Context, runID string) ([]persistence.TaskView, error) {
	rows, err := s.Tasks(ctx, runID)
	if err != nil {
		return nil, err
	}
	return heads(rows), nil
}

func validateRerun(p RerunParams) error {
	switch reason := strings.TrimSpace(p.Reason); {
	case reason == "":
		return apperrors.InvalidArgument("reason is required")
	case len([]rune(reason)) > MaxRerunReasonRunes:
		return apperrors.InvalidArgument(fmt.Sprintf("reason is longer than %d characters", MaxRerunReasonRunes))
	case len(p.TaskKeys) > MaxRerunTasks:
		return apperrors.InvalidArgument(fmt.Sprintf("task_ids has more than %d entries", MaxRerunTasks))
	}
	return nil
}

// reopen moves the run back to RUNNING with run.resumed, restarts its workflow deadline and wakes
// the scheduler.
func (s *RunService) reopen(ctx context.Context, tx pgx.Tx, run persistence.RunView, p RerunParams, superseded, created []string) error {
	deadline, err := s.restartedDeadline(ctx, tx, run)
	if err != nil {
		return err
	}
	ev := events.NewRunEvent(statemachine.EventRunResumed, events.RunPayload{
		RunID: run.ID, DefinitionID: run.DefinitionID, Status: model.RunRunning, PreviousStatus: run.Status,
		Reason: strings.TrimSpace(p.Reason), Actor: p.Actor, SupersededTaskIDs: superseded, RerunTaskIDs: created,
	})
	if err := s.RunRepo.Transition(ctx, tx, persistence.RunTransitionParams{
		RunID: run.ID, From: run.Status, To: model.RunRunning, Version: run.Version, DeadlineAt: deadline, Event: &ev,
	}); err != nil {
		return err
	}
	return s.Notify.Notify(ctx, tx, persistence.SchedulerWakeChannel)
}

// restartedDeadline is now + the definition's timeout_seconds, or nil without one: a rerun gets
// the whole workflow timeout again, or a run that failed on its deadline would fail at once.
func (s *RunService) restartedDeadline(ctx context.Context, tx pgx.Tx, run persistence.RunView) (*time.Time, error) {
	stored, err := s.DefRepo.Get(ctx, tx, run.DefinitionID)
	if err != nil {
		return nil, err
	}
	def, err := model.Decode(stored.Definition)
	if err != nil {
		return nil, apperrors.Internal(fmt.Errorf("decode stored definition %s: %w", stored.ID, err))
	}
	if def.TimeoutSeconds <= 0 {
		return nil, nil
	}
	at := s.Clock.Now().Add(time.Duration(def.TimeoutSeconds) * time.Second)
	return &at, nil
}

// heads keeps the rows no other row supersedes, in their order.
func heads(rows []persistence.TaskView) []persistence.TaskView {
	superseded := make(map[string]bool, len(rows))
	for _, r := range rows {
		if r.SupersedesTaskID != "" {
			superseded[r.SupersedesTaskID] = true
		}
	}
	out := make([]persistence.TaskView, 0, len(rows))
	for _, r := range rows {
		if !superseded[r.ID] {
			out = append(out, r)
		}
	}
	return out
}

// planRerun picks the head rows a rerun replaces, in creation order: the targets (keys, or every
// FAILED or TIMED_OUT head when keys is empty) and every descendant that did not succeed.
func planRerun(heads []persistence.TaskView, keys []string) ([]persistence.TaskView, error) {
	byKey := make(map[string]persistence.TaskView, len(heads))
	children := map[string][]string{}
	for _, h := range heads {
		byKey[h.Key] = h
		for _, dep := range h.DependsOn {
			children[dep] = append(children[dep], h.Key)
		}
	}
	targets, err := rerunTargets(heads, byKey, keys)
	if err != nil {
		return nil, err
	}
	pick := map[string]bool{}
	for _, k := range targets {
		pick[k] = true
	}
	// Walk every descendant, succeeded ones included: what lies below a kept task still depends on
	// the targets.
	seen := map[string]bool{}
	for queue := slices.Clone(targets); len(queue) > 0; queue = queue[1:] {
		for _, c := range children[queue[0]] {
			if !seen[c] {
				seen[c] = true
				pick[c] = pick[c] || byKey[c].Status != model.TaskSucceeded
				queue = append(queue, c)
			}
		}
	}
	var out []persistence.TaskView
	for _, h := range heads {
		if !pick[h.Key] {
			continue
		}
		if !statemachine.IsTerminalTask(h.Status) {
			return nil, apperrors.InvalidState(fmt.Sprintf("task %s is %s; a rerun replaces only finished tasks", h.Key, h.Status))
		}
		out = append(out, h)
	}
	return out, nil
}

func rerunTargets(heads []persistence.TaskView, byKey map[string]persistence.TaskView, keys []string) ([]string, error) {
	if len(keys) == 0 {
		var out []string
		for _, h := range heads {
			if h.Status == model.TaskFailed || h.Status == model.TaskTimedOut {
				out = append(out, h.Key)
			}
		}
		if len(out) == 0 {
			return nil, apperrors.InvalidState("the run has no FAILED or TIMED_OUT task to retry")
		}
		return out, nil
	}
	for _, k := range keys {
		h, ok := byKey[k]
		switch {
		case !ok:
			return nil, apperrors.NotFound(fmt.Sprintf("task %q not found in the run", k))
		case h.Status != model.TaskFailed && h.Status != model.TaskTimedOut && h.Status != model.TaskCancelled:
			return nil, apperrors.InvalidState(fmt.Sprintf("task %s is %s; only a FAILED, TIMED_OUT or CANCELLED task can be rerun", k, h.Status))
		}
	}
	return keys, nil
}
