// Package statemachine holds the task, attempt and run state machines of 00E §3 as data, plus
// the pure rules built on them: readiness (run_if), run-status derivation and retry delays
// (task 02.02, ADR-020). Repositories apply a transition only after the matching Can*
// function allows it, and then with a conditional UPDATE … WHERE status = $from.
package statemachine

import (
	"errors"
	"fmt"
	"slices"

	"github.com/daishakpatel/flowforge/internal/apperrors"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
)

// ErrInvalidTransition is wrapped by every error from TaskTransition, AttemptTransition and
// RunTransition. Those errors are INVALID_STATE *apperrors.Error values.
var ErrInvalidTransition = errors.New("invalid state transition")

// taskTransitions is 00E §3.1 as data: from → allowed targets. Read-only.
var taskTransitions = map[model.TaskStatus][]model.TaskStatus{
	model.TaskPending:   {model.TaskReady, model.TaskSkipped, model.TaskCancelled},
	model.TaskReady:     {model.TaskQueued, model.TaskCancelled},
	model.TaskQueued:    {model.TaskRunning, model.TaskCancelled},
	model.TaskRunning:   {model.TaskSucceeded, model.TaskRetryWait, model.TaskFailed, model.TaskTimedOut, model.TaskCancelled},
	model.TaskRetryWait: {model.TaskQueued, model.TaskCancelled},
}

// CanTaskTransition reports whether 00E §3.1 allows a task to move from one status to another.
func CanTaskTransition(from, to model.TaskStatus) bool {
	return slices.Contains(taskTransitions[from], to)
}

// TaskTransition returns nil when CanTaskTransition allows the move, else an INVALID_STATE
// error wrapping ErrInvalidTransition.
func TaskTransition(from, to model.TaskStatus) error {
	if CanTaskTransition(from, to) {
		return nil
	}
	return invalid("task", from, to)
}

// IsTerminalTask reports whether s is final: SUCCEEDED, FAILED, TIMED_OUT, CANCELLED or SKIPPED.
// A failed task is never retried in place; remediation creates a new task row (ADR-008).
func IsTerminalTask(s model.TaskStatus) bool {
	switch s {
	case model.TaskSucceeded, model.TaskFailed, model.TaskTimedOut, model.TaskCancelled, model.TaskSkipped:
		return true
	default:
		return false
	}
}

func invalid[S ~string](kind string, from, to S) error {
	return apperrors.Wrap(apperrors.CodeInvalidState, fmt.Sprintf("%s %s → %s is not allowed", kind, from, to), ErrInvalidTransition)
}
