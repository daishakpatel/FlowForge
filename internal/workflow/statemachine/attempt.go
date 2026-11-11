package statemachine

import (
	"slices"

	"github.com/daishakpatel/flowforge/internal/workflow/model"
)

// attemptTransitions is 00E §3.2 as data: an attempt only ever leaves RUNNING. Read-only.
var attemptTransitions = map[model.AttemptStatus][]model.AttemptStatus{
	model.AttemptRunning: {model.AttemptSucceeded, model.AttemptFailed, model.AttemptTimedOut,
		model.AttemptAbandoned, model.AttemptCancelled},
}

// CanAttemptTransition reports whether 00E §3.2 allows an attempt to move from one status to
// another. A completion must additionally match the attempt's fencing token (ADR-003); that
// check belongs to the conditional UPDATE, not here.
func CanAttemptTransition(from, to model.AttemptStatus) bool {
	return slices.Contains(attemptTransitions[from], to)
}

// AttemptTransition returns nil when CanAttemptTransition allows the move, else an
// INVALID_STATE error wrapping ErrInvalidTransition.
func AttemptTransition(from, to model.AttemptStatus) error {
	if CanAttemptTransition(from, to) {
		return nil
	}
	return invalid("attempt", from, to)
}

// IsTerminalAttempt reports whether s is final (every status except RUNNING).
func IsTerminalAttempt(s model.AttemptStatus) bool {
	return s != model.AttemptRunning && slices.Contains(model.AttemptStatuses(), s)
}
