package statemachine

import (
	"slices"

	"github.com/daishakpatel/flowforge/internal/workflow/model"
)

// runTransitions is 00E §3.3 as data. Read-only. 00E lists no move out of CREATED or PAUSED
// other than to RUNNING, so cancelling a run in either state needs a 00E change first.
var runTransitions = map[model.RunStatus][]model.RunStatus{
	model.RunCreated:        {model.RunRunning},
	model.RunRunning:        {model.RunSucceeded, model.RunFailed, model.RunCancelled, model.RunNeedsAttention, model.RunPaused},
	model.RunPaused:         {model.RunRunning},
	model.RunNeedsAttention: {model.RunRunning, model.RunFailed, model.RunCancelled},
}

// CanRunTransition reports whether 00E §3.3 allows a run to move from one status to another.
func CanRunTransition(from, to model.RunStatus) bool {
	return slices.Contains(runTransitions[from], to)
}

// RunTransition returns nil when CanRunTransition allows the move, else an INVALID_STATE error
// wrapping ErrInvalidTransition.
func RunTransition(from, to model.RunStatus) error {
	if CanRunTransition(from, to) {
		return nil
	}
	return invalid("run", from, to)
}

// IsTerminalRun reports whether s is final: SUCCEEDED, FAILED or CANCELLED. NEEDS_ATTENTION is
// not: a rerun or an executed approval moves the run back to RUNNING (ADR-008).
func IsTerminalRun(s model.RunStatus) bool {
	return s == model.RunSucceeded || s == model.RunFailed || s == model.RunCancelled
}

// DeriveRunStatus computes a run's status from its tasks (00E §3.3–3.4). Pass the current
// instance of each task key (leave out rows a rerun has superseded). PAUSED and CANCELLED come
// from the run's own flags, so the caller checks those first.
//
//   - Any task not terminal: RUNNING.
//   - Otherwise every failure must be handled. A FAILED or TIMED_OUT task is handled when a task
//     that depends on it directly, with run_if on_failure, always or expr, SUCCEEDED. A
//     CANCELLED task is never handled.
//   - No unhandled failure: SUCCEEDED. Otherwise NEEDS_ATTENTION under the needs_attention
//     policy, else FAILED.
func DeriveRunStatus(tasks []TaskView, onFailure model.OnFailure) model.RunStatus {
	handled := map[string]bool{}
	for _, t := range tasks {
		if !IsTerminalTask(t.Status) {
			return model.RunRunning
		}
		if t.Status == model.TaskSucceeded && t.RunIf != model.RunIfOnSuccess {
			for _, dep := range t.DependsOn {
				handled[dep] = true
			}
		}
	}
	for _, t := range tasks {
		failed := t.Status == model.TaskFailed || t.Status == model.TaskTimedOut
		if t.Status == model.TaskCancelled || (failed && !handled[t.Key]) {
			if onFailure == model.OnFailureNeedsAttention {
				return model.RunNeedsAttention
			}
			return model.RunFailed
		}
	}
	return model.RunSucceeded
}
