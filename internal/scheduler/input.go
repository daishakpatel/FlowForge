package scheduler

import (
	"context"
	"encoding/json"

	"github.com/daishakpatel/flowforge/internal/persistence"
)

// ErrorCodeInvalidInput is the task error code recorded when input resolution fails: the task
// moves READY → FAILED without an attempt, and its task.failed event carries this code. It is
// never retried (00E §3.1).
const ErrorCodeInvalidInput = "INVALID_INPUT"

// InputResolver computes the input a task is queued with, at READY → QUEUED (00E §2: input is
// resolved at that point). deps are the task's direct dependencies, all terminal. An error
// fails the task with ErrorCodeInvalidInput. Task 03.06 replaces the default with templating
// over the run input and dependency results.
type InputResolver interface {
	Resolve(ctx context.Context, run persistence.RunView, task persistence.TaskView, deps []persistence.TaskView) (json.RawMessage, error)
}

// IdentityResolver is the default InputResolver: the task's config, verbatim.
type IdentityResolver struct{}

var _ InputResolver = IdentityResolver{}

// Resolve implements InputResolver.
func (IdentityResolver) Resolve(_ context.Context, _ persistence.RunView, task persistence.TaskView, _ []persistence.TaskView) (json.RawMessage, error) {
	return task.Config, nil
}
