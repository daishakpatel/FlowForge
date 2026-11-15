package scheduler

import (
	"context"
	"encoding/json"

	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/workflow/statemachine"
	"github.com/daishakpatel/flowforge/internal/workflow/template"
)

// ErrorCodeInvalidInput is the task error code recorded when input resolution fails: the task
// moves READY → FAILED without an attempt, and its task.failed event carries this code. It is
// never retried (00E §3.1).
const ErrorCodeInvalidInput = "INVALID_INPUT"

// InputResolver computes the input a task is queued with, at READY → QUEUED (00E §2: input is
// resolved at that point). ancestors are the task's head ancestors, direct dependencies first,
// all terminal. An error fails the task with ErrorCodeInvalidInput.
type InputResolver interface {
	Resolve(ctx context.Context, run persistence.RunView, task persistence.TaskView, ancestors []persistence.TaskView) (json.RawMessage, error)
}

// IdentityResolver is an InputResolver that queues the task's config verbatim (no templating).
type IdentityResolver struct{}

var _ InputResolver = IdentityResolver{}

// Resolve implements InputResolver.
func (IdentityResolver) Resolve(_ context.Context, _ persistence.RunView, task persistence.TaskView, _ []persistence.TaskView) (json.RawMessage, error) {
	return task.Config, nil
}

// TemplateResolver is the production InputResolver (task 03.06): it resolves the ${…}
// placeholders in a task's config against the run input and the task's ancestors with
// template.Resolver. A placeholder it cannot resolve, or an input over template.MaxInputBytes,
// fails the task with ErrorCodeInvalidInput.
type TemplateResolver struct {
	template.Resolver
}

var _ InputResolver = TemplateResolver{}

// Resolve implements InputResolver.
func (r TemplateResolver) Resolve(_ context.Context, run persistence.RunView, task persistence.TaskView, ancestors []persistence.TaskView) (json.RawMessage, error) {
	s := template.Scope{RunInput: run.Input, Tasks: make(map[string]template.Task, len(ancestors))}
	for _, a := range ancestors {
		s.Tasks[a.Key] = template.Task{Status: string(a.Status), Result: a.Result}
	}
	return r.Resolver.Resolve(task.Config, s)
}

// RunBinder is implemented by an ExprEvaluator whose expressions read run.input
// (expr.Evaluator): the loop binds each run's input before evaluating that run's tasks.
type RunBinder interface {
	ForRun(input json.RawMessage) statemachine.ExprEvaluator
}
