package statemachine

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/daishakpatel/flowforge/internal/workflow/model"
)

// TaskView is the part of a task row the pure rules need.
type TaskView struct {
	Key       string // task_key
	Status    model.TaskStatus
	RunIf     model.RunIf
	RunIfExpr string
	DependsOn []string        // task keys
	Result    json.RawMessage // for expressions; nil when there is no result
}

// ReadinessDecision is what the scheduler does with a PENDING task.
type ReadinessDecision string

// The readiness decisions.
const (
	Wait  ReadinessDecision = "wait"  // leave PENDING: a dependency is not terminal yet
	Ready ReadinessDecision = "ready" // PENDING → READY
	Skip  ReadinessDecision = "skip"  // PENDING → SKIPPED: run_if is not satisfied
)

// ExprEvaluator evaluates a run_if expression against the task's dependencies. Task 03.06
// implements it.
type ExprEvaluator interface {
	Evaluate(expr string, deps []TaskView) (bool, error)
}

// ErrNoEvaluator is returned for a run_if=expr task when no ExprEvaluator is configured.
var ErrNoEvaluator = errors.New("run_if expr needs an expression evaluator")

// EvaluateReadiness applies 00E §3.4 to a task, given its direct dependencies:
//
//   - not PENDING, or any dependency not terminal: Wait
//   - on_success: Ready if every dependency SUCCEEDED, else Skip (so a skip propagates)
//   - on_failure: Ready if any dependency FAILED or TIMED_OUT, else Skip
//   - always: Ready
//   - expr: Ready if eval returns true, Skip if false; an error (including a nil eval) is returned
//
// A task with no dependencies is Ready under on_success and always, and Skip under on_failure.
// It returns an error if deps lacks one of task.DependsOn or run_if is unknown.
func EvaluateReadiness(task TaskView, deps []TaskView, eval ExprEvaluator) (ReadinessDecision, error) {
	if task.Status != model.TaskPending {
		return Wait, nil
	}
	byKey := make(map[string]TaskView, len(deps))
	for _, d := range deps {
		byKey[d.Key] = d
	}
	allSucceeded, anyFailed := true, false
	for _, key := range task.DependsOn {
		d, ok := byKey[key]
		if !ok {
			return Wait, fmt.Errorf("readiness of %s: dependency %s not provided", task.Key, key)
		}
		if !IsTerminalTask(d.Status) {
			return Wait, nil
		}
		allSucceeded = allSucceeded && d.Status == model.TaskSucceeded
		anyFailed = anyFailed || d.Status == model.TaskFailed || d.Status == model.TaskTimedOut
	}
	switch task.RunIf {
	case model.RunIfOnSuccess:
		return decide(allSucceeded), nil
	case model.RunIfOnFailure:
		return decide(anyFailed), nil
	case model.RunIfAlways:
		return Ready, nil
	case model.RunIfExpr:
		if eval == nil {
			return Wait, fmt.Errorf("readiness of %s: %w", task.Key, ErrNoEvaluator)
		}
		ok, err := eval.Evaluate(task.RunIfExpr, deps)
		if err != nil {
			return Wait, fmt.Errorf("readiness of %s: evaluate run_if_expr: %w", task.Key, err)
		}
		return decide(ok), nil
	default:
		return Wait, fmt.Errorf("readiness of %s: unknown run_if %q", task.Key, task.RunIf)
	}
}

func decide(run bool) ReadinessDecision {
	if run {
		return Ready
	}
	return Skip
}
