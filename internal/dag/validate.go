package dag

import (
	"regexp"
	"slices"
	"strings"

	"github.com/daishakpatel/flowforge/internal/workflow/model"
)

// Definition limits (00E §3.5 and task 02.02).
const (
	MaxTasks                  = 500
	MinTaskTimeoutSeconds     = 1
	MaxTaskTimeoutSeconds     = 86400
	MaxWorkflowTimeoutSeconds = 7 * 86400 // 0 means no workflow timeout
	MinMaxAttempts            = 1
	MaxMaxAttempts            = 20
	MaxRetryDelayMS           = 86_400_000 // 24 h
	MaxTypeLength             = 128
)

// Patterns for task IDs and queue names (00E §3.5) and workflow names.
var (
	taskIDPattern       = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,62}$`)
	workflowNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,127}$`)
	exprRefPattern      = regexp.MustCompile(`\btasks\.([a-z][a-z0-9_-]{0,62})\b`)
)

// ExprValidator checks the syntax of a run_if expression; it returns nil when the expression is
// valid. Task 03.06 provides one for its expression grammar.
type ExprValidator func(expr string) error

// ExprRefs returns the task IDs a run_if expression refers to.
type ExprRefs func(expr string) []string

// Validator validates definitions. Its zero value checks everything except expression syntax;
// task 03.06 sets ValidateExpr (and, if its grammar needs it, Refs). Validator holds no state,
// so it is safe for concurrent use.
type Validator struct {
	ValidateExpr ExprValidator // nil: expressions are only checked for being non-empty
	Refs         ExprRefs      // nil: every "tasks.<id>" in the text counts as a reference
}

// Validate is Validator{}.Validate.
func Validate(def *model.Definition) []ValidationIssue { return Validator{}.Validate(def) }

// Validate returns every problem in def, sorted by (task ID, code, message); nil means the
// definition is valid. It runs in O(V+E) apart from expression checks, which walk ancestors
// once per run_if=expr task.
func (v Validator) Validate(def *model.Definition) []ValidationIssue {
	if def == nil {
		return []ValidationIssue{{Code: CodeInvalidWorkflowField, Message: "definition is missing"}}
	}
	var c collector
	validateWorkflow(def, &c)
	g := newGraph(def)
	seen := make(map[string]bool, len(def.Tasks))
	for i := range def.Tasks {
		t := &def.Tasks[i]
		if seen[t.ID] {
			c.add(CodeDuplicateTaskID, t.ID, "task id %q is used more than once", t.ID)
		}
		seen[t.ID] = true
		validateTask(t, g, &c)
		v.validateRunIf(def, t, &c)
	}
	if _, done := g.topo(); slices.Contains(done, false) {
		path := g.cycle(done)
		c = append(c, ValidationIssue{Code: CodeCycle, TaskID: path[0], Path: path,
			Message: "dependency cycle: " + strings.Join(path, " → ") + " (each task depends on the next)"})
	}
	return c.sorted()
}

func validateWorkflow(def *model.Definition, c *collector) {
	if !workflowNamePattern.MatchString(def.Name) {
		c.add(CodeInvalidWorkflowField, "", "name %q must match %s", def.Name, workflowNamePattern)
	}
	if def.Version < 1 {
		c.add(CodeInvalidWorkflowField, "", "version must be at least 1, got %d", def.Version)
	}
	if def.TimeoutSeconds < 0 || def.TimeoutSeconds > MaxWorkflowTimeoutSeconds {
		c.add(CodeInvalidWorkflowField, "", "timeout_seconds must be 0 (none) to %d, got %d", MaxWorkflowTimeoutSeconds, def.TimeoutSeconds)
	}
	if def.OnFailure != model.OnFailureFail && def.OnFailure != model.OnFailureNeedsAttention {
		c.add(CodeInvalidWorkflowField, "", "on_failure must be %q or %q, got %q", model.OnFailureFail, model.OnFailureNeedsAttention, def.OnFailure)
	}
	if mp := def.Concurrency.MaxParallelTasks; mp < 0 || mp > MaxTasks {
		c.add(CodeInvalidWorkflowField, "", "concurrency.max_parallel_tasks must be 0 (no limit) to %d, got %d", MaxTasks, mp)
	}
	switch n := len(def.Tasks); {
	case n == 0:
		c.add(CodeInvalidWorkflowField, "", "a workflow needs at least one task")
	case n > MaxTasks:
		c.add(CodeTooManyTasks, "", "%d tasks; the limit is %d", n, MaxTasks)
	}
}

func validateTask(t *model.TaskDef, g *graph, c *collector) {
	if !taskIDPattern.MatchString(t.ID) {
		c.add(CodeInvalidTaskID, t.ID, "task id %q must match %s", t.ID, taskIDPattern)
	}
	if strings.TrimSpace(t.Type) == "" {
		c.add(CodeMissingType, t.ID, "type is required")
	} else if len(t.Type) > MaxTypeLength {
		c.add(CodeMissingType, t.ID, "type is longer than %d characters", MaxTypeLength)
	}
	if !taskIDPattern.MatchString(t.Queue) {
		c.add(CodeInvalidQueue, t.ID, "queue %q must match %s", t.Queue, taskIDPattern)
	}
	if t.TimeoutSeconds < MinTaskTimeoutSeconds || t.TimeoutSeconds > MaxTaskTimeoutSeconds {
		c.add(CodeInvalidTimeout, t.ID, "timeout_seconds must be %d to %d, got %d", MinTaskTimeoutSeconds, MaxTaskTimeoutSeconds, t.TimeoutSeconds)
	}
	validateRetry(t, c)
	for _, dep := range t.DependsOn {
		switch _, known := g.index[dep]; {
		case dep == t.ID:
			c.add(CodeSelfDependency, t.ID, "task depends on itself")
		case !known:
			c.add(CodeUnknownDependency, t.ID, "depends on unknown task %q", dep)
		}
	}
}

func validateRetry(t *model.TaskDef, c *collector) {
	r := t.Retry
	if r.MaxAttempts < MinMaxAttempts || r.MaxAttempts > MaxMaxAttempts {
		c.add(CodeInvalidRetry, t.ID, "retry.max_attempts must be %d to %d, got %d", MinMaxAttempts, MaxMaxAttempts, r.MaxAttempts)
	}
	if r.Strategy != model.StrategyFixed && r.Strategy != model.StrategyExponential {
		c.add(CodeInvalidRetry, t.ID, "retry.strategy must be %q or %q, got %q", model.StrategyFixed, model.StrategyExponential, r.Strategy)
	}
	if r.InitialDelayMS < 0 || r.InitialDelayMS > MaxRetryDelayMS {
		c.add(CodeInvalidRetry, t.ID, "retry.initial_delay_ms must be 0 to %d, got %d", MaxRetryDelayMS, r.InitialDelayMS)
	}
	if r.MaxDelayMS < r.InitialDelayMS || r.MaxDelayMS > MaxRetryDelayMS {
		c.add(CodeInvalidRetry, t.ID, "retry.max_delay_ms must be from initial_delay_ms (%d) to %d, got %d", r.InitialDelayMS, MaxRetryDelayMS, r.MaxDelayMS)
	}
	if !(r.Multiplier >= 1) { // also rejects NaN
		c.add(CodeInvalidRetry, t.ID, "retry.multiplier must be at least 1, got %g", r.Multiplier)
	}
	if !(r.Jitter >= 0 && r.Jitter <= 1) {
		c.add(CodeInvalidRetry, t.ID, "retry.jitter must be 0 to 1, got %g", r.Jitter)
	}
	if slices.ContainsFunc(r.NonRetryableErrors, func(s string) bool { return strings.TrimSpace(s) == "" }) {
		c.add(CodeInvalidRetry, t.ID, "retry.non_retryable_errors must not contain blank codes")
	}
}

func (v Validator) validateRunIf(def *model.Definition, t *model.TaskDef, c *collector) {
	switch t.RunIf {
	case model.RunIfOnSuccess, model.RunIfAlways:
	case model.RunIfOnFailure:
		if len(t.DependsOn) == 0 {
			c.add(CodeInvalidRunIf, t.ID, "run_if %q needs at least one dependency", t.RunIf)
		}
	case model.RunIfExpr:
		v.validateExpr(def, t, c)
		return
	default:
		c.add(CodeInvalidRunIf, t.ID, "run_if must be on_success, on_failure, always or expr, got %q", t.RunIf)
	}
	if t.RunIfExpr != "" {
		c.add(CodeInvalidRunIf, t.ID, "run_if_expr is only allowed with run_if %q", model.RunIfExpr)
	}
}

func (v Validator) validateExpr(def *model.Definition, t *model.TaskDef, c *collector) {
	if strings.TrimSpace(t.RunIfExpr) == "" {
		c.add(CodeEmptyExpression, t.ID, "run_if %q needs a non-empty run_if_expr", model.RunIfExpr)
		return
	}
	if v.ValidateExpr != nil {
		if err := v.ValidateExpr(t.RunIfExpr); err != nil {
			c.add(CodeInvalidRunIf, t.ID, "run_if_expr is invalid: %v", err)
		}
	}
	refs := v.Refs
	if refs == nil {
		refs = defaultRefs
	}
	ancestors := Ancestors(def, t.ID)
	for _, ref := range refs(t.RunIfExpr) {
		if !slices.Contains(ancestors, ref) {
			c.add(CodeInvalidRunIf, t.ID, "run_if_expr refers to %q, which is not an ancestor of this task", ref)
		}
	}
}

// defaultRefs finds each "tasks.<id>" in the expression text (00E §3.4 grammar), once each.
func defaultRefs(expr string) []string {
	var out []string
	for _, m := range exprRefPattern.FindAllStringSubmatch(expr, -1) {
		if !slices.Contains(out, m[1]) {
			out = append(out, m[1])
		}
	}
	return out
}
