package dag

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/daishakpatel/flowforge/internal/workflow/model"
)

// task returns a valid task with the given dependencies.
func task(id string, deps ...string) model.TaskDef {
	return model.TaskDef{
		ID: id, Type: "noop", Queue: "default", DependsOn: deps, TimeoutSeconds: 60, RunIf: model.RunIfOnSuccess,
		Retry: model.RetryPolicy{MaxAttempts: 1, Strategy: model.StrategyFixed, InitialDelayMS: 1000, MaxDelayMS: 60000, Multiplier: 2},
	}
}

// def returns a valid workflow around tasks.
func def(tasks ...model.TaskDef) *model.Definition {
	return &model.Definition{Name: "wf", Version: 1, OnFailure: model.OnFailureFail, Tasks: tasks}
}

func with(t model.TaskDef, f func(*model.TaskDef)) model.TaskDef { f(&t); return t }

// chain returns n tasks t0..t(n-1), each depending on the one before.
func chain(n int) []model.TaskDef {
	out := make([]model.TaskDef, n)
	for i := range n {
		if i == 0 {
			out[i] = task("t0")
		} else {
			out[i] = task(fmt.Sprintf("t%d", i), fmt.Sprintf("t%d", i-1))
		}
	}
	return out
}

type issueKey struct {
	code IssueCode
	task string
}

func keys(issues []ValidationIssue) []issueKey {
	out := make([]issueKey, len(issues))
	for i, is := range issues {
		out[i] = issueKey{is.Code, is.TaskID}
	}
	return out
}

func TestValidate_Table(t *testing.T) {
	t.Parallel()
	long := strings.Repeat("a", 63)
	tests := []struct {
		name string
		def  *model.Definition
		want []issueKey // exact, in the order Validate must return them
	}{
		{"valid single task", def(task("a")), nil},
		{"valid diamond", def(task("a"), task("b", "a"), task("c", "a"), task("d", "b", "c")), nil},
		{"duplicate id", def(task("a"), task("a")), []issueKey{{CodeDuplicateTaskID, "a"}}},
		{"id with upper case", def(task("Fetch")), []issueKey{{CodeInvalidTaskID, "Fetch"}}},
		{"id starting with a digit", def(task("1st")), []issueKey{{CodeInvalidTaskID, "1st"}}},
		{"id of 63 chars", def(task(long)), nil},
		{"id of 64 chars", def(task(long + "a")), []issueKey{{CodeInvalidTaskID, long + "a"}}},
		{"unknown dependency", def(task("a", "ghost")), []issueKey{{CodeUnknownDependency, "a"}}},
		{"self dependency is not also a cycle", def(task("a", "a")), []issueKey{{CodeSelfDependency, "a"}}},
		{"2-cycle", def(task("a", "b"), task("b", "a")), []issueKey{{CodeCycle, "a"}}},
		{"3-cycle", def(task("c", "b"), task("b", "a"), task("a", "c")), []issueKey{{CodeCycle, "a"}}},
		{"cycle in a disconnected component", def(task("x"), task("y", "x"), task("p", "q"), task("q", "p")), []issueKey{{CodeCycle, "p"}}},
		{"500 tasks", def(chain(500)...), nil},
		{"501 tasks", def(chain(501)...), []issueKey{{CodeTooManyTasks, ""}}},
		{"max_attempts 0", def(with(task("a"), func(t *model.TaskDef) { t.Retry.MaxAttempts = 0 })), []issueKey{{CodeInvalidRetry, "a"}}},
		{"max_attempts 20 and 21", def(with(task("a"), func(t *model.TaskDef) { t.Retry.MaxAttempts = 20 }), with(task("b"), func(t *model.TaskDef) { t.Retry.MaxAttempts = 21 })), []issueKey{{CodeInvalidRetry, "b"}}},
		{"multiplier below 1", def(with(task("a"), func(t *model.TaskDef) { t.Retry.Multiplier = 0.5 })), []issueKey{{CodeInvalidRetry, "a"}}},
		{"multiplier exactly 1", def(with(task("a"), func(t *model.TaskDef) { t.Retry.Multiplier = 1 })), nil},
		{"jitter bounds", def(with(task("a"), func(t *model.TaskDef) { t.Retry.Jitter = 1 }), with(task("b"), func(t *model.TaskDef) { t.Retry.Jitter = 1.5 }), with(task("c"), func(t *model.TaskDef) { t.Retry.Jitter = -0.1 })), []issueKey{{CodeInvalidRetry, "b"}, {CodeInvalidRetry, "c"}}},
		{"max delay below initial", def(with(task("a"), func(t *model.TaskDef) { t.Retry.MaxDelayMS = 500 })), []issueKey{{CodeInvalidRetry, "a"}}},
		{"negative initial delay", def(with(task("a"), func(t *model.TaskDef) { t.Retry.InitialDelayMS = -1 })), []issueKey{{CodeInvalidRetry, "a"}}},
		{"delay above 24 h", def(with(task("a"), func(t *model.TaskDef) { t.Retry.MaxDelayMS = MaxRetryDelayMS + 1 })), []issueKey{{CodeInvalidRetry, "a"}}},
		{"unknown strategy", def(with(task("a"), func(t *model.TaskDef) { t.Retry.Strategy = "linear" })), []issueKey{{CodeInvalidRetry, "a"}}},
		{"blank non-retryable code", def(with(task("a"), func(t *model.TaskDef) { t.Retry.NonRetryableErrors = []string{"X", " "} })), []issueKey{{CodeInvalidRetry, "a"}}},
		{"timeout bounds", def(with(task("a"), func(t *model.TaskDef) { t.TimeoutSeconds = 1 }), with(task("b"), func(t *model.TaskDef) { t.TimeoutSeconds = 86400 }), with(task("c"), func(t *model.TaskDef) { t.TimeoutSeconds = 0 }), with(task("d"), func(t *model.TaskDef) { t.TimeoutSeconds = 86401 })), []issueKey{{CodeInvalidTimeout, "c"}, {CodeInvalidTimeout, "d"}}},
		{"missing and blank type", def(with(task("a"), func(t *model.TaskDef) { t.Type = "" }), with(task("b"), func(t *model.TaskDef) { t.Type = "  " }), with(task("c"), func(t *model.TaskDef) { t.Type = strings.Repeat("x", 129) })), []issueKey{{CodeMissingType, "a"}, {CodeMissingType, "b"}, {CodeMissingType, "c"}}},
		{"invalid queue", def(with(task("a"), func(t *model.TaskDef) { t.Queue = "Market Data" })), []issueKey{{CodeInvalidQueue, "a"}}},
		{"unknown run_if", def(with(task("a"), func(t *model.TaskDef) { t.RunIf = "sometimes" })), []issueKey{{CodeInvalidRunIf, "a"}}},
		{"expr without expression", def(task("a"), with(task("b", "a"), func(t *model.TaskDef) { t.RunIf = model.RunIfExpr; t.RunIfExpr = "  " })), []issueKey{{CodeEmptyExpression, "b"}}},
		{"expression without run_if expr", def(with(task("a"), func(t *model.TaskDef) { t.RunIfExpr = "true" })), []issueKey{{CodeInvalidRunIf, "a"}}},
		{"on_failure on a root task", def(with(task("a"), func(t *model.TaskDef) { t.RunIf = model.RunIfOnFailure })), []issueKey{{CodeInvalidRunIf, "a"}}},
		{"expr referring to an ancestor", def(task("a"), task("b", "a"), with(task("c", "b"), func(t *model.TaskDef) {
			t.RunIf = model.RunIfExpr
			t.RunIfExpr = `tasks.a.status == "SUCCEEDED"`
		})), nil},
		{"expr referring to a non-ancestor", def(task("a"), task("b"), with(task("c", "a"), func(t *model.TaskDef) {
			t.RunIf = model.RunIfExpr
			t.RunIfExpr = `tasks.b.result.ok && tasks.ghost.status == "FAILED"`
		})), []issueKey{{CodeInvalidRunIf, "c"}, {CodeInvalidRunIf, "c"}}},
		{"workflow fields", &model.Definition{Name: "", Version: 0, TimeoutSeconds: -1, OnFailure: "retry",
			Concurrency: model.Concurrency{MaxParallelTasks: 501}}, []issueKey{
			{CodeInvalidWorkflowField, ""}, {CodeInvalidWorkflowField, ""}, {CodeInvalidWorkflowField, ""},
			{CodeInvalidWorkflowField, ""}, {CodeInvalidWorkflowField, ""}, {CodeInvalidWorkflowField, ""}}},
		{"workflow timeout upper bound", func() *model.Definition {
			d := def(task("a"))
			d.TimeoutSeconds = MaxWorkflowTimeoutSeconds + 1
			return d
		}(), []issueKey{{CodeInvalidWorkflowField, ""}}},
		{"workflow limits at their maximum", func() *model.Definition {
			d := def(task("a"))
			d.TimeoutSeconds, d.Concurrency.MaxParallelTasks, d.OnFailure = MaxWorkflowTimeoutSeconds, MaxTasks, model.OnFailureNeedsAttention
			return d
		}(), nil},
		{"nil definition", nil, []issueKey{{CodeInvalidWorkflowField, ""}}},
		{"many issues aggregated in order", def(
			with(task("b", "ghost"), func(t *model.TaskDef) { t.Type = ""; t.Retry.MaxAttempts = 0 }),
			with(task("a", "a"), func(t *model.TaskDef) { t.Queue = "Q" }),
		), []issueKey{{CodeInvalidQueue, "a"}, {CodeSelfDependency, "a"}, {CodeInvalidRetry, "b"}, {CodeMissingType, "b"}, {CodeUnknownDependency, "b"}}},
	}
	if len(tests) < 25 {
		t.Fatalf("want at least 25 cases, have %d", len(tests))
	}
	covered := map[IssueCode]bool{}
	for _, tt := range tests {
		got := Validate(tt.def)
		if !slices.Equal(keys(got), tt.want) {
			t.Errorf("%s:\n got  %v\n want %v\n %+v", tt.name, keys(got), tt.want, got)
		}
		for _, is := range got {
			covered[is.Code] = true
			if is.Message == "" {
				t.Errorf("%s: issue %s has no message", tt.name, is.Code)
			}
		}
	}
	for _, c := range IssueCodes() {
		if !covered[c] {
			t.Errorf("no case produces %s", c)
		}
	}
}

// TestValidate_CyclePathIsRealCycle: the reported path is closed and every step is an actual
// dependency edge.
func TestValidate_CyclePathIsRealCycle(t *testing.T) {
	t.Parallel()
	for name, d := range map[string]*model.Definition{
		"2-cycle":      def(task("a", "b"), task("b", "a")),
		"3-cycle":      def(task("c", "b"), task("b", "a"), task("a", "c")),
		"disconnected": def(task("x"), task("p", "r", "x"), task("q", "p"), task("r", "q"), task("z", "r")),
	} {
		deps := map[string][]string{}
		for _, tk := range d.Tasks {
			deps[tk.ID] = tk.DependsOn
		}
		issues := Validate(d)
		if len(issues) != 1 || issues[0].Code != CodeCycle {
			t.Fatalf("%s: issues = %+v", name, issues)
		}
		path := issues[0].Path
		if len(path) < 3 || path[0] != path[len(path)-1] || issues[0].TaskID != path[0] {
			t.Fatalf("%s: path %v is not closed", name, path)
		}
		for i := 0; i+1 < len(path); i++ {
			if !slices.Contains(deps[path[i]], path[i+1]) {
				t.Errorf("%s: %s does not depend on %s (path %v)", name, path[i], path[i+1], path)
			}
		}
		if !strings.Contains(issues[0].Message, strings.Join(path, " → ")) {
			t.Errorf("%s: message %q lacks the path", name, issues[0].Message)
		}
	}
}

func TestValidate_DeterministicAcrossTaskOrder(t *testing.T) {
	t.Parallel()
	tasks := []model.TaskDef{task("a", "ghost"), task("b", "c"), task("c", "b"), with(task("d"), func(t *model.TaskDef) { t.Queue = "" })}
	reversed := slices.Clone(tasks)
	slices.Reverse(reversed)
	first, second := Validate(def(tasks...)), Validate(def(reversed...))
	if fmt.Sprint(first) != fmt.Sprint(second) {
		t.Errorf("order-dependent result:\n%v\n%v", first, second)
	}
}

func TestValidator_ExprHooks(t *testing.T) {
	t.Parallel()
	d := def(task("a"), with(task("b", "a"), func(t *model.TaskDef) { t.RunIf = model.RunIfExpr; t.RunIfExpr = "a ok" }))
	v := Validator{
		ValidateExpr: func(expr string) error {
			if strings.Contains(expr, "ok") {
				return errors.New("unexpected token ok")
			}
			return nil
		},
		Refs: func(string) []string { return []string{"a", "zzz"} },
	}
	got := v.Validate(d)
	if !slices.Equal(keys(got), []issueKey{{CodeInvalidRunIf, "b"}, {CodeInvalidRunIf, "b"}}) {
		t.Fatalf("issues = %+v", got)
	}
	if !strings.Contains(got[0].Message+got[1].Message, "unexpected token ok") || !strings.Contains(got[0].Message+got[1].Message, `"zzz"`) {
		t.Errorf("messages = %q / %q", got[0].Message, got[1].Message)
	}
}

func TestGraphHelpers(t *testing.T) {
	t.Parallel()
	d := def(task("a"), task("b", "a"), task("c", "a"), task("d", "b", "c"), task("e"))
	if got, err := TopoOrder(d); err != nil || !slices.Equal(got, []string{"a", "e", "b", "c", "d"}) {
		t.Errorf("TopoOrder = %v, %v", got, err)
	}
	if _, err := TopoOrder(def(task("a", "b"), task("b", "a"))); !errors.Is(err, ErrCycle) {
		t.Errorf("TopoOrder of a cycle: %v", err)
	}
	if got := Ancestors(d, "d"); !slices.Equal(got, []string{"a", "b", "c"}) {
		t.Errorf("Ancestors(d) = %v", got)
	}
	if Ancestors(d, "a") != nil || Ancestors(d, "ghost") != nil {
		t.Error("a root and an unknown id have no ancestors")
	}
	if got := Roots(d); !slices.Equal(got, []string{"a", "e"}) {
		t.Errorf("Roots = %v", got)
	}
	ch := Children(d)
	if !slices.Equal(ch["a"], []string{"b", "c"}) || !slices.Equal(ch["b"], []string{"d"}) || len(ch["d"]) != 0 || len(ch) != 5 {
		t.Errorf("Children = %v", ch)
	}
}

// bench500 returns 500-task DAGs of three shapes for the FR-WF-02 timing target (< 100 ms).
func bench500() map[string]*model.Definition {
	wide := []model.TaskDef{task("t0")}
	for i := 1; i < 500; i++ {
		wide = append(wide, task(fmt.Sprintf("t%d", i), "t0"))
	}
	dense := make([]model.TaskDef, 500)
	for i := range 500 {
		var deps []string
		for j := max(0, i-10); j < i; j++ {
			deps = append(deps, fmt.Sprintf("t%d", j))
		}
		dense[i] = task(fmt.Sprintf("t%d", i), deps...)
	}
	return map[string]*model.Definition{"wide": def(wide...), "deep": def(chain(500)...), "dense": def(dense...)}
}

func BenchmarkValidate500(b *testing.B) {
	for name, d := range bench500() {
		b.Run(name, func(b *testing.B) {
			for b.Loop() {
				if issues := Validate(d); len(issues) != 0 {
					b.Fatal(issues)
				}
			}
		})
	}
}
