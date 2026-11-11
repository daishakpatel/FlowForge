package statemachine

import (
	"errors"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/daishakpatel/flowforge/internal/workflow/model"
)

func view(key string, status model.TaskStatus, runIf model.RunIf, deps ...string) TaskView {
	return TaskView{Key: key, Status: status, RunIf: runIf, DependsOn: deps}
}

type evalFunc func(string, []TaskView) (bool, error)

func (f evalFunc) Evaluate(expr string, deps []TaskView) (bool, error) { return f(expr, deps) }

// TestEvaluateReadiness_AllCombinations walks every pair of dependency statuses for each
// non-expression run_if and checks the decision against 00E §3.4, restated independently here.
func TestEvaluateReadiness_AllCombinations(t *testing.T) {
	t.Parallel()
	terminal := map[model.TaskStatus]bool{model.TaskSucceeded: true, model.TaskFailed: true, model.TaskTimedOut: true, model.TaskCancelled: true, model.TaskSkipped: true}
	failed := func(s model.TaskStatus) bool { return s == model.TaskFailed || s == model.TaskTimedOut }
	for _, runIf := range []model.RunIf{model.RunIfOnSuccess, model.RunIfOnFailure, model.RunIfAlways} {
		for _, s1 := range model.TaskStatuses() {
			for _, s2 := range model.TaskStatuses() {
				var want ReadinessDecision
				switch {
				case !terminal[s1] || !terminal[s2]:
					want = Wait
				case runIf == model.RunIfAlways:
					want = Ready
				case runIf == model.RunIfOnSuccess && s1 == model.TaskSucceeded && s2 == model.TaskSucceeded:
					want = Ready
				case runIf == model.RunIfOnFailure && (failed(s1) || failed(s2)):
					want = Ready
				default:
					want = Skip
				}
				task := view("t", model.TaskPending, runIf, "a", "b")
				got, err := EvaluateReadiness(task, []TaskView{view("a", s1, model.RunIfOnSuccess), view("b", s2, model.RunIfOnSuccess)}, nil)
				if err != nil || got != want {
					t.Errorf("%s with deps %s,%s: %s (%v), want %s", runIf, s1, s2, got, err, want)
				}
			}
		}
	}
}

func TestEvaluateReadiness_EdgeCases(t *testing.T) {
	t.Parallel()
	succeeded := view("a", model.TaskSucceeded, model.RunIfOnSuccess)
	for name, tc := range map[string]struct {
		task TaskView
		deps []TaskView
		eval ExprEvaluator
		want ReadinessDecision
		err  bool
	}{
		"no deps, on_success":          {view("t", model.TaskPending, model.RunIfOnSuccess), nil, nil, Ready, false},
		"no deps, on_failure":          {view("t", model.TaskPending, model.RunIfOnFailure), nil, nil, Skip, false},
		"no deps, always":              {view("t", model.TaskPending, model.RunIfAlways), nil, nil, Ready, false},
		"not pending":                  {view("t", model.TaskReady, model.RunIfOnSuccess, "a"), []TaskView{succeeded}, nil, Wait, false},
		"missing dependency":           {view("t", model.TaskPending, model.RunIfOnSuccess, "a", "b"), []TaskView{succeeded}, nil, Wait, true},
		"unknown run_if":               {view("t", model.TaskPending, "sometimes", "a"), []TaskView{succeeded}, nil, Wait, true},
		"expr with nil evaluator":      {view("t", model.TaskPending, model.RunIfExpr, "a"), []TaskView{succeeded}, nil, Wait, true},
		"expr true":                    {view("t", model.TaskPending, model.RunIfExpr, "a"), []TaskView{succeeded}, evalFunc(func(string, []TaskView) (bool, error) { return true, nil }), Ready, false},
		"expr false":                   {view("t", model.TaskPending, model.RunIfExpr, "a"), []TaskView{succeeded}, evalFunc(func(string, []TaskView) (bool, error) { return false, nil }), Skip, false},
		"expr error":                   {view("t", model.TaskPending, model.RunIfExpr, "a"), []TaskView{succeeded}, evalFunc(func(string, []TaskView) (bool, error) { return false, errors.New("bad") }), Wait, true},
		"expr waits for deps first":    {view("t", model.TaskPending, model.RunIfExpr, "a"), []TaskView{view("a", model.TaskRunning, model.RunIfOnSuccess)}, nil, Wait, false},
		"extra deps given are ignored": {view("t", model.TaskPending, model.RunIfOnSuccess, "a"), []TaskView{succeeded, view("z", model.TaskFailed, model.RunIfOnSuccess)}, nil, Ready, false},
	} {
		got, err := EvaluateReadiness(tc.task, tc.deps, tc.eval)
		if got != tc.want || (err != nil) != tc.err {
			t.Errorf("%s: %s, %v; want %s, error %v", name, got, err, tc.want, tc.err)
		}
	}
	_, err := EvaluateReadiness(view("t", model.TaskPending, model.RunIfExpr, "a"), []TaskView{succeeded}, nil)
	if !errors.Is(err, ErrNoEvaluator) {
		t.Errorf("nil evaluator error = %v, want ErrNoEvaluator", err)
	}
	var gotExpr string
	var gotDeps int
	task := view("t", model.TaskPending, model.RunIfExpr, "a")
	task.RunIfExpr = `tasks.a.result.ok`
	_, _ = EvaluateReadiness(task, []TaskView{succeeded}, evalFunc(func(e string, d []TaskView) (bool, error) { gotExpr, gotDeps = e, len(d); return true, nil }))
	if gotExpr != task.RunIfExpr || gotDeps != 1 {
		t.Errorf("evaluator got %q with %d deps", gotExpr, gotDeps)
	}
}

func TestDeriveRunStatus(t *testing.T) {
	t.Parallel()
	ok, fail := model.TaskSucceeded, model.TaskFailed
	tests := []struct {
		name   string
		tasks  []TaskView
		policy model.OnFailure
		want   model.RunStatus
	}{
		{"all succeeded", []TaskView{view("a", ok, model.RunIfOnSuccess), view("b", ok, model.RunIfOnSuccess, "a")}, model.OnFailureFail, model.RunSucceeded},
		{"still running", []TaskView{view("a", ok, model.RunIfOnSuccess), view("b", model.TaskRetryWait, model.RunIfOnSuccess, "a")}, model.OnFailureFail, model.RunRunning},
		{"pending task", []TaskView{view("a", fail, model.RunIfOnSuccess), view("b", model.TaskPending, model.RunIfOnFailure, "a")}, model.OnFailureFail, model.RunRunning},
		{"unhandled failure", []TaskView{view("a", fail, model.RunIfOnSuccess), view("b", model.TaskSkipped, model.RunIfOnSuccess, "a")}, model.OnFailureFail, model.RunFailed},
		{"unhandled timeout", []TaskView{view("a", model.TaskTimedOut, model.RunIfOnSuccess)}, model.OnFailureFail, model.RunFailed},
		{"handled by on_failure branch", []TaskView{
			view("check", fail, model.RunIfOnSuccess), view("publish", model.TaskSkipped, model.RunIfOnSuccess, "check"),
			view("investigate", ok, model.RunIfOnFailure, "check")}, model.OnFailureFail, model.RunSucceeded},
		{"handled by always", []TaskView{view("a", model.TaskTimedOut, model.RunIfOnSuccess), view("cleanup", ok, model.RunIfAlways, "a")}, model.OnFailureFail, model.RunSucceeded},
		{"handled by expr", []TaskView{view("a", fail, model.RunIfOnSuccess), view("x", ok, model.RunIfExpr, "a")}, model.OnFailureFail, model.RunSucceeded},
		{"on_success dependent does not handle", []TaskView{view("a", fail, model.RunIfOnSuccess), view("b", ok, model.RunIfOnSuccess, "a")}, model.OnFailureFail, model.RunFailed},
		{"handler failed", []TaskView{view("a", fail, model.RunIfOnSuccess), view("h", fail, model.RunIfOnFailure, "a")}, model.OnFailureFail, model.RunFailed},
		{"handler skipped", []TaskView{view("a", fail, model.RunIfOnSuccess), view("h", model.TaskSkipped, model.RunIfOnFailure, "a")}, model.OnFailureFail, model.RunFailed},
		{"needs_attention policy", []TaskView{view("a", fail, model.RunIfOnSuccess)}, model.OnFailureNeedsAttention, model.RunNeedsAttention},
		{"needs_attention policy, all fine", []TaskView{view("a", ok, model.RunIfOnSuccess)}, model.OnFailureNeedsAttention, model.RunSucceeded},
		{"skipped tasks only", []TaskView{view("a", ok, model.RunIfOnSuccess), view("b", model.TaskSkipped, model.RunIfOnFailure, "a")}, model.OnFailureFail, model.RunSucceeded},
		{"cancelled task", []TaskView{view("a", model.TaskCancelled, model.RunIfOnSuccess)}, model.OnFailureFail, model.RunFailed},
		{"no tasks", nil, model.OnFailureFail, model.RunSucceeded},
	}
	for _, tt := range tests {
		if got := DeriveRunStatus(tt.tasks, tt.policy); got != tt.want {
			t.Errorf("%s: %s, want %s", tt.name, got, tt.want)
		}
	}
}

func TestNextRetryDelay_ExactSchedules(t *testing.T) {
	t.Parallel()
	exp := model.RetryPolicy{Strategy: model.StrategyExponential, InitialDelayMS: 1000, MaxDelayMS: 60000, Multiplier: 2}
	fixed := model.RetryPolicy{Strategy: model.StrategyFixed, InitialDelayMS: 1500, MaxDelayMS: 60000, Multiplier: 2}
	sec := time.Second
	for i, want := range []time.Duration{sec, 2 * sec, 4 * sec, 8 * sec, 16 * sec, 32 * sec, 60 * sec, 60 * sec} {
		if got := NextRetryDelay(exp, i+1, nil); got != want {
			t.Errorf("exponential attempt %d: %v, want %v", i+1, got, want)
		}
		if got := NextRetryDelay(fixed, i+1, seeded(1, 2)); got != 1500*time.Millisecond {
			t.Errorf("fixed attempt %d: %v", i+1, got)
		}
	}
	if NextRetryDelay(exp, 0, nil) != sec || NextRetryDelay(exp, -3, nil) != sec {
		t.Error("attempt numbers below 1 count as 1")
	}
	if got := NextRetryDelay(exp, 10_000, nil); got != time.Minute {
		t.Errorf("huge attempt number overflows instead of capping: %v", got)
	}
	zero := exp
	zero.InitialDelayMS = 0
	if got := NextRetryDelay(zero, 50, seeded(1, 2)); got != 0 {
		t.Errorf("zero initial delay: %v", got)
	}
	m15 := model.RetryPolicy{Strategy: model.StrategyExponential, InitialDelayMS: 1000, MaxDelayMS: 10000, Multiplier: 1.5}
	if got := NextRetryDelay(m15, 3, nil); got != 2250*time.Millisecond {
		t.Errorf("multiplier 1.5, attempt 3: %v, want 2.25s", got)
	}
}

// TestNextRetryDelay_JitterBounds samples 10 000 delays per attempt and checks the bounds of
// task 02.02 §5 hold; the seed is fixed and printed on failure.
func TestNextRetryDelay_JitterBounds(t *testing.T) {
	t.Parallel()
	const seed1, seed2 = 2026, 1107
	rng := seeded(seed1, seed2)
	p := model.RetryPolicy{Strategy: model.StrategyExponential, InitialDelayMS: 1000, MaxDelayMS: 60000, Multiplier: 2, Jitter: 0.2}
	ceiling := time.Duration(float64(p.MaxDelay()) * 1.2)
	for attempt := 1; attempt <= 10; attempt++ {
		base := min(time.Duration(1<<(attempt-1))*time.Second, p.MaxDelay())
		lo, hi := time.Duration(float64(base)*0.8), time.Duration(float64(base)*1.2)
		var below, above bool
		for range 10_000 {
			d := NextRetryDelay(p, attempt, rng)
			if d < lo || d > hi || d < 0 || d > ceiling {
				t.Fatalf("seed (%d,%d) attempt %d: %v outside [%v, %v]", seed1, seed2, attempt, d, lo, hi)
			}
			below, above = below || d < base, above || d > base
		}
		if !below || !above {
			t.Errorf("seed (%d,%d) attempt %d: jitter never went both ways", seed1, seed2, attempt)
		}
	}
	a := NextRetryDelay(p, 3, seeded(7, 7))
	if b := NextRetryDelay(p, 3, seeded(7, 7)); a != b {
		t.Errorf("same seed, different delays: %v vs %v", a, b)
	}
	if got := NextRetryDelay(p, 3, nil); got != 4*time.Second {
		t.Errorf("nil rng should mean no jitter: %v", got)
	}
}

// seeded returns a deterministic RNG; tests need reproducible jitter (00C §8.1), not security.
func seeded(a, b uint64) *rand.Rand {
	return rand.New(rand.NewPCG(a, b)) //nolint:gosec // G404: deterministic test randomness by design
}
