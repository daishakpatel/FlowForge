package expr

import (
	"encoding/json"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/daishakpatel/flowforge/internal/workflow/model"
	"github.com/daishakpatel/flowforge/internal/workflow/statemachine"
)

var deps = []statemachine.TaskView{
	{Key: "qc", Status: model.TaskFailed},
	{Key: "fetch", Status: model.TaskSucceeded, Result: json.RawMessage(`{"rows":12,"ok":true,"src":"ny","list":[1,2],"none":null,"big":1e400}`)},
	{Key: "skip", Status: model.TaskSkipped},
}

func eval(t *testing.T, s string) (bool, error) {
	t.Helper()
	return Evaluator{RunInput: json.RawMessage(`{"min":10,"env":"prod","flag":false}`)}.Evaluate(s, deps)
}

func TestEval_Table(t *testing.T) {
	tests := []struct {
		expr string
		want bool
	}{
		{`tasks.qc.status == "FAILED"`, true},
		{`tasks.qc.status != 'FAILED'`, false},
		{`tasks.fetch.result.rows > 10`, true},
		{`tasks.fetch.result.rows >= run.input.min && tasks.fetch.result.src == "ny"`, true},
		{`tasks.fetch.result.rows < 12`, false},
		{`tasks.fetch.result.rows <= 12`, true},
		{`tasks.fetch.result.list[1] == 2`, true},
		{`tasks.fetch.result.ok`, true},
		{`"abc" < "abd"`, true},
		{`-1.5e1 < -15.0`, false},
		// precedence: && binds tighter than ||; ! negates the whole comparison.
		{`true || false && false`, true},
		{`(true || false) && false`, false},
		{`!tasks.qc.status == "SUCCEEDED"`, true},
		{`!(1 < 2) || run.input.flag == false`, true},
		{`!run.input.flag && true`, true},
		// null comparisons: missing paths and result-less tasks are null.
		{`tasks.fetch.result.none == null`, true},
		{`tasks.fetch.result.missing == null`, true},
		{`tasks.skip.result == null`, true},
		{`tasks.skip.result.x != null`, false},
		{`null == false`, false},
		{`1 == "1"`, false},
		{`tasks.fetch.result.list == tasks.fetch.result.list`, true},
		// short-circuit: the right side would be a type error.
		{`false && (1 < "a")`, false},
		{`true || tasks.fetch.result.src > 3`, true},
	}
	for _, tt := range tests {
		got, err := eval(t, tt.expr)
		if err != nil || got != tt.want {
			t.Errorf("%s = %v, %v; want %v", tt.expr, got, err, tt.want)
		}
	}
}

func TestEval_Errors(t *testing.T) {
	tests := []struct{ expr, want string }{
		{`1 < "a"`, "compares a number with a string"},
		{`null < 1`, "needs numbers or strings"},
		{`"a" >= true`, "compares a string with a boolean"},
		{`1 && true`, "&& needs a boolean"},
		{`true || "x"`, ""}, // short-circuited: no error
		{`false || "x"`, "|| needs a boolean"},
		{`!1`, "! needs a boolean"},
		{`tasks.fetch.result.rows`, "not a boolean"},
		{`tasks.ghost.status == "FAILED"`, "not an ancestor"},
		{`tasks.fetch.result.big == 1`, "number 1e400"},
	}
	for _, tt := range tests {
		_, err := eval(t, tt.expr)
		switch {
		case tt.want == "" && err != nil:
			t.Errorf("%s: unexpected error %v", tt.expr, err)
		case tt.want != "" && (err == nil || !strings.Contains(err.Error(), tt.want)):
			t.Errorf("%s: err = %v; want it to contain %q", tt.expr, err, tt.want)
		}
	}
}

func TestParse_Errors(t *testing.T) {
	for _, s := range []string{
		``, `   `, `(`, `)`, `1 ==`, `== 1`, `1 == 2 == 3`, `a < b < c`, `(true`, `true)`, `1 = 1`,
		`"open`, `'open`, `"bad \q"`, `1.2.3`, `1e999`, `tasks.x`, `tasks.x.foo`, `run.output.x`,
		`env.HOME == 1`, `tasks.a.status == "x" &`, `!`, `true false`, `#`, `tasks.a.status[0]`,
		strings.Repeat("(", 100) + "true" + strings.Repeat(")", 100),
		strings.Repeat("a", MaxLength+1),
	} {
		if _, err := Parse(s); err == nil {
			t.Errorf("Parse(%.40q) succeeded; want an error", s)
		}
	}
}

func TestRefsAndValidate(t *testing.T) {
	got := Refs(`tasks.a.status == "FAILED" || (tasks.b.result.x > 1 && tasks.a.result.y == run.input.z) || !tasks.c.result.ok`)
	if !slices.Equal(got, []string{"a", "b", "c"}) {
		t.Errorf("Refs = %v; want [a b c]", got)
	}
	if Refs(`tasks.a.status ==`) != nil {
		t.Error("Refs of an invalid expression should be nil")
	}
	if Validate(`tasks.a.status == "FAILED"`) != nil || Validate(`tasks.a.status = 1`) == nil {
		t.Error("Validate disagrees with Parse")
	}
}

func TestEvaluator_ForRun(t *testing.T) {
	e := Evaluator{}.ForRun(json.RawMessage(`{"go":true}`))
	if ok, err := e.Evaluate(`run.input.go`, nil); !ok || err != nil {
		t.Errorf("bound run input: %v, %v", ok, err)
	}
}

// fuzzSeed fixes the generated inputs; quote it when the test fails.
const fuzzSeed = 20261115

// TestExpr_RandomInputsNeverPanic is the task's fuzz requirement as a deterministic test: 50 000
// random token soups and byte strings are parsed and, when they parse, evaluated. Any panic fails
// the test; errors are expected.
func TestExpr_RandomInputsNeverPanic(t *testing.T) {
	rng := rand.New(rand.NewPCG(fuzzSeed, 0)) //nolint:gosec // G404: a fixed seed makes the inputs reproducible; nothing secret
	pieces := []string{
		"tasks.qc.status", "tasks.fetch.result.rows", "tasks.fetch.result.list[1]", "tasks.skip.result.x",
		"run.input.min", "tasks.ghost.status", "true", "false", "null", "1", "-2.5", "1e3", `"FAILED"`, `'x'`,
		"==", "!=", "<", "<=", ">", ">=", "&&", "||", "!", "(", ")", " ", `"`, "[", "]", ".", "-", "e",
	}
	const n = 50_000
	ok := 0
	for i := range n {
		var b strings.Builder
		switch {
		case i%2 == 0: // well-formed, to reach the evaluator
			b.WriteString(randomExpr(rng, pieces[:14], 3))
		case i%5 == 1: // raw bytes
			for range rng.IntN(24) {
				b.WriteByte(byte(rng.IntN(256))) //nolint:gosec // G115: IntN(256) fits a byte
			}
		default:
			for range 1 + rng.IntN(12) {
				b.WriteString(pieces[rng.IntN(len(pieces))])
				if rng.IntN(2) == 0 {
					b.WriteByte(' ')
				}
			}
		}
		if _, err := eval(t, b.String()); err == nil {
			ok++
		}
	}
	t.Logf("seed %d: %d inputs, %d evaluated without error", fuzzSeed, n, ok)
}

// randomExpr builds a grammatical expression from operands, depth levels deep.
func randomExpr(rng *rand.Rand, operands []string, depth int) string {
	if depth == 0 || rng.IntN(3) == 0 {
		return operands[rng.IntN(len(operands))]
	}
	l, r := randomExpr(rng, operands, depth-1), randomExpr(rng, operands, depth-1)
	switch rng.IntN(4) {
	case 0:
		return "!(" + l + ")"
	case 1:
		return "(" + l + ")" + []string{" && ", " || "}[rng.IntN(2)] + "(" + r + ")"
	default:
		return "(" + l + ")" + []string{" == ", " != ", " < ", " <= ", " > ", " >= "}[rng.IntN(6)] + "(" + r + ")"
	}
}

// FuzzEval explores further with `go test -fuzz FuzzEval ./internal/workflow/expr/`.
func FuzzEval(f *testing.F) {
	for _, s := range []string{`tasks.qc.status == "FAILED"`, `!(1 < 2) || true`, `tasks.fetch.result.list[0] >= -1e2`, `"a\"b" != 'c'`} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		_, _ = eval(t, s) // must not panic; errors are fine
	})
}
