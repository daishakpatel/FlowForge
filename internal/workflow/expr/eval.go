package expr

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/daishakpatel/flowforge/internal/workflow/statemachine"
	"github.com/daishakpatel/flowforge/internal/workflow/template"
)

// Eval evaluates the expression against s and returns its boolean value.
//
//   - A path that does not exist, or a result path of a task without a result, is null. A task
//     outside s (not an ancestor) is an error.
//   - == and != compare any two values: numbers by value, and values of different types are
//     unequal, so x == null is how to test for a missing value.
//   - < <= > >= need two numbers or two strings; anything else is an error.
//   - && || ! need booleans; && and || short-circuit, so the unevaluated side can't fail.
//   - The whole expression must be a boolean.
func (x *Expr) Eval(s template.Scope) (bool, error) {
	v, err := (&evaluator{r: template.NewReader(s)}).eval(x.root)
	if err != nil {
		return false, err
	}
	b, ok := v.(bool)
	if !ok {
		return false, fmt.Errorf("expression is %s, not a boolean", typeName(v))
	}
	return b, nil
}

type evaluator struct{ r *template.Reader }

func (e *evaluator) eval(n node) (any, error) {
	switch n := n.(type) {
	case lit:
		return n.v, nil
	case path:
		return e.path(n)
	case not:
		b, err := e.boolean(n.x, "!")
		return !b, err
	case binary:
		return e.binary(n)
	}
	return nil, fmt.Errorf("unknown node %T", n)
}

func (e *evaluator) path(p path) (any, error) {
	v, found, err := e.r.Lookup(p.ref)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", p.text, err)
	}
	if !found {
		return nil, nil
	}
	if num, ok := v.(json.Number); ok {
		f, err := num.Float64()
		if err != nil {
			return nil, fmt.Errorf("%s: number %s: %w", p.text, num, err)
		}
		return f, nil
	}
	return v, nil
}

func (e *evaluator) boolean(n node, op string) (bool, error) {
	v, err := e.eval(n)
	if err != nil {
		return false, err
	}
	b, ok := v.(bool)
	if !ok {
		return false, fmt.Errorf("%s needs a boolean, got %s", op, typeName(v))
	}
	return b, nil
}

func (e *evaluator) binary(n binary) (any, error) {
	if n.op == "&&" || n.op == "||" {
		l, err := e.boolean(n.l, n.op)
		if err != nil || l == (n.op == "||") {
			return l, err // short-circuit
		}
		return e.boolean(n.r, n.op)
	}
	l, err := e.eval(n.l)
	if err != nil {
		return nil, err
	}
	r, err := e.eval(n.r)
	if err != nil {
		return nil, err
	}
	switch n.op {
	case "==":
		return equal(l, r), nil
	case "!=":
		return !equal(l, r), nil
	}
	return order(n.op, l, r)
}

func equal(l, r any) bool {
	if typeName(l) != typeName(r) {
		return false
	}
	return reflect.DeepEqual(l, r)
}

func order(op string, l, r any) (bool, error) {
	var c int
	switch lv := l.(type) {
	case float64:
		rv, ok := r.(float64)
		if !ok {
			return false, fmt.Errorf("%s compares a number with %s", op, typeName(r))
		}
		c = cmp(lv < rv, lv > rv)
	case string:
		rv, ok := r.(string)
		if !ok {
			return false, fmt.Errorf("%s compares a string with %s", op, typeName(r))
		}
		c = cmp(lv < rv, lv > rv)
	default:
		return false, fmt.Errorf("%s needs numbers or strings, got %s", op, typeName(l))
	}
	switch op {
	case "<":
		return c < 0, nil
	case "<=":
		return c <= 0, nil
	case ">":
		return c > 0, nil
	}
	return c >= 0, nil
}

func cmp(less, greater bool) int {
	switch {
	case less:
		return -1
	case greater:
		return 1
	}
	return 0
}

func typeName(v any) string {
	switch v.(type) {
	case nil:
		return "null"
	case bool:
		return "a boolean"
	case float64:
		return "a number"
	case string:
		return "a string"
	case []any:
		return "an array"
	case map[string]any:
		return "an object"
	}
	return fmt.Sprintf("%T", v)
}

// Evaluator implements statemachine.ExprEvaluator: it evaluates a run_if_expr against the
// tasks it is given, which must include every task the expression refers to (the scheduler passes
// the task's ancestors), and RunInput for run.input paths.
type Evaluator struct {
	RunInput json.RawMessage
}

var _ statemachine.ExprEvaluator = Evaluator{}

// Evaluate implements statemachine.ExprEvaluator.
func (e Evaluator) Evaluate(expr string, deps []statemachine.TaskView) (bool, error) {
	x, err := Parse(expr)
	if err != nil {
		return false, err
	}
	s := template.Scope{RunInput: e.RunInput, Tasks: make(map[string]template.Task, len(deps))}
	for _, d := range deps {
		s.Tasks[d.Key] = template.Task{Status: string(d.Status), Result: d.Result}
	}
	return x.Eval(s)
}

// ForRun returns e evaluating run.input paths against input (the scheduler binds each run's).
func (e Evaluator) ForRun(input json.RawMessage) statemachine.ExprEvaluator {
	e.RunInput = input
	return e
}

// Validate checks expr's syntax; it is the dag.ExprValidator hook.
func Validate(expr string) error {
	_, err := Parse(expr)
	return err
}

// Refs returns the task ids expr refers to, or nil when it does not parse (Validate reports
// that); it is the dag.ExprRefs hook.
func Refs(expr string) []string {
	x, err := Parse(expr)
	if err != nil {
		return nil
	}
	return x.Refs()
}
