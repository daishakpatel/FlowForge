package template

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func scope() Scope {
	return Scope{
		RunInput: json.RawMessage(`{"symbols":["AAPL","MSFT"],"label":"hourly","n":3,"cfg":{"deep":{"x":true}}}`),
		Tasks: map[string]Task{
			"fetch":   {Status: "SUCCEEDED", Result: json.RawMessage(`{"window_start":"2026-11-15T10:00:00Z","rows":[{"px":101.5},{"px":102}],"ok":false,"none":null}`)},
			"skipped": {Status: "SKIPPED"},
			"nulled":  {Status: "FAILED", Result: json.RawMessage(`null`)},
		},
	}
}

func TestResolve_Table(t *testing.T) {
	tests := []struct {
		name, config, want string
	}{
		{"no placeholders unchanged", `{"a": 1,  "b":"x"}`, `{"a": 1,  "b":"x"}`},
		{"whole value keeps array", `{"s":"${run.input.symbols}"}`, `{"s":["AAPL","MSFT"]}`},
		{"whole value keeps number", `{"n":"${run.input.n}","px":"${tasks.fetch.result.rows[0].px}"}`, `{"n":3,"px":101.5}`},
		{"whole value keeps bool", `{"ok":"${tasks.fetch.result.ok}"}`, `{"ok":false}`},
		{"whole value keeps object", `{"c":"${run.input.cfg}"}`, `{"c":{"deep":{"x":true}}}`},
		{"whole value keeps null", `{"z":"${tasks.fetch.result.none}"}`, `{"z":null}`},
		{"whole input", `{"all":"${run.input}"}`, `{"all":{"cfg":{"deep":{"x":true}},"label":"hourly","n":3,"symbols":["AAPL","MSFT"]}}`},
		{"spaces inside braces", `{"l":"${ run.input.label }"}`, `{"l":"hourly"}`},
		{"interpolation", `{"note":"run ${run.input.label} n=${run.input.n} ok=${tasks.fetch.result.ok}"}`, `{"note":"run hourly n=3 ok=false"}`},
		{"interpolated object is JSON", `{"note":"syms=${run.input.symbols}"}`, `{"note":"syms=[\"AAPL\",\"MSFT\"]"}`},
		{"status", `{"st":"${tasks.skipped.status}"}`, `{"st":"SKIPPED"}`},
		{"nested objects and arrays", `{"a":[{"b":"${tasks.fetch.result.window_start}"},["${run.input.n}",7]]}`, `{"a":[{"b":"2026-11-15T10:00:00Z"},[3,7]]}`},
		{"index chain", `{"x":"${run.input.cfg.deep.x}","y":"${tasks.fetch.result.rows[1].px}"}`, `{"x":true,"y":102}`},
		{"keys are not templated", `{"${run.input.label}":1}`, `{"${run.input.label}":1}`},
		{"no html escaping", `{"q":"<${run.input.label}>&"}`, `{"q":"<hourly>&"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Resolver{}.Resolve(json.RawMessage(tt.config), scope())
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != tt.want {
				t.Errorf("Resolve(%s)\n got  %s\n want %s", tt.config, got, tt.want)
			}
		})
	}
}

func TestResolve_Errors(t *testing.T) {
	tests := []struct {
		name, config, path, reason string
	}{
		{"missing key", `{"a":"${run.input.nope}"}`, "run.input.nope", "path not found"},
		{"index out of range", `{"a":"${tasks.fetch.result.rows[2]}"}`, "tasks.fetch.result.rows[2]", "path not found"},
		{"index on object", `{"a":"${run.input.cfg[0]}"}`, "run.input.cfg[0]", "path not found"},
		{"result of a task without one", `{"a":"${tasks.skipped.result}"}`, "tasks.skipped.result", "path not found"},
		{"null result", `{"a":"${tasks.nulled.result.x}"}`, "tasks.nulled.result.x", "path not found"},
		{"non-ancestor", `{"a":"x ${tasks.other.status}"}`, "tasks.other.status", "not an ancestor"},
		{"bad root", `{"a":"${env.HOME}"}`, "env.HOME", "is not run.input"},
		{"status with path", `{"a":"${tasks.fetch.status.x}"}`, "tasks.fetch.status.x", "is not run.input"},
		{"bad index", `{"a":"${run.input.symbols[-1]}"}`, "run.input.symbols[-1]", "bad index"},
		{"unterminated", `{"a":"${run.input.n"}`, "run.input.n", "unterminated"},
		{"empty", `{"a":"${}"}`, "", "empty reference"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Resolver{}.Resolve(json.RawMessage(tt.config), scope())
			var te *TemplateError
			if !errors.As(err, &te) {
				t.Fatalf("err = %v; want a *TemplateError", err)
			}
			if te.Path != tt.path || !strings.Contains(te.Reason, tt.reason) {
				t.Errorf("TemplateError{%q, %q}; want path %q, reason containing %q", te.Path, te.Reason, tt.path, tt.reason)
			}
		})
	}
}

func TestResolve_SizeCap(t *testing.T) {
	big := strings.Repeat("x", MaxInputBytes)
	s := Scope{RunInput: json.RawMessage(`{"big":"` + big + `"}`)}
	if _, err := (Resolver{}).Resolve(json.RawMessage(`{"a":"${run.input.big}"}`), s); !errors.Is(err, ErrTooLarge) {
		t.Errorf("oversize resolved input: err = %v; want ErrTooLarge", err)
	}
	if _, err := (Resolver{}).Resolve(json.RawMessage(`{"a":"`+big+`"}`), Scope{}); !errors.Is(err, ErrTooLarge) {
		t.Errorf("oversize config without placeholders: err = %v; want ErrTooLarge", err)
	}
	fits := strings.Repeat("x", MaxInputBytes-20)
	s = Scope{RunInput: json.RawMessage(`{"big":"` + fits + `"}`)}
	if _, err := (Resolver{}).Resolve(json.RawMessage(`{"a":"${run.input.big}"}`), s); err != nil {
		t.Errorf("input under the cap: %v", err)
	}
}

func TestRefs(t *testing.T) {
	refs, err := Refs(json.RawMessage(`{"a":"${run.input.x} and ${tasks.fetch.status}","b":["${tasks.proc.result.rows[0]}"],"c":1}`))
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, r := range refs {
		got = append(got, r.Task)
	}
	if strings.Join(got, ",") != ",fetch,proc" {
		t.Errorf("Refs tasks = %q; want run input, fetch, proc", got)
	}
	if _, err := Refs(json.RawMessage(`{"a":"${tasks.x}"}`)); err == nil {
		t.Error("Refs accepted ${tasks.x}")
	}
	if refs, err := Refs(json.RawMessage(`{"a":1}`)); refs != nil || err != nil {
		t.Errorf("Refs without placeholders = %v, %v", refs, err)
	}
}
