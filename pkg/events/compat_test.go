package events_test

import (
	"cmp"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
)

// The schema-compatibility rules (docs/architecture/event-schemas.md, task 05.01): within one
// schema version, a schema may only gain optional properties. schemas/baseline holds the snapshot
// of every published version; a breaking change is a new file (…v2), never an edit of …v1.

const (
	currentDir  = "../../schemas/events"
	baselineDir = "../../schemas/baseline"
)

// annotations are the keywords that never change what validates.
var annotations = map[string]bool{"title": true, "description": true, "examples": true, "$comment": true}

// compatProblems lists every way next breaks consumers or producers written against prev:
//   - a property removed or retyped, a required property removed, made optional or newly added;
//   - an enum that lost a value (adding values is allowed);
//   - any other validation keyword changed at all (pattern, lengths, bounds, format, const,
//     additionalProperties, allOf/if/then payload dispatch, $ref, …): whether such a change only
//     widens is not decided, so it needs a new version.
//
// It walks nested object properties and array items; $ref'd schemas are checked in their own files.
func compatProblems(prev, next map[string]any, at string) []string {
	var out []string
	if pt, nt := typeOf(prev), typeOf(next); pt != nt {
		out = append(out, fmt.Sprintf("%s: type changed from %s to %s", at, pt, nt))
	}
	for _, k := range sortedKeys(union(prev, next)) {
		switch {
		case annotations[k] || k == "type" || k == "required" || k == "properties" || k == "items":
		case k == "enum":
			for _, v := range anys(prev[k]) {
				if !slices.ContainsFunc(anys(next[k]), func(w any) bool { return reflect.DeepEqual(v, w) }) {
					out = append(out, fmt.Sprintf("%s: enum value %v removed", at, v))
				}
			}
			if _, ok := next[k]; ok && prev[k] == nil {
				out = append(out, fmt.Sprintf("%s: enum added", at))
			}
		case !reflect.DeepEqual(prev[k], next[k]):
			out = append(out, fmt.Sprintf("%s: %s changed", at, k))
		}
	}
	pr, nr := strs(prev["required"]), strs(next["required"])
	for _, f := range pr {
		if !slices.Contains(nr, f) {
			out = append(out, fmt.Sprintf("%s: required property %q removed or made optional", at, f))
		}
	}
	for _, f := range nr {
		if !slices.Contains(pr, f) {
			out = append(out, fmt.Sprintf("%s: new required property %q (old producers lack it)", at, f))
		}
	}
	pp, np := obj(prev["properties"]), obj(next["properties"])
	for _, f := range sortedKeys(pp) {
		n, ok := np[f]
		if !ok {
			out = append(out, fmt.Sprintf("%s/%s: property removed", at, f))
			continue
		}
		out = append(out, compatProblems(obj(pp[f]), obj(n), at+"/"+f)...)
	}
	if pi, ok := prev["items"]; ok {
		out = append(out, compatProblems(obj(pi), obj(next["items"]), at+"/items")...)
	}
	return out
}

func typeOf(s map[string]any) string {
	if t, ok := s["type"]; ok {
		b, _ := json.Marshal(t) // re-encoding a decoded JSON value cannot fail
		return string(b)
	}
	return "(any)"
}

func obj(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

func strs(v any) []string {
	var out []string
	for _, x := range anys(v) {
		if s, ok := x.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func anys(v any) []any {
	a, _ := v.([]any)
	return a
}

func union(a, b map[string]any) map[string]any {
	u := make(map[string]any, len(a)+len(b))
	for k, v := range a {
		u[k] = v
	}
	for k, v := range b {
		u[k] = v
	}
	return u
}

func sortedKeys(m map[string]any) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	slices.Sort(ks)
	return ks
}

func loadDir(t *testing.T, dir string) map[string]map[string]any {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*.schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	out := make(map[string]map[string]any, len(files))
	for _, f := range files {
		b, err := os.ReadFile(f) //nolint:gosec // G304: f comes from globbing schemas/events or schemas/baseline (or the test's temp copy)
		if err != nil {
			t.Fatal(err)
		}
		var s map[string]any
		if err := json.Unmarshal(b, &s); err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		out[strings.TrimSuffix(filepath.Base(f), ".schema.json")] = s
	}
	return out
}

func checkDirs(t *testing.T, cur, base string) []string {
	t.Helper()
	current, baseline := loadDir(t, cur), loadDir(t, base)
	var problems []string
	for _, name := range slices.Sorted(func(yield func(string) bool) {
		for k := range baseline {
			if !yield(k) {
				return
			}
		}
	}) {
		next, ok := current[name]
		if !ok {
			problems = append(problems, name+": published schema version removed")
			continue
		}
		for _, p := range compatProblems(baseline[name], next, "") {
			problems = append(problems, name+": "+p)
		}
	}
	for name := range current {
		if _, ok := baseline[name]; !ok {
			problems = append(problems, fmt.Sprintf("%s: no baseline; copy schemas/events/%[1]s.schema.json to schemas/baseline/ in the change that adds it", name))
		}
	}
	return problems
}

func TestSchemaCompat_CurrentMatchesBaseline(t *testing.T) {
	t.Parallel()
	if len(loadDir(t, baselineDir)) == 0 {
		t.Fatal("no baseline snapshots in schemas/baseline")
	}
	for _, p := range checkDirs(t, currentDir, baselineDir) {
		t.Error(p + " (a breaking change needs a new schema version: docs/architecture/event-schemas.md)")
	}
}

// TestSchemaCompat_DetectsBreakingChanges edits a temporary copy of the current schemas and
// checks each breaking edit is caught and the additive one is not.
func TestSchemaCompat_DetectsBreakingChanges(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		schema   string // default market_tick.v1
		edit     func(s map[string]any)
		breaking bool
	}{
		{"remove required price", "", func(s map[string]any) {
			s["required"] = slices.DeleteFunc(anys(s["required"]), func(v any) bool { return v == "price" })
		}, true},
		{"retype size", "", func(s map[string]any) { obj(s["properties"])["size"] = map[string]any{"type": "string"} }, true},
		{"rename symbol", "", func(s map[string]any) {
			p := obj(s["properties"])
			p["ticker"] = p["symbol"]
			delete(p, "symbol")
			s["required"] = append(slices.DeleteFunc(anys(s["required"]), func(v any) bool { return v == "symbol" }), "ticker")
		}, true},
		{"add required field", "", func(s map[string]any) { s["required"] = append(anys(s["required"]), "venue") }, true},
		{"add optional field", "", func(s map[string]any) {
			obj(s["properties"])["venue"] = map[string]any{"type": "string"}
		}, false},
		{"tighten symbol pattern", "", func(s map[string]any) {
			obj(obj(s["properties"])["symbol"])["pattern"] = "^[A-Z]{1,4}$"
		}, true},
		{"raise a minimum", "", func(s map[string]any) {
			obj(obj(s["properties"])["sequence"])["minimum"] = 1
		}, true},
		{"forbid unknown fields", "", func(s map[string]any) { s["additionalProperties"] = false }, true},
		{"edit a description", "", func(s map[string]any) { s["description"] = "reworded" }, false},
		{"drop a workflow event type", "flowforge_event.v1", func(s map[string]any) {
			p := obj(obj(s["properties"])["event_type"])
			p["enum"] = slices.DeleteFunc(anys(p["enum"]), func(v any) bool { return v == "worker.lost" })
		}, true},
		{"add a workflow event type", "flowforge_event.v1", func(s map[string]any) {
			p := obj(obj(s["properties"])["event_type"])
			p["enum"] = append(anys(p["enum"]), "worker.drained")
		}, false},
		{"repoint a payload schema", "envelope.v1", func(s map[string]any) {
			then := obj(obj(anys(s["allOf"])[0])["then"])
			obj(obj(then["properties"])["payload"])["$ref"] = "workflow_task.v1.schema.json"
		}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			for name, s := range loadDir(t, currentDir) {
				if name == cmp.Or(tc.schema, "market_tick.v1") {
					tc.edit(s)
				}
				b, err := json.Marshal(s)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, name+".schema.json"), b, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			problems := checkDirs(t, dir, baselineDir)
			if tc.breaking && len(problems) == 0 {
				t.Error("breaking edit not detected")
			}
			if !tc.breaking && len(problems) > 0 {
				t.Errorf("additive edit reported: %v", problems)
			}
		})
	}
	t.Run("remove a published version", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		for name, s := range loadDir(t, currentDir) {
			if name == "market_invalid.v1" {
				continue
			}
			b, _ := json.Marshal(s) // re-encoding a decoded schema cannot fail
			if err := os.WriteFile(filepath.Join(dir, name+".schema.json"), b, 0o600); err != nil {
				t.Fatal(err)
			}
		}
		if p := checkDirs(t, dir, baselineDir); len(p) != 1 || !strings.Contains(p[0], "market_invalid.v1") {
			t.Errorf("problems = %v, want the removed market_invalid.v1", p)
		}
	})
}
