// Package template resolves the ${…} placeholders in a task's config into its input (task
// 03.06, FR-WF-11). It is deliberately not a scripting language: a placeholder is one reference,
// with no functions, operators or loops. It is pure (ADR-020, lint-enforced).
//
// References:
//
//	${run.input.<path>}         the run's input
//	${tasks.<id>.result.<path>} a task's result; <id> must be an ancestor of the task
//	${tasks.<id>.status}        a task's status, e.g. "SUCCEEDED"
//
// A path is dotted keys with [n] array indexes, e.g. rows[0].px. A string that is exactly one
// placeholder becomes the referenced JSON value, keeping its type; placeholders inside a longer
// string are interpolated as text (strings verbatim, anything else as compact JSON).
package template

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
)

// MaxInputBytes caps a resolved input (task 03.06 §4).
const MaxInputBytes = 256 << 10

// Scope is what placeholders can read.
type Scope struct {
	RunInput json.RawMessage
	// Tasks holds the referencing task's ancestors by key; any other task is out of scope.
	Tasks map[string]Task
}

// Task is a task as seen by references.
type Task struct {
	Status string
	Result json.RawMessage // nil or JSON null: the task has no result
}

// Reader reads references from a Scope, decoding each JSON document once. It is not safe for
// concurrent use.
type Reader struct {
	scope Scope
	docs  map[string]any
}

// NewReader returns a Reader over s.
func NewReader(s Scope) *Reader { return &Reader{scope: s, docs: map[string]any{}} }

// Lookup returns the referenced value; numbers are json.Number. found is false when the path
// does not exist, or for a result reference to a task without a result. An error means ref
// names a task outside the scope, or the stored JSON is invalid.
func (r *Reader) Lookup(ref Ref) (v any, found bool, err error) {
	if ref.Kind == KindRunInput {
		doc, err := r.doc("run.input", r.scope.RunInput)
		if err != nil {
			return nil, false, err
		}
		v, found = lookup(doc, ref.Path)
		return v, found, nil
	}
	t, ok := r.scope.Tasks[ref.Task]
	if !ok {
		return nil, false, fmt.Errorf("task %q is not an ancestor of this task", ref.Task)
	}
	if ref.Kind == KindTaskStatus {
		return t.Status, true, nil
	}
	doc, err := r.doc("tasks."+ref.Task, t.Result)
	if err != nil || doc == nil {
		return nil, false, err
	}
	v, found = lookup(doc, ref.Path)
	return v, found, nil
}

func (r *Reader) doc(name string, raw json.RawMessage) (any, error) {
	if v, ok := r.docs[name]; ok {
		return v, nil
	}
	var v any
	if len(bytes.TrimSpace(raw)) > 0 {
		var err error
		if v, err = decode(raw); err != nil {
			return nil, fmt.Errorf("decode %s: %w", name, err)
		}
	}
	r.docs[name] = v
	return v, nil
}

func decode(raw []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	err := dec.Decode(&v)
	return v, err
}

// Resolver resolves a task's config into its input. The zero value is ready to use.
type Resolver struct{}

// Resolve replaces every placeholder in config's string leaves, recursively through objects and
// arrays; object keys are not templated. It returns a *TemplateError for a placeholder it cannot
// resolve and an error wrapping ErrTooLarge when the result is over MaxInputBytes. A config
// without placeholders is returned unchanged.
func (Resolver) Resolve(config json.RawMessage, s Scope) (json.RawMessage, error) {
	out := config
	if bytes.Contains(config, []byte("${")) {
		doc, err := decode(config)
		if err != nil {
			return nil, fmt.Errorf("decode config: %w", err)
		}
		r := NewReader(s)
		if doc, err = walk(doc, r.expand); err != nil {
			return nil, err
		}
		if out, err = encode(doc); err != nil {
			return nil, err
		}
	}
	if len(out) > MaxInputBytes {
		return nil, fmt.Errorf("%w: %d bytes; the limit is %d", ErrTooLarge, len(out), MaxInputBytes)
	}
	return out, nil
}

// walk applies fn to every string leaf of v.
func walk(v any, fn func(string) (any, error)) (any, error) {
	switch c := v.(type) {
	case string:
		return fn(c)
	case map[string]any:
		for _, k := range slices.Sorted(maps.Keys(c)) { // key order: deterministic Refs and first error
			var err error
			if c[k], err = walk(c[k], fn); err != nil {
				return nil, err
			}
		}
	case []any:
		for i, e := range c {
			var err error
			if c[i], err = walk(e, fn); err != nil {
				return nil, err
			}
		}
	}
	return v, nil
}

// expand resolves the placeholders in one string.
func (r *Reader) expand(s string) (any, error) {
	parts, err := split(s)
	if err != nil || len(parts) == 0 {
		return s, err
	}
	if len(parts) == 1 && parts[0].start == 0 && parts[0].end == len(s) {
		return r.value(parts[0].ref)
	}
	var b strings.Builder
	last := 0
	for _, p := range parts {
		v, err := r.value(p.ref)
		if err != nil {
			return nil, err
		}
		b.WriteString(s[last:p.start])
		if err := writeText(&b, v); err != nil {
			return nil, err
		}
		last = p.end
	}
	b.WriteString(s[last:])
	return b.String(), nil
}

func (r *Reader) value(text string) (any, error) {
	ref, err := ParseRef(text)
	if err != nil {
		return nil, &TemplateError{Path: text, Reason: err.Error()}
	}
	v, found, err := r.Lookup(ref)
	switch {
	case err != nil:
		return nil, &TemplateError{Path: text, Reason: err.Error()}
	case !found:
		return nil, &TemplateError{Path: text, Reason: "path not found"}
	}
	return v, nil
}

func writeText(b *strings.Builder, v any) error {
	if s, ok := v.(string); ok {
		b.WriteString(s)
		return nil
	}
	raw, err := encode(v)
	b.Write(raw)
	return err
}

func encode(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, fmt.Errorf("encode: %w", err)
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

// placeholder is one ${…} in a string: s[start:end] is the whole placeholder, ref its inside.
type placeholder struct {
	start, end int
	ref        string
}

// split finds the placeholders in s; an unterminated one is a *TemplateError.
func split(s string) ([]placeholder, error) {
	var out []placeholder
	for from := 0; ; {
		i := strings.Index(s[from:], "${")
		if i < 0 {
			return out, nil
		}
		start := from + i
		j := strings.IndexByte(s[start:], '}')
		if j < 0 {
			return nil, &TemplateError{Path: s[start+2:], Reason: "unterminated placeholder"}
		}
		out = append(out, placeholder{start: start, end: start + j + 1, ref: s[start+2 : start+j]})
		from = start + j + 1
	}
}

// Refs returns every reference in config's placeholders, for definition-time validation; a
// placeholder that does not parse is a *TemplateError.
func Refs(config json.RawMessage) ([]Ref, error) {
	if !bytes.Contains(config, []byte("${")) {
		return nil, nil
	}
	doc, err := decode(config)
	if err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}
	var out []Ref
	_, err = walk(doc, func(s string) (any, error) {
		parts, err := split(s)
		for _, p := range parts {
			ref, perr := ParseRef(p.ref)
			if perr != nil {
				return nil, &TemplateError{Path: p.ref, Reason: perr.Error()}
			}
			out = append(out, ref)
		}
		return s, err
	})
	return out, err
}
