package template

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Kind is what a reference reads.
type Kind int

// The reference kinds (task 03.06 §4).
const (
	KindRunInput   Kind = iota + 1 // run.input<path>
	KindTaskStatus                 // tasks.<id>.status
	KindTaskResult                 // tasks.<id>.result<path>
)

// Segment is one step of a path: an object key, or an array index when IsIndex.
type Segment struct {
	Key     string
	Index   int
	IsIndex bool
}

// Ref is a parsed reference such as run.input.symbols, tasks.fetch.status or
// tasks.fetch.result.rows[0].px. Task is set for the task kinds; Path is what follows
// run.input or tasks.<id>.result.
type Ref struct {
	Kind Kind
	Task string
	Path []Segment
}

// ParseRef parses a reference. Keys are dotted and match [A-Za-z0-9_-]+; [n] indexes an array.
// Whitespace around the reference is ignored.
func ParseRef(s string) (Ref, error) {
	segs, err := splitPath(strings.TrimSpace(s))
	if err != nil {
		return Ref{}, err
	}
	key := func(i int) string {
		if i < len(segs) && !segs[i].IsIndex {
			return segs[i].Key
		}
		return ""
	}
	switch {
	case key(0) == "run" && key(1) == "input":
		return Ref{Kind: KindRunInput, Path: segs[2:]}, nil
	case key(0) == "tasks" && key(1) != "" && key(2) == "status" && len(segs) == 3:
		return Ref{Kind: KindTaskStatus, Task: key(1)}, nil
	case key(0) == "tasks" && key(1) != "" && key(2) == "result":
		return Ref{Kind: KindTaskResult, Task: key(1), Path: segs[3:]}, nil
	}
	return Ref{}, fmt.Errorf("%q is not run.input…, tasks.<id>.status or tasks.<id>.result…", strings.TrimSpace(s))
}

// splitPath splits "a.b[0][1].c" into segments.
func splitPath(s string) ([]Segment, error) {
	if s == "" {
		return nil, errors.New("empty reference")
	}
	var out []Segment
	for _, part := range strings.Split(s, ".") {
		name, rest, hasIndex := strings.Cut(part, "[")
		if !validKey(name) {
			return nil, fmt.Errorf("bad key %q in %q", name, s)
		}
		out = append(out, Segment{Key: name})
		if !hasIndex {
			continue
		}
		for _, idx := range strings.Split(rest, "[") {
			digits, ok := strings.CutSuffix(idx, "]")
			n, err := strconv.Atoi(digits)
			if !ok || err != nil || n < 0 || digits != strconv.Itoa(n) {
				return nil, fmt.Errorf("bad index [%s in %q", idx, s)
			}
			out = append(out, Segment{Index: n, IsIndex: true})
		}
	}
	return out, nil
}

func validKey(k string) bool {
	if k == "" {
		return false
	}
	for _, c := range k {
		if !isKeyChar(c) {
			return false
		}
	}
	return true
}

func isKeyChar(c rune) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-'
}

// lookup walks path through a decoded JSON value and reports whether every step exists.
func lookup(v any, path []Segment) (any, bool) {
	for _, s := range path {
		switch c := v.(type) {
		case map[string]any:
			if s.IsIndex {
				return nil, false
			}
			var ok bool
			if v, ok = c[s.Key]; !ok {
				return nil, false
			}
		case []any:
			if !s.IsIndex || s.Index >= len(c) {
				return nil, false
			}
			v = c[s.Index]
		default:
			return nil, false
		}
	}
	return v, true
}
