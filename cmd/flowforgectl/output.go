package main

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
)

// printer writes results as JSON (-o json) or as a table of chosen columns (-o table).
type printer struct {
	w    io.Writer
	json bool
}

// print writes v, an API response decoded as generic JSON. A list response ({"items": [...]})
// becomes one row per item; an object becomes one row. cols are the fields shown in table mode.
func (p printer) print(v any, cols ...string) error {
	if p.json {
		enc := json.NewEncoder(p.w)
		enc.SetIndent("", "  ")
		return enc.Encode(v)
	}
	rows := []any{v}
	if m, ok := v.(map[string]any); ok {
		if items, ok := m["items"].([]any); ok {
			rows = items
		}
	}
	tw := tabwriter.NewWriter(p.w, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, strings.ToUpper(strings.Join(cols, "\t"))) // a tabwriter buffers; Flush reports errors
	for _, r := range rows {
		m, _ := r.(map[string]any)
		cells := make([]string, len(cols))
		for i, c := range cols {
			cells[i] = cell(m[c])
		}
		_, _ = fmt.Fprintln(tw, strings.Join(cells, "\t"))
	}
	return tw.Flush()
}

func cell(v any) string {
	switch x := v.(type) {
	case nil:
		return "-"
	case string:
		return x
	case float64:
		return fmt.Sprintf("%g", x)
	case bool:
		return fmt.Sprintf("%t", x)
	}
	raw, _ := json.Marshal(v) // values came from JSON, so they re-encode
	return string(raw)
}
