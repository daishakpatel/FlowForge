package logging

import (
	"fmt"
	"log/slog"
	"regexp"
	"slices"
)

const masked = "***"

// sensitiveKey matches attribute keys (and group names) whose values are always masked.
var sensitiveKey = regexp.MustCompile(`(?i)(password|passwd|secret|token|api[_-]?key|authorization|dsn)`)

// secretPatterns mask secret-shaped substrings, in order. Each keeps a non-secret prefix so the
// log still shows what was there.
var secretPatterns = []struct {
	re   *regexp.Regexp
	repl string
}{
	{regexp.MustCompile(`\bff_[A-Za-z0-9]+_[A-Za-z0-9]{16,}`), "ff_" + masked},                // API keys ff_<id>_<secret> (FR-SEC-01)
	{regexp.MustCompile(`(?i)(bearer\s+)[A-Za-z0-9._~+/=-]+`), "${1}" + masked},               // Authorization: Bearer …
	{regexp.MustCompile(`(?i)(postgres(?:ql)?://[^:/@\s]+:)[^@\s]+@`), "${1}" + masked + "@"}, // DSN passwords
	// key=value, key: value and JSON; an auth scheme ("Authorization: Bearer x") is masked with its value.
	{regexp.MustCompile(`(?i)((?:password|passwd|secret|token|api[_-]?key|authorization|dsn)["']?\s*[:=]\s*["']?)(?:bearer\s+)?[^\s"'&,;}]+`), "${1}" + masked},
}

// Redact masks secret-shaped substrings in s: FlowForge API keys, bearer tokens, passwords in
// postgres:// URLs, and the value in "password=…", "token: …", `"api_key":"…"` and similar.
// The logger applies it to every message and value; use it directly for text that is stored
// or forwarded rather than logged, such as audit details, task logs and AI evidence.
func Redact(s string) string {
	for _, p := range secretPatterns {
		s = p.re.ReplaceAllString(s, p.repl)
	}
	return s
}

// redactAttr masks a's value when its key or any enclosing group looks sensitive, and otherwise
// redacts secret-shaped text in string, error and other non-scalar values. Values arrive here
// already resolved (slog calls LogValue first).
func redactAttr(groups []string, a slog.Attr) slog.Attr {
	if sensitiveKey.MatchString(a.Key) || slices.ContainsFunc(groups, sensitiveKey.MatchString) {
		return slog.String(a.Key, masked)
	}
	switch a.Value.Kind() {
	case slog.KindString:
		a.Value = slog.StringValue(Redact(a.Value.String()))
	case slog.KindAny:
		if err, ok := a.Value.Any().(error); ok {
			return slog.String(a.Key, Redact(err.Error()))
		}
		// Structs and maps are formatted as text only when they contain something to mask,
		// so ordinary values keep their JSON shape.
		if s := fmt.Sprintf("%+v", a.Value.Any()); Redact(s) != s {
			return slog.String(a.Key, Redact(s))
		}
	}
	return a
}
