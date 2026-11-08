// Package logging builds FlowForge's structured loggers (task 01.04, 00E §11, NFR-OBS-03).
//
// New returns a *slog.Logger that writes JSON (or text) with the canonical field names, adds
// the request-scoped fields stored in a context by WithFields, and redacts secrets: values of
// sensitive-looking keys and secret-shaped strings anywhere in a message or value (see Redact).
// There is no global logger; main builds one and passes it down.
package logging

import (
	"io"
	"log/slog"
	"os"
)

// Canonical log field names (00E §11). Use these keys rather than string literals.
const (
	KeyRequestID = "request_id"
	KeyRunID     = "run_id"
	KeyTaskID    = "task_id"
	KeyAttemptID = "attempt_id"
	KeyWorkerID  = "worker_id"
	KeyTraceID   = "trace_id"
	KeySpanID    = "span_id"
	KeyService   = "service"
	KeyVersion   = "version"
	KeyError     = "err"
	keyTime      = "ts" // 00E §11 names the timestamp "ts"; slog's default is "time"
)

// Config selects the logger's level, format and fixed fields. Level and Format take the values
// validated by internal/config (FF_LOG_LEVEL, FF_LOG_FORMAT).
type Config struct {
	Level   string    // debug | info | warn | error; anything else means info
	Format  string    // json | text; anything else means json
	Service string    // added as "service" when set
	Version string    // added as "version" when set
	Writer  io.Writer // nil means os.Stdout
}

// New returns a logger for cfg. Every record passes through redaction, and records logged with
// a *Context method include the fields stored in that context.
func New(cfg Config) *slog.Logger {
	var level slog.Level
	if err := level.UnmarshalText([]byte(cfg.Level)); err != nil {
		level = slog.LevelInfo
	}
	w := cfg.Writer
	if w == nil {
		w = os.Stdout
	}
	opts := &slog.HandlerOptions{Level: level, ReplaceAttr: replaceAttr}
	var h slog.Handler = slog.NewJSONHandler(w, opts)
	if cfg.Format == "text" {
		h = slog.NewTextHandler(w, opts)
	}
	l := slog.New(&contextHandler{Handler: h})
	if cfg.Service != "" {
		l = l.With(KeyService, cfg.Service)
	}
	if cfg.Version != "" {
		l = l.With(KeyVersion, cfg.Version)
	}
	return l
}

// Err returns the canonical "err" attribute. Its text is redacted like any other value.
func Err(err error) slog.Attr { return slog.Any(KeyError, err) }

// replaceAttr renames the timestamp and redacts every other attribute, including the message.
func replaceAttr(groups []string, a slog.Attr) slog.Attr {
	if len(groups) == 0 && a.Key == slog.TimeKey {
		a.Key = keyTime
		return a
	}
	return redactAttr(groups, a)
}
