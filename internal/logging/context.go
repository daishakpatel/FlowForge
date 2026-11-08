package logging

import (
	"context"
	"log/slog"
	"maps"
)

type (
	fieldsKey struct{}
	loggerKey struct{}
)

// WithFields returns a context carrying extra log fields as key-value pairs, such as
// WithFields(ctx, KeyRunID, runID). A key set again replaces the earlier value. Loggers from New
// add these fields to every record logged with a *Context method (InfoContext and so on), and
// FromContext binds them, so request-scoped IDs appear without being passed around.
func WithFields(ctx context.Context, args ...any) context.Context {
	var r slog.Record
	r.Add(args...)
	merged := withRoom(fieldsFrom(ctx), r.NumAttrs())
	r.Attrs(func(a slog.Attr) bool {
		merged = setAttr(merged, a)
		return true
	})
	return context.WithValue(ctx, fieldsKey{}, merged)
}

// NewContext returns a context carrying l, for FromContext.
func NewContext(ctx context.Context, l *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey{}, l)
}

// FromContext returns the logger stored by NewContext (slog.Default() if none) with the
// context's fields bound, so plain Info calls include them too.
func FromContext(ctx context.Context) *slog.Logger {
	l, ok := ctx.Value(loggerKey{}).(*slog.Logger)
	if !ok {
		l = slog.Default()
	}
	fields := fieldsFrom(ctx)
	if len(fields) == 0 {
		return l
	}
	args := make([]any, len(fields))
	for i, a := range fields {
		args[i] = a
	}
	return l.With(args...)
}

func fieldsFrom(ctx context.Context) []slog.Attr {
	fs, _ := ctx.Value(fieldsKey{}).([]slog.Attr)
	return fs
}

// withRoom copies fs with room for n more, so contexts never share a backing array.
func withRoom(fs []slog.Attr, n int) []slog.Attr {
	return append(make([]slog.Attr, 0, len(fs)+n), fs...)
}

// setAttr replaces the attribute with a's key, or appends a.
func setAttr(fs []slog.Attr, a slog.Attr) []slog.Attr {
	for i := range fs {
		if fs[i].Key == a.Key {
			fs[i] = a
			return fs
		}
	}
	return append(fs, a)
}

// contextHandler adds the context's fields to each record. It skips a field whose key is
// already on the record or was bound with With before any group, so FromContext loggers and
// explicit attributes never produce duplicate keys. Fields added inside an open group land in
// that group, as any attribute would.
type contextHandler struct {
	slog.Handler
	bound   map[string]bool
	grouped bool
}

func (h *contextHandler) Handle(ctx context.Context, r slog.Record) error {
	fields := fieldsFrom(ctx)
	if len(fields) == 0 {
		return h.Handler.Handle(ctx, r)
	}
	present := maps.Clone(h.bound)
	if present == nil {
		present = map[string]bool{}
	}
	r.Attrs(func(a slog.Attr) bool {
		present[a.Key] = true
		return true
	})
	r = r.Clone()
	for _, a := range fields {
		if !present[a.Key] {
			r.AddAttrs(a)
		}
	}
	return h.Handler.Handle(ctx, r)
}

func (h *contextHandler) WithAttrs(as []slog.Attr) slog.Handler {
	bound := maps.Clone(h.bound)
	if !h.grouped {
		if bound == nil {
			bound = map[string]bool{}
		}
		for _, a := range as {
			bound[a.Key] = true
		}
	}
	return &contextHandler{Handler: h.Handler.WithAttrs(as), bound: bound, grouped: h.grouped}
}

func (h *contextHandler) WithGroup(name string) slog.Handler {
	return &contextHandler{Handler: h.Handler.WithGroup(name), bound: h.bound, grouped: true}
}
