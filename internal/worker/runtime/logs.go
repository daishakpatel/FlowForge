package runtime

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"

	"google.golang.org/protobuf/types/known/timestamppb"

	workerv1 "github.com/daishakpatel/flowforge/internal/gen/proto/worker/v1"
)

// logSink holds one attempt's log lines: at most max, dropping the oldest on overflow and
// counting the drops. Handlers write to it through logHandler; the worker ships it when the
// attempt ends. Safe for concurrent use.
type logSink struct {
	mu      sync.Mutex
	lines   []*workerv1.LogLine
	max     int
	dropped int
}

func newLogSink(maxLines int) *logSink { return &logSink{max: maxLines} }

func (s *logSink) add(l *workerv1.LogLine) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.lines) == s.max {
		s.lines = s.lines[1:]
		s.dropped++
	}
	s.lines = append(s.lines, l)
}

// take returns and clears the buffered lines and the drop count.
func (s *logSink) take() ([]*workerv1.LogLine, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	lines, dropped := s.lines, s.dropped
	s.lines, s.dropped = nil, 0
	return lines, dropped
}

// logHandler is the slog.Handler behind TaskContext.Logger: each record becomes a LogLine with
// its attributes as fields_json. Groups prefix attribute keys ("group.key").
type logHandler struct {
	sink   *logSink
	attrs  []slog.Attr
	prefix string
}

func (h *logHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *logHandler) Handle(_ context.Context, r slog.Record) error {
	fields := map[string]any{}
	for _, a := range h.attrs {
		fields[a.Key] = a.Value.Resolve().Any()
	}
	r.Attrs(func(a slog.Attr) bool {
		fields[h.prefix+a.Key] = a.Value.Resolve().Any()
		return true
	})
	line := &workerv1.LogLine{Ts: timestamppb.New(r.Time), Level: levelName(r.Level), Message: r.Message}
	if len(fields) > 0 {
		if b, err := json.Marshal(fields); err == nil { // an unmarshalable value drops the fields, not the line
			line.FieldsJson = b
		}
	}
	h.sink.add(line)
	return nil
}

func (h *logHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	next := &logHandler{sink: h.sink, prefix: h.prefix, attrs: append([]slog.Attr(nil), h.attrs...)}
	for _, a := range attrs {
		next.attrs = append(next.attrs, slog.Attr{Key: h.prefix + a.Key, Value: a.Value})
	}
	return next
}

func (h *logHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	return &logHandler{sink: h.sink, attrs: h.attrs, prefix: h.prefix + name + "."}
}

// levelName maps slog levels to the API's DEBUG | INFO | WARN | ERROR.
func levelName(l slog.Level) string {
	switch {
	case l >= slog.LevelError:
		return "ERROR"
	case l >= slog.LevelWarn:
		return "WARN"
	case l >= slog.LevelInfo:
		return "INFO"
	default:
		return "DEBUG"
	}
}

// shipLogs sends the attempt's buffered lines in one ShipLogs stream, before its outcome is
// reported. Best effort: a failure is logged and the lines counted as dropped; it never delays
// the report past one RPC timeout.
func (w *Worker) shipLogs(ctx context.Context, a *attempt) {
	lines, dropped := a.logs.take()
	if dropped > 0 {
		w.d.Metrics.logsDropped(dropped)
	}
	if len(lines) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, w.cfg.RPCTimeout)
	defer cancel()
	err := w.sendLogs(ctx, a, lines)
	if err != nil {
		w.d.Metrics.logsDropped(len(lines))
		w.d.Logger.Warn("shipping attempt logs failed; lines dropped", "attempt_id", a.task.GetAttemptId(), "lines", len(lines), "err", err)
	}
}

func (w *Worker) sendLogs(ctx context.Context, a *attempt, lines []*workerv1.LogLine) error {
	stream, err := w.d.Client.ShipLogs(ctx)
	if err != nil {
		return err
	}
	for _, l := range lines {
		l.AttemptId, l.FencingToken = a.task.GetAttemptId(), a.task.GetFencingToken()
		if err := stream.Send(l); err != nil {
			_, rerr := stream.CloseAndRecv() // the server's status explains a failed Send
			return rerr
		}
	}
	_, err = stream.CloseAndRecv()
	return err
}
