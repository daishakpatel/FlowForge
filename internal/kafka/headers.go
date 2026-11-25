package kafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

// HeaderTraceparent carries the W3C trace context of the operation that produced a record. Task
// 08.01 replaces the context value below with OpenTelemetry's propagator; the header name stays.
const HeaderTraceparent = "traceparent"

type traceKey struct{}

// WithTraceparent returns ctx carrying a W3C traceparent ("00-<trace id>-<span id>-<flags>").
func WithTraceparent(ctx context.Context, traceparent string) context.Context {
	return context.WithValue(ctx, traceKey{}, traceparent)
}

// TraceparentFrom returns the traceparent ctx carries, or "".
func TraceparentFrom(ctx context.Context) string {
	tp, _ := ctx.Value(traceKey{}).(string)
	return tp
}

// InjectTrace sets r's traceparent header from ctx, unless ctx has none or r already has one (a
// relay re-publishing a stored event keeps the event's own trace).
func InjectTrace(ctx context.Context, r *kgo.Record) {
	tp := TraceparentFrom(ctx)
	if tp == "" {
		return
	}
	if _, ok := Header(r, HeaderTraceparent); ok {
		return
	}
	SetHeader(r, HeaderTraceparent, tp)
}

// ExtractTrace returns ctx carrying r's traceparent header, if r has one.
func ExtractTrace(ctx context.Context, r *kgo.Record) context.Context {
	if tp, ok := Header(r, HeaderTraceparent); ok && tp != "" {
		return WithTraceparent(ctx, tp)
	}
	return ctx
}

// Header returns the value of r's first header named key.
func Header(r *kgo.Record, key string) (string, bool) {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value), true
		}
	}
	return "", false
}

// SetHeader sets header key on r, replacing any headers with that name.
func SetHeader(r *kgo.Record, key, value string) {
	hs := make([]kgo.RecordHeader, 0, len(r.Headers)+1) // a new slice: r.Headers may be shared with other records
	for _, h := range r.Headers {
		if h.Key != key {
			hs = append(hs, h)
		}
	}
	r.Headers = append(hs, kgo.RecordHeader{Key: key, Value: []byte(value)})
}
