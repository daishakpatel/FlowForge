// Package handlers is the worker's handler SDK (task 03.03): the Handler interface, the
// TaskContext a handler runs with, the Registry the worker dispatches through, error
// classification, and the built-in handlers used by tests and the demo. See
// docs/architecture/handlers.md.
package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"slices"
	"time"
)

// Handler runs one task type.
//
// Execute must honor ctx. The worker cancels it when the server cancels the attempt, when the
// attempt goes stale, one second before the attempt deadline, and at shutdown. It returns the
// result JSON, or an error classified with Retryable or NonRetryable; an unclassified error is a
// retryable HANDLER_ERROR.
//
// Execution is at least once (ADR-007): the same task can run again after a timeout, a lost
// worker or a retry. A handler with external side effects MUST make them idempotent with
// TaskContext.IdempotencyKey.
type Handler interface {
	Type() string
	Execute(ctx context.Context, tc TaskContext) (json.RawMessage, error)
}

// TaskContext is what a handler knows about the attempt it runs.
type TaskContext struct {
	TaskID        string
	AttemptID     string
	AttemptNumber int
	// IdempotencyKey is "task_id:attempt_number". Use it, or TaskID for effects that must
	// happen once across attempts, as the dedupe key of every external side effect.
	IdempotencyKey string
	Input          json.RawMessage
	Deadline       time.Time // the server's deadline; ctx ends a little earlier
	// Logger writes the attempt's log lines, shipped to the API when the attempt ends and
	// readable at GET /api/v1/attempts/{id}/logs. Lines are redacted server-side.
	Logger      *slog.Logger
	RunID       string
	Traceparent string // W3C trace context; empty until tracing lands (08.01)
}

// Registry maps task types to handlers. Register everything before the worker starts; after
// that it is read-only and safe for concurrent use.
type Registry struct {
	handlers map[string]Handler
}

// NewRegistry returns an empty Registry.
func NewRegistry() *Registry { return &Registry{handlers: map[string]Handler{}} }

// Register adds h under h.Type(). An empty or already registered type is an error.
func (r *Registry) Register(h Handler) error {
	typ := h.Type()
	if typ == "" {
		return errors.New("register handler: empty task type")
	}
	if _, dup := r.handlers[typ]; dup {
		return fmt.Errorf("register handler: task type %q already registered", typ)
	}
	r.handlers[typ] = h
	return nil
}

// Get returns the handler for typ.
func (r *Registry) Get(typ string) (Handler, bool) {
	h, ok := r.handlers[typ]
	return h, ok
}

// Types returns the registered task types, sorted: what the worker advertises in Register.
func (r *Registry) Types() []string {
	types := make([]string, 0, len(r.handlers))
	for t := range r.handlers {
		types = append(types, t)
	}
	slices.Sort(types)
	return types
}

// Execute runs h with tc. A panic becomes a retryable PANIC error; its stack goes to tc.Logger
// (the attempt's logs) only, never into the reported error message.
func Execute(ctx context.Context, h Handler, tc TaskContext) (result json.RawMessage, err error) {
	defer func() {
		if p := recover(); p != nil {
			if tc.Logger != nil {
				tc.Logger.ErrorContext(ctx, "handler panicked", "panic", fmt.Sprint(p), "stack", string(debug.Stack()))
			}
			result, err = nil, Retryable(CodePanic, fmt.Errorf("handler %s panicked", h.Type()))
		}
	}()
	return h.Execute(ctx, tc)
}
