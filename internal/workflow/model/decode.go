package model

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/daishakpatel/flowforge/internal/apperrors"
)

// Decode parses a workflow definition strictly: unknown fields, trailing data and bodies over
// MaxDefinitionBytes are rejected with an INVALID_ARGUMENT *apperrors.Error. Absent fields get
// the Default* values; explicit values are kept as given, even zero. Decode checks shape only;
// run dag.Validate for the semantic rules.
func Decode(data []byte) (*Definition, error) {
	if len(data) > MaxDefinitionBytes {
		return nil, apperrors.PayloadTooLarge(fmt.Sprintf("workflow definition is %d bytes; the limit is %d", len(data), MaxDefinitionBytes))
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var w wireDefinition
	if err := dec.Decode(&w); err != nil {
		return nil, apperrors.Wrap(apperrors.CodeInvalidArgument, "invalid workflow definition JSON: "+err.Error(), err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, apperrors.InvalidArgument("invalid workflow definition JSON: unexpected data after the definition")
	}
	return w.toModel(), nil
}

// The wire types mirror Definition with pointers where "absent" and "explicit zero" differ.
type wireDefinition struct {
	Name           string           `json:"name"`
	Version        int              `json:"version"`
	TimeoutSeconds *int             `json:"timeout_seconds"`
	OnFailure      *OnFailure       `json:"on_failure"`
	Concurrency    *wireConcurrency `json:"concurrency"`
	Tasks          []wireTask       `json:"tasks"`
}

type wireConcurrency struct {
	MaxParallelTasks *int `json:"max_parallel_tasks"`
}

type wireTask struct {
	ID             string          `json:"id"`
	Type           string          `json:"type"`
	Queue          *string         `json:"queue"`
	DependsOn      []string        `json:"depends_on"`
	TimeoutSeconds *int            `json:"timeout_seconds"`
	Config         json.RawMessage `json:"config"`
	Retry          *wireRetry      `json:"retry"`
	RunIf          *RunIf          `json:"run_if"`
	RunIfExpr      string          `json:"run_if_expr"`
}

type wireRetry struct {
	MaxAttempts        *int           `json:"max_attempts"`
	Strategy           *RetryStrategy `json:"strategy"`
	InitialDelayMS     *int64         `json:"initial_delay_ms"`
	MaxDelayMS         *int64         `json:"max_delay_ms"`
	Multiplier         *float64       `json:"multiplier"`
	Jitter             *float64       `json:"jitter"`
	NonRetryableErrors []string       `json:"non_retryable_errors"`
}

func (w wireDefinition) toModel() *Definition {
	d := &Definition{
		Name:           w.Name,
		Version:        w.Version,
		TimeoutSeconds: or(w.TimeoutSeconds, DefaultWorkflowTimeout),
		OnFailure:      or(w.OnFailure, DefaultOnFailure),
		Tasks:          make([]TaskDef, len(w.Tasks)),
	}
	d.Concurrency.MaxParallelTasks = DefaultMaxParallelTasks
	if w.Concurrency != nil {
		d.Concurrency.MaxParallelTasks = or(w.Concurrency.MaxParallelTasks, DefaultMaxParallelTasks)
	}
	for i, t := range w.Tasks {
		d.Tasks[i] = t.toModel()
	}
	return d
}

func (t wireTask) toModel() TaskDef {
	deps := t.DependsOn
	if deps == nil {
		deps = []string{}
	}
	var r wireRetry
	if t.Retry != nil {
		r = *t.Retry
	}
	return TaskDef{
		ID:             t.ID,
		Type:           t.Type,
		Queue:          or(t.Queue, DefaultQueue),
		DependsOn:      deps,
		TimeoutSeconds: or(t.TimeoutSeconds, DefaultTaskTimeoutSeconds),
		Config:         t.Config,
		RunIf:          or(t.RunIf, DefaultRunIf),
		RunIfExpr:      t.RunIfExpr,
		Retry: RetryPolicy{
			MaxAttempts:        or(r.MaxAttempts, DefaultMaxAttempts),
			Strategy:           or(r.Strategy, DefaultStrategy),
			InitialDelayMS:     or(r.InitialDelayMS, DefaultInitialDelayMS),
			MaxDelayMS:         or(r.MaxDelayMS, DefaultMaxDelayMS),
			Multiplier:         or(r.Multiplier, DefaultMultiplier),
			Jitter:             or(r.Jitter, DefaultJitter),
			NonRetryableErrors: r.NonRetryableErrors,
		},
	}
}

// or returns *p, or def when p is nil (the field was absent).
func or[T any](p *T, def T) T {
	if p == nil {
		return def
	}
	return *p
}
