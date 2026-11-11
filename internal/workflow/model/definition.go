// Package model holds the workflow definition (00E §3.5) and the status enums (00E §2–§3).
// It is pure: no database, transport or clock (ADR-020, lint-enforced).
package model

import (
	"encoding/json"
	"time"
)

// Definition is a decoded workflow definition with defaults applied (see Decode). Validate it
// with dag.Validate before use; Decode only checks the JSON shape.
type Definition struct {
	Name           string      `json:"name"`
	Version        int         `json:"version"`
	TimeoutSeconds int         `json:"timeout_seconds"` // 0 means no workflow-level timeout
	OnFailure      OnFailure   `json:"on_failure"`
	Concurrency    Concurrency `json:"concurrency"`
	Tasks          []TaskDef   `json:"tasks"`
}

// Concurrency limits a run.
type Concurrency struct {
	MaxParallelTasks int `json:"max_parallel_tasks"` // 0 means no per-run limit
}

// TaskDef is one task of a definition. ID is the task_key stored in workflow_tasks.
type TaskDef struct {
	ID             string          `json:"id"`
	Type           string          `json:"type"`
	Queue          string          `json:"queue"`
	DependsOn      []string        `json:"depends_on"`
	TimeoutSeconds int             `json:"timeout_seconds"`
	Config         json.RawMessage `json:"config,omitempty"`
	Retry          RetryPolicy     `json:"retry"`
	RunIf          RunIf           `json:"run_if"`
	RunIfExpr      string          `json:"run_if_expr,omitempty"` // only with run_if = expr
}

// RetryPolicy controls attempts and backoff for one task (FR-WF-07).
type RetryPolicy struct {
	MaxAttempts        int           `json:"max_attempts"`
	Strategy           RetryStrategy `json:"strategy"`
	InitialDelayMS     int64         `json:"initial_delay_ms"`
	MaxDelayMS         int64         `json:"max_delay_ms"`
	Multiplier         float64       `json:"multiplier"`
	Jitter             float64       `json:"jitter"` // fraction in [0,1]: delay varies by ±jitter
	NonRetryableErrors []string      `json:"non_retryable_errors,omitempty"`
}

// InitialDelay returns InitialDelayMS as a duration.
func (p RetryPolicy) InitialDelay() time.Duration {
	return time.Duration(p.InitialDelayMS) * time.Millisecond
}

// MaxDelay returns MaxDelayMS as a duration.
func (p RetryPolicy) MaxDelay() time.Duration { return time.Duration(p.MaxDelayMS) * time.Millisecond }
