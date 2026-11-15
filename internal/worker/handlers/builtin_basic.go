package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/daishakpatel/flowforge/internal/clock"
)

// Builtins returns the built-in handlers: noop, sleep, fail, flaky and echo, plus crash when
// exit is non-nil (cmd/flowforge-worker passes os.Exit only with
// FF_WORKER_ENABLE_CRASH_HANDLER=true). c is the clock sleep waits on.
func Builtins(c clock.Clock, exit func(code int)) []Handler {
	hs := []Handler{Noop{}, Sleep{Clock: c}, Fail{}, Flaky{}, Echo{}}
	if exit != nil {
		hs = append(hs, Crash{Exit: exit})
	}
	return hs
}

// decodeInput unmarshals a handler's input into v; bad input is NonRetryable INVALID_INPUT.
// Empty input decodes as {}.
func decodeInput(in json.RawMessage, v any) error {
	if len(in) == 0 {
		return nil
	}
	if err := json.Unmarshal(in, v); err != nil {
		return NonRetryable(CodeInvalidInput, fmt.Errorf("decode input: %w", err))
	}
	return nil
}

// Noop returns its input unchanged.
type Noop struct{}

// Type implements Handler.
func (Noop) Type() string { return "noop" }

// Execute implements Handler.
func (Noop) Execute(_ context.Context, tc TaskContext) (json.RawMessage, error) {
	if len(tc.Input) == 0 {
		return json.RawMessage(`{}`), nil
	}
	return tc.Input, nil
}

// Sleep waits {"duration_ms": n} on its clock, returning ctx's error if cancelled first, and
// then returns {"slept_ms": n}.
type Sleep struct{ Clock clock.Clock }

// Type implements Handler.
func (Sleep) Type() string { return "sleep" }

// Execute implements Handler.
func (s Sleep) Execute(ctx context.Context, tc TaskContext) (json.RawMessage, error) {
	var in struct {
		DurationMS int64 `json:"duration_ms"`
	}
	if err := decodeInput(tc.Input, &in); err != nil {
		return nil, err
	}
	if in.DurationMS < 0 {
		return nil, NonRetryable(CodeInvalidInput, errors.New("duration_ms is negative"))
	}
	if err := s.Clock.Sleep(ctx, time.Duration(in.DurationMS)*time.Millisecond); err != nil {
		return nil, err
	}
	return json.Marshal(map[string]int64{"slept_ms": in.DurationMS})
}

// Fail always fails with {"code", "retryable", "message"}; code defaults to FAILED.
type Fail struct{}

// Type implements Handler.
func (Fail) Type() string { return "fail" }

// Execute implements Handler.
func (Fail) Execute(_ context.Context, tc TaskContext) (json.RawMessage, error) {
	in := struct {
		Code      string `json:"code"`
		Retryable bool   `json:"retryable"`
		Message   string `json:"message"`
	}{Code: "FAILED", Message: "fail handler"}
	if err := decodeInput(tc.Input, &in); err != nil {
		return nil, err
	}
	err := errors.New(in.Message)
	if in.Retryable {
		return nil, Retryable(in.Code, err)
	}
	return nil, NonRetryable(in.Code, err)
}

// Flaky fails with a retryable FLAKY while attempt_number <= {"fail_attempts": n}, then
// returns its input: it exercises retries (03.04).
type Flaky struct{}

// Type implements Handler.
func (Flaky) Type() string { return "flaky" }

// Execute implements Handler.
func (Flaky) Execute(ctx context.Context, tc TaskContext) (json.RawMessage, error) {
	var in struct {
		FailAttempts int `json:"fail_attempts"`
	}
	if err := decodeInput(tc.Input, &in); err != nil {
		return nil, err
	}
	if tc.AttemptNumber <= in.FailAttempts {
		return nil, Retryable("FLAKY", fmt.Errorf("attempt %d of %d planned failures", tc.AttemptNumber, in.FailAttempts))
	}
	return Noop{}.Execute(ctx, tc)
}

// Echo logs one line and returns the input with the attempt's identity, so tests can check
// what a handler sees.
type Echo struct{}

// Type implements Handler.
func (Echo) Type() string { return "echo" }

// Execute implements Handler.
func (Echo) Execute(ctx context.Context, tc TaskContext) (json.RawMessage, error) {
	if tc.Logger != nil {
		tc.Logger.InfoContext(ctx, "echo", "attempt_number", tc.AttemptNumber)
	}
	input := tc.Input
	if len(input) == 0 {
		input = json.RawMessage(`{}`)
	}
	return json.Marshal(map[string]any{
		"input": input, "task_id": tc.TaskID, "run_id": tc.RunID,
		"attempt_number": tc.AttemptNumber, "idempotency_key": tc.IdempotencyKey,
	})
}
