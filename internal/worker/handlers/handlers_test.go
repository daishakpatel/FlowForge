package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/testutil"
)

func TestClassify(t *testing.T) {
	t.Parallel()
	base := errors.New("boom")
	for name, tc := range map[string]struct {
		err       error
		code      string
		retryable bool
	}{
		"unclassified":           {base, CodeHandlerError, true},
		"non-retryable":          {NonRetryable("BAD_INPUT", base), "BAD_INPUT", false},
		"retryable":              {Retryable("UPSTREAM", base), "UPSTREAM", true},
		"wrapped classification": {fmt.Errorf("step 2: %w", NonRetryable("NOPE", base)), "NOPE", false},
		"context cancelled":      {fmt.Errorf("read: %w", context.Canceled), CodeCancelled, false},
		"deadline":               {context.DeadlineExceeded, CodeDeadline, true},
		"empty code":             {NonRetryable("", base), CodeHandlerError, false},
		"over-long code":         {Retryable(strings.Repeat("X", 65), base), CodeHandlerError, true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			code, retryable := Classify(tc.err)
			if code != tc.code || retryable != tc.retryable {
				t.Errorf("Classify = %s/%v, want %s/%v", code, retryable, tc.code, tc.retryable)
			}
		})
	}
	if !errors.Is(NonRetryable("X", base), base) {
		t.Error("classified errors must unwrap to their cause")
	}
}

type panicky struct{}

func (panicky) Type() string { return "panicky" }
func (panicky) Execute(context.Context, TaskContext) (json.RawMessage, error) {
	panic("secret-ish detail")
}

func TestExecute_RecoversPanic(t *testing.T) {
	t.Parallel()
	var logs bytes.Buffer
	_, err := Execute(t.Context(), panicky{}, TaskContext{Logger: slog.New(slog.NewTextHandler(&logs, nil))})
	code, retryable := Classify(err)
	if code != CodePanic || !retryable {
		t.Fatalf("panic classified as %s/%v, want PANIC/true", code, retryable)
	}
	if strings.Contains(err.Error(), "secret-ish") || strings.Contains(err.Error(), "goroutine") {
		t.Errorf("reported error leaks the panic value or stack: %v", err)
	}
	if !strings.Contains(logs.String(), "secret-ish detail") || !strings.Contains(logs.String(), "handlers_test.go") {
		t.Errorf("the attempt log must carry the panic value and stack: %s", logs.String())
	}
	if _, err := Execute(t.Context(), panicky{}, TaskContext{}); err == nil {
		t.Error("a nil logger must not break panic recovery")
	}
}

func TestRegistry(t *testing.T) {
	t.Parallel()
	r := NewRegistry()
	for _, h := range Builtins(clock.Real{}, nil) {
		if err := r.Register(h); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Register(Noop{}); err == nil {
		t.Error("a duplicate type must be rejected")
	}
	if err := r.Register(Crash{}); err != nil {
		t.Fatal(err)
	}
	if got, want := r.Types(), []string{"crash", "echo", "fail", "flaky", "noop", "sleep"}; !slices.Equal(got, want) {
		t.Errorf("Types = %v, want %v", got, want)
	}
	if _, ok := r.Get("nope"); ok {
		t.Error("Get of an unknown type succeeded")
	}
	for _, h := range Builtins(clock.Real{}, nil) {
		if h.Type() == "crash" {
			t.Error("crash must only be built in when an exit function is given")
		}
	}
	if n := len(Builtins(clock.Real{}, func(int) {})); n != 6 {
		t.Errorf("Builtins with exit = %d handlers, want 6", n)
	}
}

func run(t *testing.T, h Handler, input string, attempt int) (string, error) {
	t.Helper()
	out, err := Execute(t.Context(), h, TaskContext{Input: json.RawMessage(input), AttemptNumber: attempt, TaskID: "task_1", RunID: "run_1", IdempotencyKey: "task_1:1"})
	return string(out), err
}

func TestBuiltins(t *testing.T) {
	t.Parallel()
	if out, err := run(t, Noop{}, `{"a":1}`, 1); err != nil || out != `{"a":1}` {
		t.Errorf("noop = %s, %v", out, err)
	}
	if out, err := run(t, Noop{}, ``, 1); err != nil || out != `{}` {
		t.Errorf("noop of empty input = %s, %v", out, err)
	}
	if _, err := run(t, Fail{}, `{"code":"BOOM","retryable":true}`, 1); !isClass(err, "BOOM", true) {
		t.Errorf("fail retryable = %v", err)
	}
	if _, err := run(t, Fail{}, `{}`, 1); !isClass(err, "FAILED", false) {
		t.Errorf("fail default = %v", err)
	}
	if _, err := run(t, Fail{}, `not json`, 1); !isClass(err, CodeInvalidInput, false) {
		t.Errorf("fail bad input = %v", err)
	}
	if _, err := run(t, Flaky{}, `{"fail_attempts":2}`, 2); !isClass(err, "FLAKY", true) {
		t.Errorf("flaky attempt 2 of 2 = %v", err)
	}
	if out, err := run(t, Flaky{}, `{"fail_attempts":2}`, 3); err != nil || out != `{"fail_attempts":2}` {
		t.Errorf("flaky attempt 3 = %s, %v", out, err)
	}
	out, err := run(t, Echo{}, `{"x":true}`, 4)
	var echoed map[string]any
	if err != nil || json.Unmarshal([]byte(out), &echoed) != nil || echoed["attempt_number"] != float64(4) ||
		echoed["idempotency_key"] != "task_1:1" || echoed["run_id"] != "run_1" {
		t.Errorf("echo = %s, %v", out, err)
	}
	exited := -1
	if _, err := Execute(t.Context(), Crash{Exit: func(c int) { exited = c }}, TaskContext{}); exited != 1 || !isClass(err, "CRASH_RETURNED", false) {
		t.Errorf("crash: exit %d, err %v", exited, err)
	}
}

func TestSleep_HonorsClockAndContext(t *testing.T) {
	t.Parallel()
	fc := clock.NewFake(time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC))
	h := Sleep{Clock: fc}
	done := make(chan string, 1)
	go func() {
		out, _ := Execute(t.Context(), h, TaskContext{Input: json.RawMessage(`{"duration_ms":1500}`)})
		done <- string(out)
	}()
	fc.BlockUntilWaiters(1)
	fc.Advance(1500 * time.Millisecond)
	if out := <-done; out != `{"slept_ms":1500}` {
		t.Errorf("sleep = %s", out)
	}

	ctx, cancel := context.WithCancel(t.Context())
	errc := make(chan error, 1)
	go func() {
		_, err := Execute(ctx, h, TaskContext{Input: json.RawMessage(`{"duration_ms":60000}`)})
		errc <- err
	}()
	fc.BlockUntilWaiters(1)
	cancel()
	var err error
	testutil.WaitFor(t, func() bool {
		select {
		case err = <-errc:
			return true
		default:
			return false
		}
	}, 5*time.Second)
	if code, _ := Classify(err); code != CodeCancelled {
		t.Errorf("cancelled sleep = %v, want CANCELLED", err)
	}
	if _, err := Execute(t.Context(), h, TaskContext{Input: json.RawMessage(`{"duration_ms":-1}`)}); !isClass(err, CodeInvalidInput, false) {
		t.Errorf("negative duration = %v", err)
	}
}

func isClass(err error, code string, retryable bool) bool {
	c, r := Classify(err)
	return err != nil && c == code && r == retryable
}
