package model

import (
	"encoding/json"
	"errors"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/daishakpatel/flowforge/internal/apperrors"
)

func TestDecode_00EExample(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile("testdata/market-data-hourly.json")
	if err != nil {
		t.Fatal(err)
	}
	d, err := Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	if d.Name != "market-data-hourly" || d.Version != 1 || d.TimeoutSeconds != 3600 || d.OnFailure != OnFailureFail ||
		d.Concurrency.MaxParallelTasks != 8 || len(d.Tasks) != 1 {
		t.Fatalf("definition = %+v", d)
	}
	task := d.Tasks[0]
	want := RetryPolicy{MaxAttempts: 3, Strategy: StrategyExponential, InitialDelayMS: 1000, MaxDelayMS: 60000,
		Multiplier: 2.0, Jitter: 0.2, NonRetryableErrors: []string{"INVALID_INPUT"}}
	if task.ID != "fetch" || task.Type != "market-data-ingest" || task.Queue != "market-data" || task.TimeoutSeconds != 120 ||
		task.RunIf != RunIfOnSuccess || len(task.DependsOn) != 0 || !retryEqual(task.Retry, want) {
		t.Fatalf("task = %+v", task)
	}
	var cfg map[string]any
	if err := json.Unmarshal(task.Config, &cfg); err != nil || cfg["window_minutes"] != 5.0 {
		t.Errorf("config kept raw: %s (%v)", task.Config, err)
	}
	if task.Retry.InitialDelay() != time.Second || task.Retry.MaxDelay() != time.Minute {
		t.Errorf("durations = %v, %v", task.Retry.InitialDelay(), task.Retry.MaxDelay())
	}
}

func retryEqual(a, b RetryPolicy) bool {
	return a.MaxAttempts == b.MaxAttempts && a.Strategy == b.Strategy && a.InitialDelayMS == b.InitialDelayMS &&
		a.MaxDelayMS == b.MaxDelayMS && a.Multiplier == b.Multiplier && a.Jitter == b.Jitter &&
		slices.Equal(a.NonRetryableErrors, b.NonRetryableErrors)
}

func TestDecode_DefaultsForAbsentFields(t *testing.T) {
	t.Parallel()
	d, err := Decode([]byte(`{"name":"wf","version":1,"tasks":[{"id":"a","type":"noop"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if d.TimeoutSeconds != DefaultWorkflowTimeout || d.OnFailure != DefaultOnFailure || d.Concurrency.MaxParallelTasks != DefaultMaxParallelTasks {
		t.Errorf("workflow defaults = %+v", d)
	}
	task := d.Tasks[0]
	want := RetryPolicy{MaxAttempts: DefaultMaxAttempts, Strategy: DefaultStrategy, InitialDelayMS: DefaultInitialDelayMS,
		MaxDelayMS: DefaultMaxDelayMS, Multiplier: DefaultMultiplier, Jitter: DefaultJitter}
	if task.Queue != DefaultQueue || task.TimeoutSeconds != DefaultTaskTimeoutSeconds || task.RunIf != DefaultRunIf ||
		task.DependsOn == nil || len(task.DependsOn) != 0 || !retryEqual(task.Retry, want) {
		t.Errorf("task defaults = %+v", task)
	}
	// An empty retry object and an empty concurrency object default field by field too.
	d, err = Decode([]byte(`{"name":"wf","version":1,"concurrency":{},"tasks":[{"id":"a","type":"t","retry":{"max_attempts":4}}]}`))
	if err != nil || d.Tasks[0].Retry.MaxAttempts != 4 || d.Tasks[0].Retry.Strategy != DefaultStrategy || d.Concurrency.MaxParallelTasks != 0 {
		t.Errorf("partial objects: %+v, %v", d, err)
	}
}

// TestDecode_ExplicitZeroKept: an explicit zero must survive so dag.Validate can reject it.
func TestDecode_ExplicitZeroKept(t *testing.T) {
	t.Parallel()
	d, err := Decode([]byte(`{"name":"wf","version":1,"tasks":[{"id":"a","type":"t","timeout_seconds":0,
		"retry":{"max_attempts":0,"multiplier":0,"initial_delay_ms":0}}]}`))
	if err != nil {
		t.Fatal(err)
	}
	r := d.Tasks[0].Retry
	if d.Tasks[0].TimeoutSeconds != 0 || r.MaxAttempts != 0 || r.Multiplier != 0 || r.InitialDelayMS != 0 {
		t.Errorf("explicit zeros replaced by defaults: %+v", d.Tasks[0])
	}
}

func TestDecode_Rejects(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		body string
		code apperrors.Code
	}{
		"unknown top-level field": {`{"name":"wf","version":1,"tasks":[],"owner":"x"}`, apperrors.CodeInvalidArgument},
		"unknown task field":      {`{"name":"wf","version":1,"tasks":[{"id":"a","type":"t","retries":3}]}`, apperrors.CodeInvalidArgument},
		"unknown retry field":     {`{"name":"wf","version":1,"tasks":[{"id":"a","type":"t","retry":{"attempts":3}}]}`, apperrors.CodeInvalidArgument},
		"wrong type":              {`{"name":"wf","version":"1","tasks":[]}`, apperrors.CodeInvalidArgument},
		"not an object":           {`[1,2]`, apperrors.CodeInvalidArgument},
		"malformed":               {`{"name":`, apperrors.CodeInvalidArgument},
		"empty":                   {``, apperrors.CodeInvalidArgument},
		"trailing data":           {`{"name":"wf","version":1,"tasks":[]} {}`, apperrors.CodeInvalidArgument},
		"too large":               {`{"name":"` + strings.Repeat("x", MaxDefinitionBytes) + `"}`, apperrors.CodePayloadTooLarge},
	} {
		_, err := Decode([]byte(tc.body))
		if apperrors.CodeOf(err) != tc.code {
			t.Errorf("%s: err = %v, want %s", name, err, tc.code)
		}
	}
	// The JSON error stays reachable for callers that want the position.
	_, err := Decode([]byte(`{"name":"wf","version":"1"}`))
	if _, ok := errors.AsType[*json.UnmarshalTypeError](err); !ok {
		t.Errorf("want a wrapped *json.UnmarshalTypeError, got %v", err)
	}
}

func TestDecode_KeepsInvalidEnumsForValidation(t *testing.T) {
	t.Parallel()
	d, err := Decode([]byte(`{"name":"wf","version":1,"on_failure":"retry","tasks":[{"id":"a","type":"t","run_if":"sometimes","retry":{"strategy":"linear"}}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if d.OnFailure != "retry" || d.Tasks[0].RunIf != "sometimes" || d.Tasks[0].Retry.Strategy != "linear" {
		t.Errorf("enum values must pass through unchanged for dag.Validate: %+v", d)
	}
}

func TestStatusLists_Match00E(t *testing.T) {
	t.Parallel()
	var tasks, attempts, runs []string
	for _, s := range TaskStatuses() {
		tasks = append(tasks, string(s))
	}
	for _, s := range AttemptStatuses() {
		attempts = append(attempts, string(s))
	}
	for _, s := range RunStatuses() {
		runs = append(runs, string(s))
	}
	// Literal copies of the CHECK constraints in 00E §2.
	if got := strings.Join(tasks, ","); got != "PENDING,READY,QUEUED,RUNNING,RETRY_WAIT,SUCCEEDED,FAILED,TIMED_OUT,CANCELLED,SKIPPED" {
		t.Errorf("task statuses = %s", got)
	}
	if got := strings.Join(attempts, ","); got != "RUNNING,SUCCEEDED,FAILED,TIMED_OUT,ABANDONED,CANCELLED" {
		t.Errorf("attempt statuses = %s", got)
	}
	if got := strings.Join(runs, ","); got != "CREATED,RUNNING,PAUSED,SUCCEEDED,FAILED,CANCELLED,NEEDS_ATTENTION" {
		t.Errorf("run statuses = %s", got)
	}
}
