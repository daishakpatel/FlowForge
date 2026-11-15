//go:build integration

package scheduler

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/daishakpatel/flowforge/internal/workflow/model"
)

// succeed claims a QUEUED task and completes it with result, as a worker would.
func (e *env) succeed(t *testing.T, taskID, result string) {
	t.Helper()
	if err := e.claim(t.Context(), taskID); err != nil {
		t.Fatal(err)
	}
	if err := e.finishResult(t.Context(), taskID, true, json.RawMessage(result)); err != nil {
		t.Fatal(err)
	}
}

// inputOf returns a task's resolved input.
func (e *env) inputOf(t *testing.T, taskID string) json.RawMessage {
	t.Helper()
	v, err := e.tasks.Get(t.Context(), e.pool, taskID)
	if err != nil {
		t.Fatal(err)
	}
	return v.Input
}

// sameJSON compares JSON by value (jsonb reorders keys).
func sameJSON(t *testing.T, got json.RawMessage, want string) {
	t.Helper()
	var g, w any
	for raw, v := range map[string]*any{string(got): &g, want: &w} {
		dec := json.NewDecoder(bytes.NewReader([]byte(raw)))
		dec.UseNumber()
		if err := dec.Decode(v); err != nil {
			t.Fatalf("decode %s: %v", raw, err)
		}
	}
	if !reflect.DeepEqual(g, w) {
		t.Errorf("JSON = %s; want %s", got, want)
	}
}

// TestTemplating_ChainPassesResults: a 3-task chain whose configs read the run input and
// upstream results (task 03.06 §8), including a grandparent's result; whole-value placeholders
// keep their JSON type.
func TestTemplating_ChainPassesResults(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	run, id := e.createRunInput(t, "", `[
	  {"id":"fetch","type":"t","config":{"symbols":"${run.input.symbols}"}},
	  {"id":"process","type":"t","depends_on":["fetch"],
	   "config":{"from":"${tasks.fetch.result.window_start}","rows":"${tasks.fetch.result.rows}","note":"run ${run.input.label}"}},
	  {"id":"report","type":"t","depends_on":["process"],
	   "config":{"total":"${tasks.process.result.total}","fetched":"${tasks.fetch.result.rows}","after":"${tasks.process.status}","px":"${tasks.process.result.px[1]}"}}]`,
		json.RawMessage(`{"symbols":["AAPL","MSFT"],"label":"hourly"}`))
	e.tick(t)
	sameJSON(t, e.inputOf(t, id["fetch"]), `{"symbols":["AAPL","MSFT"]}`)
	e.succeed(t, id["fetch"], `{"window_start":"2026-11-15T10:00:00Z","rows":42}`)
	e.tick(t)
	sameJSON(t, e.inputOf(t, id["process"]), `{"from":"2026-11-15T10:00:00Z","rows":42,"note":"run hourly"}`)
	e.succeed(t, id["process"], `{"total":42.5,"px":[101.5,{"bid":102}]}`)
	e.tick(t)
	sameJSON(t, e.inputOf(t, id["report"]), `{"total":42.5,"fetched":42,"after":"SUCCEEDED","px":{"bid":102}}`)
	e.succeed(t, id["report"], `{}`)
	e.tick(t)
	if st := e.runStatus(t, run); st != model.RunSucceeded {
		t.Errorf("run %s; want SUCCEEDED", st)
	}
}

// TestTemplating_UnresolvedPlaceholderFailsWithoutRetries: a placeholder whose path is missing
// at run time fails the task READY → FAILED with INVALID_INPUT, with no attempt, although its
// retry policy allows three (task 03.06 §8).
func TestTemplating_UnresolvedPlaceholderFailsWithoutRetries(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	run, id := e.createRun(t, "", `[{"id":"a","type":"t"},
	  {"id":"b","type":"t","depends_on":["a"],"retry":{"max_attempts":3},"config":{"x":"${tasks.a.result.missing}"}}]`)
	e.tick(t)
	e.succeed(t, id["a"], `{"present":1}`)
	e.tick(t)
	e.tick(t)
	b, err := e.tasks.Get(t.Context(), e.pool, id["b"])
	if err != nil {
		t.Fatal(err)
	}
	if b.Status != model.TaskFailed || b.AttemptCount != 0 || b.Input != nil {
		t.Errorf("b = %s, attempts %d, input %s; want FAILED, 0, none", b.Status, b.AttemptCount, b.Input)
	}
	if n := e.count(t, `SELECT count(*) FROM task_attempts WHERE task_id = $1`, id["b"]); n != 0 {
		t.Errorf("b has %d attempts", n)
	}
	if n := e.count(t, `SELECT count(*) FROM workflow_events WHERE task_id = $1 AND event_type = 'task.failed'
	  AND payload->>'error_code' = 'INVALID_INPUT'`, id["b"]); n != 1 {
		t.Errorf("task.failed INVALID_INPUT events for b: %d", n)
	}
	if st := e.runStatus(t, run); st != model.RunFailed {
		t.Errorf("run %s; want FAILED (b's failure is unhandled)", st)
	}
}

// marketDataTasks is a market-data-like DAG (task 03.06 §5 and §8): quality-check fails, so its
// on_failure branch runs, its success branch (and what follows it) is skipped, report (always)
// and the matching expression branch run, and the non-matching one is skipped.
const marketDataTasks = `[
  {"id":"fetch","type":"ingest"},
  {"id":"validate","type":"t","depends_on":["fetch"]},
  {"id":"process","type":"t","depends_on":["validate"],"config":{"rows":"${tasks.fetch.result.rows}"}},
  {"id":"quality-check","type":"t","depends_on":["process"]},
  {"id":"ai-investigate","type":"t","depends_on":["quality-check"],"run_if":"on_failure",
   "config":{"failed":"${tasks.quality-check.status}"}},
  {"id":"publish","type":"t","depends_on":["quality-check"]},
  {"id":"notify","type":"t","depends_on":["publish"]},
  {"id":"report","type":"t","depends_on":["quality-check"],"run_if":"always"},
  {"id":"alert","type":"t","depends_on":["quality-check"],"run_if":"expr",
   "run_if_expr":"tasks.quality-check.status == \"FAILED\" && tasks.fetch.result.rows >= run.input.min_rows"},
  {"id":"escalate","type":"t","depends_on":["quality-check"],"run_if":"expr",
   "run_if_expr":"tasks.fetch.result.rows > 1000 || !tasks.validate.status == \"SUCCEEDED\""}]`

func TestConditional_MarketDataFailureBranch(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	run, id := e.createRunInput(t, "", marketDataTasks, json.RawMessage(`{"min_rows":10}`))
	e.tick(t)
	e.succeed(t, id["fetch"], `{"rows":42}`)
	for _, k := range []string{"validate", "process"} {
		e.tick(t)
		e.succeed(t, id[k], `{}`)
	}
	sameJSON(t, e.inputOf(t, id["process"]), `{"rows":42}`)
	e.tick(t)
	e.run(t, id["quality-check"], false)
	e.tick(t)
	want := map[string]model.TaskStatus{
		"ai-investigate": model.TaskQueued, "publish": model.TaskSkipped, "notify": model.TaskSkipped,
		"report": model.TaskQueued, "alert": model.TaskQueued, "escalate": model.TaskSkipped,
	}
	got := e.statuses(t, run)
	for k, st := range want {
		if got[k] != st {
			t.Errorf("%s = %s; want %s", k, got[k], st)
		}
	}
	sameJSON(t, e.inputOf(t, id["ai-investigate"]), `{"failed":"FAILED"}`)
	for _, k := range []string{"ai-investigate", "report", "alert"} {
		e.succeed(t, id[k], `{}`)
	}
	e.tick(t)
	// quality-check's failure is handled by ai-investigate (on_failure, SUCCEEDED): 00E §3.4.
	if st := e.runStatus(t, run); st != model.RunSucceeded {
		t.Errorf("run %s; want SUCCEEDED by the handled-failure rule", st)
	}
}
