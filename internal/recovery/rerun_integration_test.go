//go:build integration

package recovery

import (
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/daishakpatel/flowforge/internal/api/service"
	"github.com/daishakpatel/flowforge/internal/apperrors"
	"github.com/daishakpatel/flowforge/internal/testutil"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
)

// rerunDAG: quality-check fails and nothing handles it, so the run needs attention. Its success
// branch publish is skipped; cleanup (always, after publish) succeeds anyway and is a descendant
// of quality-check that a rerun keeps; archive is a sibling the rerun must not touch.
var rerunDAG = []string{
	`{"id":"fetch","type":"t"}`,
	`{"id":"archive","type":"t","depends_on":["fetch"]}`,
	`{"id":"quality-check","type":"t","depends_on":["fetch"],"retry":{"max_attempts":1}}`,
	`{"id":"publish","type":"t","depends_on":["quality-check"],"config":{"checked":"${tasks.quality-check.result.ok}"}}`,
	`{"id":"cleanup","type":"t","depends_on":["publish"],"run_if":"always"}`,
}

// TestRerun_CreatesNewGenerationAndCompletes covers task 03.06 §8's rerun integration test: new
// rows with supersedes_task_id, originals unchanged, only non-succeeded descendants recreated,
// the run back to RUNNING, a concurrent and a repeated identical request creating no duplicate,
// both generations in the history, and the run then completing through the new rows.
func TestRerun_CreatesNewGenerationAndCompletes(t *testing.T) {
	e := newEnv(t)
	w := e.worker(t, "worker-1")
	run, id := e.startRunWith(t, `"on_failure":"needs_attention",`, rerunDAG...)
	once(t, e.decide)
	must(t, w.complete(t, w.mustPoll(t, 1)[0], `{}`)) // fetch
	once(t, e.decide)
	for _, task := range w.mustPoll(t, 2) { // archive, quality-check
		if task.GetTaskId() == id["quality-check"] {
			must(t, w.fail(t, task, "BAD_DATA", false))
		} else {
			must(t, w.complete(t, task, `{}`))
		}
	}
	once(t, e.decide) // publish SKIPPED; cleanup (always) queued
	must(t, w.complete(t, w.mustPoll(t, 1)[0], `{}`))
	once(t, e.decide)
	if st := e.runStatus(t, run); st != model.RunNeedsAttention {
		t.Fatalf("run %s before the rerun; want NEEDS_ATTENTION", st)
	}
	snapshot := func(k string) string {
		v := e.task(t, id[k])
		return fmt.Sprintf("%s/v%d/a%d", v.Status, v.Version, v.AttemptCount)
	}
	before := map[string]string{}
	for _, k := range []string{"quality-check", "publish", "cleanup", "archive"} {
		before[k] = snapshot(k)
	}

	// A double click: two identical requests at once. One commits; the other is rejected.
	params := service.RerunParams{RunID: run, TaskKeys: []string{"quality-check"}, Reason: "vendor data fixed", Actor: "ops"}
	var wg sync.WaitGroup
	results := make([][]string, 2)
	errs := make([]error, 2)
	for i := range 2 {
		wg.Go(func() { results[i], errs[i] = e.runs.Rerun(t.Context(), params) })
	}
	wg.Wait()
	if (errs[0] == nil) == (errs[1] == nil) {
		t.Fatalf("concurrent reruns: errors %v / %v; want exactly one to succeed", errs[0], errs[1])
	}
	created, lost := results[0], errs[1]
	if errs[0] != nil {
		created, lost = results[1], errs[0]
	}
	if code := apperrors.CodeOf(lost); code != apperrors.CodeInvalidState && code != apperrors.CodeConflict {
		t.Errorf("losing rerun: %v; want INVALID_STATE or CONFLICT", lost)
	}
	if _, err := e.runs.Rerun(t.Context(), params); apperrors.CodeOf(err) != apperrors.CodeInvalidState {
		t.Errorf("repeated rerun: %v; want INVALID_STATE", err)
	}

	// New rows supersede quality-check and the skipped publish; cleanup (succeeded) and archive
	// (not a descendant) are kept. Originals are unchanged.
	if len(created) != 2 {
		t.Fatalf("created %v; want 2 rows", created)
	}
	for i, k := range []string{"quality-check", "publish"} {
		v := e.task(t, created[i])
		if v.Key != k || v.SupersedesTaskID != id[k] || v.Status != model.TaskPending || v.AttemptCount != 0 || v.Input != nil {
			t.Errorf("new %s row = %+v", k, v)
		}
	}
	for k, was := range before {
		if now := snapshot(k); now != was {
			t.Errorf("original %s changed: %s → %s", k, was, now)
		}
	}
	all, err := e.runs.Tasks(t.Context(), run)
	if err != nil {
		t.Fatal(err)
	}
	heads, err := e.runs.HeadTasks(t.Context(), run)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 7 || len(heads) != 5 {
		t.Errorf("history %d rows, heads %d; want 7 and 5", len(all), len(heads))
	}
	r, err := e.runs.Get(t.Context(), run)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != model.RunRunning || r.CompletedAt != nil {
		t.Errorf("run after rerun: %s, completed_at %v", r.Status, r.CompletedAt)
	}
	checkResumedEvent(t, e, run, []string{id["quality-check"], id["publish"]}, created)
	testutil.AssertInvariants(t, e.srv.Pool)

	// The scheduler runs the new generation through head-instance semantics.
	e.syncClock(t) // the new rows' available_at is the database's now()
	once(t, e.decide)
	qc := w.mustPoll(t, 1)[0]
	if qc.GetTaskId() != created[0] {
		t.Fatalf("claimed %s; want the new quality-check row %s", qc.GetTaskId(), created[0])
	}
	must(t, w.complete(t, qc, `{"ok":true}`))
	once(t, e.decide)
	pub := w.mustPoll(t, 1)[0]
	var in map[string]any
	if err := json.Unmarshal(pub.GetInputJson(), &in); err != nil || pub.GetTaskId() != created[1] || in["checked"] != true {
		t.Errorf("publish claim: task %s, input %s (%v)", pub.GetTaskId(), pub.GetInputJson(), err)
	}
	must(t, w.complete(t, pub, `{}`))
	once(t, e.decide)
	if st := e.runStatus(t, run); st != model.RunSucceeded {
		t.Errorf("run %s after the rerun completed; want SUCCEEDED", st)
	}
	if n := e.count(t, `SELECT count(*) FROM workflow_tasks WHERE run_id = $1`, run); n != 7 {
		t.Errorf("%d task rows at the end; want 7", n)
	}
	testutil.AssertInvariants(t, e.srv.Pool)
}

// TestRerun_RetryOfFailedRun: retry reruns every failed task of a FAILED run, and the skipped
// task after it.
func TestRerun_RetryOfFailedRun(t *testing.T) {
	e := newEnv(t)
	w := e.worker(t, "worker-1")
	run, id := e.startRun(t, `{"id":"a","type":"t","retry":{"max_attempts":1}}`, `{"id":"b","type":"t","depends_on":["a"]}`)
	once(t, e.decide)
	must(t, w.fail(t, w.mustPoll(t, 1)[0], "BOOM", false))
	once(t, e.decide)
	if st := e.runStatus(t, run); st != model.RunFailed {
		t.Fatalf("run %s; want FAILED", st)
	}
	created, err := e.runs.Retry(t.Context(), run, "", "ops")
	if err != nil || len(created) != 2 {
		t.Fatalf("retry: %v, %v; want a and the skipped b", created, err)
	}
	if e.task(t, created[1]).SupersedesTaskID != id["b"] {
		t.Errorf("retry did not recreate the skipped b")
	}
	e.syncClock(t)
	once(t, e.decide)
	must(t, w.complete(t, w.mustPoll(t, 1)[0], `{}`))
	once(t, e.decide)
	must(t, w.complete(t, w.mustPoll(t, 1)[0], `{}`))
	once(t, e.decide)
	if st := e.runStatus(t, run); st != model.RunSucceeded {
		t.Errorf("run %s after retry; want SUCCEEDED", st)
	}
}

func checkResumedEvent(t *testing.T, e *env, run string, superseded, created []string) {
	t.Helper()
	var raw []byte
	if err := e.srv.Pool.QueryRow(t.Context(), `SELECT payload FROM workflow_events
	  WHERE run_id = $1 AND event_type = 'run.resumed' ORDER BY id DESC LIMIT 1`, run).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var p struct {
		Reason     string   `json:"reason"`
		Actor      string   `json:"actor"`
		Superseded []string `json:"superseded_task_ids"`
		Rerun      []string `json:"rerun_task_ids"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatal(err)
	}
	if p.Reason != "vendor data fixed" || p.Actor != "ops" || !slices.Equal(p.Superseded, superseded) || !slices.Equal(p.Rerun, created) {
		t.Errorf("run.resumed payload = %s", raw)
	}
}

func (e *env) count(t *testing.T, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := e.srv.Pool.QueryRow(t.Context(), sql, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return n
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
