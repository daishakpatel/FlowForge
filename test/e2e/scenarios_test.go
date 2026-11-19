//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/daishakpatel/flowforge/internal/testutil"
)

const runTimeout = 90 * time.Second

// TestDiamondAcrossTwoWorkers: a→{b,c}→d completes, b and c run in parallel on both workers,
// and d sees b's result through templating. Phase-1 acceptance: a multi-step DAG completes.
func TestDiamondAcrossTwoWorkers(t *testing.T) {
	h := New(t)
	h.StartAll(2, 1)
	r := h.StartRun(h.CreateWorkflow(fixture("diamond")), nil)
	start := time.Now()
	h.mustStatus("run", h.WaitRun(r.ID, runTimeout).Status, "SUCCEEDED")
	h.metric("diamond_run", time.Since(start))
	workers := map[string]bool{}
	for _, task := range h.Tasks(r.ID, false) {
		h.mustStatus("task "+task.TaskKey, task.Status, "SUCCEEDED")
		for _, a := range h.Attempts(task.ID) {
			workers[a.WorkerID] = true
		}
	}
	if !workers["worker-1"] || !workers["worker-2"] {
		t.Errorf("attempts ran on %v, want both worker-1 and worker-2", workers)
	}
	if d := h.Task(r.ID, "d"); !strings.Contains(string(d.Result), `"b_slept":800`) || !strings.Contains(string(d.Result), `"c_status":"SUCCEEDED"`) {
		t.Errorf("d's result %s lacks b's and c's templated values", d.Result)
	}
	h.AssertHealthy()
}

// TestFanOutRespectsCapacity: 8 parallel tasks on 2 workers of capacity 2 never run more than 2
// at once per worker, but do run in parallel.
func TestFanOutRespectsCapacity(t *testing.T) {
	h := New(t)
	h.StartAll(2, 2)
	r := h.StartRun(h.CreateWorkflow(fixture("fanout")), nil)
	h.mustStatus("run", h.WaitRun(r.ID, runTimeout).Status, "SUCCEEDED")
	type edge struct {
		at    time.Time
		delta int
	}
	perWorker := map[string][]edge{}
	var all []edge
	for _, task := range h.Tasks(r.ID, false) {
		for _, a := range h.Attempts(task.ID) {
			if a.FinishedAt == nil {
				t.Fatalf("attempt %s has no finished_at", a.ID)
			}
			// An end sorts before a start at the same instant: the slot was freed first.
			es := []edge{{a.StartedAt, 1}, {*a.FinishedAt, -1}}
			perWorker[a.WorkerID] = append(perWorker[a.WorkerID], es...)
			all = append(all, es...)
		}
	}
	peak := func(es []edge) int {
		sort.Slice(es, func(i, j int) bool {
			return es[i].at.Before(es[j].at) || (es[i].at.Equal(es[j].at) && es[i].delta < es[j].delta)
		})
		cur, best := 0, 0
		for _, e := range es {
			cur += e.delta
			best = max(best, cur)
		}
		return best
	}
	for w, es := range perWorker {
		if p := peak(es); p > 2 {
			t.Errorf("worker %s ran %d attempts at once, capacity is 2", w, p)
		}
	}
	if p := peak(all); p < 3 || p > 4 {
		t.Errorf("peak parallelism %d, want 3..4 (2 workers × capacity 2)", p)
	}
	h.AssertHealthy()
}

// TestRetryThenSucceed: flaky fails twice with a retryable error and succeeds on attempt 3.
// Phase-1 acceptance: a failed task retries per its policy.
func TestRetryThenSucceed(t *testing.T) {
	h := New(t)
	h.StartAll(1, 1)
	r := h.StartRun(h.CreateWorkflow(fixture("retry")), nil)
	h.mustStatus("run", h.WaitRun(r.ID, runTimeout).Status, "SUCCEEDED")
	atts := h.Attempts(h.Task(r.ID, "flaky").ID)
	if len(atts) != 3 {
		t.Fatalf("flaky has %d attempts, want 3", len(atts))
	}
	for i, a := range atts {
		want, code := "FAILED", "FLAKY"
		if i == 2 {
			want, code = "SUCCEEDED", ""
		}
		if a.Status != want || (code != "" && (a.ErrorCode == nil || *a.ErrorCode != code)) {
			t.Errorf("attempt %d: %s %v, want %s %s", a.AttemptNumber, a.Status, a.ErrorCode, want, code)
		}
	}
	h.AssertHealthy()
}

// TestNonRetryableFailure: a non-retryable error fails the task on its first attempt despite
// max_attempts 3, and the run fails without running the dependent task.
func TestNonRetryableFailure(t *testing.T) {
	h := New(t)
	h.StartAll(1, 1)
	r := h.StartRun(h.CreateWorkflow(fixture("non-retryable")), nil)
	h.mustStatus("run", h.WaitRun(r.ID, runTimeout).Status, "FAILED")
	boom := h.Task(r.ID, "boom")
	h.mustStatus("boom", boom.Status, "FAILED")
	if atts := h.Attempts(boom.ID); len(atts) != 1 || atts[0].ErrorCode == nil || *atts[0].ErrorCode != "BAD_INPUT" {
		t.Errorf("boom's attempts %+v, want one BAD_INPUT", atts)
	}
	if after := h.Task(r.ID, "after"); after.Status == "SUCCEEDED" || after.AttemptCount != 0 {
		t.Errorf("after is %s with %d attempts; it must not run", after.Status, after.AttemptCount)
	}
	h.AssertHealthy()
}

// TestTaskTimeout: a task over its timeout_seconds times out and fails the run.
func TestTaskTimeout(t *testing.T) {
	h := New(t)
	h.StartAll(1, 1)
	r := h.StartRun(h.CreateWorkflow(fixture("timeout")), nil)
	started := time.Now()
	h.mustStatus("run", h.WaitRun(r.ID, runTimeout).Status, "FAILED")
	if d := time.Since(started); d > 10*time.Second {
		t.Errorf("the 1 s timeout took %s to fail the run (the handler sleeps 30 s)", d)
	}
	// Whoever ends the attempt first decides its status (handlers.md, recovery.md): the worker's
	// context ends just before the deadline and reports retryable DEADLINE (task FAILED), unless
	// the scheduler's TimeoutLoop marks it TIMED_OUT first (task TIMED_OUT).
	slow := h.Task(r.ID, "slow")
	atts := h.Attempts(slow.ID)
	byWorker := slow.Status == "FAILED" && len(atts) == 1 && atts[0].Status == "FAILED" && atts[0].ErrorCode != nil && *atts[0].ErrorCode == "DEADLINE"
	byServer := slow.Status == "TIMED_OUT" && len(atts) == 1 && atts[0].Status == "TIMED_OUT"
	if !byWorker && !byServer {
		t.Errorf("slow is %s with attempts %+v; want a DEADLINE failure or a TIMED_OUT", slow.Status, atts)
	}
	h.AssertHealthy()
}

// TestWorkerKilledMidTask: SIGKILL of the worker running a task; the task is reassigned to
// another worker after the lease expires. Phase-1 acceptance: a worker restart is handled
// without corruption.
func TestWorkerKilledMidTask(t *testing.T) {
	h := New(t)
	h.StartAPI()
	h.StartScheduler()
	w1 := h.StartWorker("worker-1", 1)
	r := h.StartRun(h.CreateWorkflow(fixture("chain")), nil)
	first := h.WaitAttempt(r.ID, "a", 30*time.Second, running("worker-1"))
	h.Kill(w1)
	killed := time.Now()
	h.StartWorker("worker-2", 1)
	h.WaitAttempt(r.ID, "a", 30*time.Second, running("worker-2"))
	h.metric("worker_loss_recovery", time.Since(killed))
	h.mustStatus("run", h.WaitRun(r.ID, runTimeout).Status, "SUCCEEDED")
	for _, a := range h.Attempts(h.Task(r.ID, "a").ID) {
		if a.ID == first.ID && a.Status != "ABANDONED" {
			t.Errorf("the killed worker's attempt is %s, want ABANDONED", a.Status)
		}
	}
	h.AssertHealthy()
}

// TestWorkerRestartedWithSameID: the killed worker comes back under the same id before its
// lease expires; its previous incarnation's attempt must still be recovered promptly.
func TestWorkerRestartedWithSameID(t *testing.T) {
	h := New(t)
	h.StartAPI()
	h.StartScheduler()
	w1 := h.StartWorker("worker-1", 1)
	r := h.StartRun(h.CreateWorkflow(fixture("chain")), nil)
	first := h.WaitAttempt(r.ID, "a", 30*time.Second, running("worker-1"))
	killed := time.Now()
	h.Restart(w1)
	h.WaitAttempt(r.ID, "a", 30*time.Second, func(a Attempt) bool { return a.ID != first.ID })
	h.metric("worker_restart_same_id_recovery", time.Since(killed))
	h.mustStatus("run", h.WaitRun(r.ID, runTimeout).Status, "SUCCEEDED")
	h.AssertHealthy()
}

// TestZombieWorkerIsRejected: a SIGSTOPped worker loses its lease, its task is reassigned and
// succeeds elsewhere; after SIGCONT the zombie's result is rejected as stale.
func TestZombieWorkerIsRejected(t *testing.T) {
	h := New(t)
	h.StartAPI()
	h.StartScheduler()
	w1 := h.StartWorker("worker-1", 1)
	r := h.StartRun(h.CreateWorkflow(fixture("chain")), nil)
	first := h.WaitAttempt(r.ID, "a", 30*time.Second, running("worker-1"))
	h.Pause(w1)
	h.StartWorker("worker-2", 1)
	h.WaitAttempt(r.ID, "a", 30*time.Second, func(a Attempt) bool { return a.WorkerID == "worker-2" && a.Status == "SUCCEEDED" })
	h.Resume(w1)
	testutil.WaitFor(t, func() bool {
		raw, _ := os.ReadFile(w1.logPath)
		return strings.Contains(string(raw), "attempt is stale; result dropped")
	}, 30*time.Second)
	h.mustStatus("run", h.WaitRun(r.ID, runTimeout).Status, "SUCCEEDED")
	for _, a := range h.Attempts(h.Task(r.ID, "a").ID) {
		if a.ID == first.ID && a.Status != "ABANDONED" {
			t.Errorf("the zombie's attempt is %s, want ABANDONED", a.Status)
		}
	}
	h.AssertHealthy()
}

// TestSchedulerRestartMidRun: the scheduler is killed while a task runs and restarted after it
// finished; the run resumes and completes. Phase-1 acceptance: a scheduler restart does not lose
// pending state.
func TestSchedulerRestartMidRun(t *testing.T) {
	h := New(t)
	_, sched, _ := h.StartAll(1, 1)
	r := h.StartRun(h.CreateWorkflow(fixture("chain")), nil)
	h.WaitAttempt(r.ID, "b", 30*time.Second, running(""))
	h.Kill(sched)
	h.WaitAttempt(r.ID, "b", 30*time.Second, func(a Attempt) bool { return a.Status == "SUCCEEDED" })
	restarted := time.Now()
	h.Restart(sched)
	h.metric("scheduler_restart_ready", time.Since(restarted))
	h.WaitAttempt(r.ID, "c", 30*time.Second, func(Attempt) bool { return true })
	h.metric("scheduler_restart_recovery", time.Since(restarted))
	h.mustStatus("run", h.WaitRun(r.ID, runTimeout).Status, "SUCCEEDED")
	h.AssertHealthy()
}

// TestAPIRestartMidRun: the API is killed while a worker runs a task; workers reconnect after
// the restart and the run completes.
func TestAPIRestartMidRun(t *testing.T) {
	h := New(t)
	api, _, _ := h.StartAll(1, 1)
	r := h.StartRun(h.CreateWorkflow(fixture("chain")), nil)
	h.WaitAttempt(r.ID, "b", 30*time.Second, running(""))
	restarted := time.Now()
	h.Restart(api)
	h.metric("api_restart_ready", time.Since(restarted))
	h.WaitAttempt(r.ID, "c", 30*time.Second, func(Attempt) bool { return true })
	h.metric("api_restart_recovery", time.Since(restarted))
	h.mustStatus("run", h.WaitRun(r.ID, runTimeout).Status, "SUCCEEDED")
	h.AssertHealthy()
}

// TestDatabasePause: Postgres freezes for 5 s mid-run; every process rides it out and the run
// completes with the invariants intact.
func TestDatabasePause(t *testing.T) {
	h := New(t)
	h.StartAll(1, 1)
	r := h.StartRun(h.CreateWorkflow(fixture("chain")), nil)
	h.WaitAttempt(r.ID, "a", 30*time.Second, running(""))
	h.PausePostgres(5 * time.Second)
	resumed := time.Now()
	h.mustStatus("run", h.WaitRun(r.ID, runTimeout).Status, "SUCCEEDED")
	h.metric("db_pause_run_completion_after_unpause", time.Since(resumed))
	h.AssertHealthy()
}

// TestCancelRun: cancelling a run stops its running attempt and leaves the rest unrun.
func TestCancelRun(t *testing.T) {
	h := New(t)
	h.StartAll(1, 1)
	r := h.StartRun(h.CreateWorkflow(fixture("long-sleep")), nil)
	att := h.WaitAttempt(r.ID, "long", 30*time.Second, running(""))
	cancelled := time.Now()
	h.MustDo("POST", "/api/v1/runs/"+r.ID+"/cancel", nil, nil, http.StatusOK)
	h.mustStatus("run", h.WaitRun(r.ID, 30*time.Second).Status, "CANCELLED")
	h.metric("cancel_to_cancelled", time.Since(cancelled))
	h.WaitAttempt(r.ID, "long", 30*time.Second, func(a Attempt) bool { return a.ID == att.ID && a.Status == "CANCELLED" })
	if after := h.Task(r.ID, "after"); after.AttemptCount != 0 {
		t.Errorf("after ran %d times after the cancel", after.AttemptCount)
	}
	h.AssertHealthy()
}

// TestPauseResume: a paused run finishes its running task but starts nothing new until resumed.
func TestPauseResume(t *testing.T) {
	h := New(t)
	h.StartAll(1, 1)
	r := h.StartRun(h.CreateWorkflow(fixture("chain")), nil)
	h.WaitAttempt(r.ID, "a", 30*time.Second, running(""))
	h.MustDo("POST", "/api/v1/runs/"+r.ID+"/pause", nil, nil, http.StatusOK)
	h.WaitAttempt(r.ID, "a", 30*time.Second, func(a Attempt) bool { return a.Status == "SUCCEEDED" })
	// Negative check over a bounded window: 20 scheduler ticks.
	if testutil.Eventually(func() bool { return len(h.attemptsQuiet(h.Task(r.ID, "b").ID)) > 0 }, 2*time.Second, 100*time.Millisecond) {
		t.Fatal("b started while the run was paused")
	}
	h.mustStatus("run", h.Run(r.ID).Status, "PAUSED")
	h.MustDo("POST", "/api/v1/runs/"+r.ID+"/resume", nil, nil, http.StatusOK)
	h.mustStatus("run", h.WaitRun(r.ID, runTimeout).Status, "SUCCEEDED")
	h.AssertHealthy()
}

// TestRerunAfterFailure: the only worker dies under a one-attempt task, so the task fails; its
// on_failure branch runs and escalates (fails), which fails the run. After a new worker is up, a
// rerun of the task succeeds: the on_failure branch is skipped, the success branch runs, and the
// run succeeds (workflow-semantics.md, handled-failure rule; ADR-008).
func TestRerunAfterFailure(t *testing.T) {
	h := New(t)
	h.StartAPI()
	h.StartScheduler()
	w1 := h.StartWorker("worker-1", 1)
	r := h.StartRun(h.CreateWorkflow(fixture("rerun")), nil)
	h.WaitAttempt(r.ID, "work", 30*time.Second, running("worker-1"))
	h.Kill(w1)
	h.StartWorker("worker-2", 1)
	h.mustStatus("run", h.WaitRun(r.ID, runTimeout).Status, "FAILED")
	h.mustStatus("work", h.Task(r.ID, "work").Status, "FAILED")
	h.mustStatus("investigate", h.Task(r.ID, "investigate").Status, "FAILED")
	h.mustStatus("publish", h.Task(r.ID, "publish").Status, "SKIPPED")

	h.MustDo("POST", "/api/v1/runs/"+r.ID+"/rerun", map[string]any{"task_ids": []string{"work"}, "reason": "worker crashed"}, nil, http.StatusOK)
	h.mustStatus("rerun", h.WaitRun(r.ID, runTimeout).Status, "SUCCEEDED")
	h.mustStatus("work", h.Task(r.ID, "work").Status, "SUCCEEDED")
	h.mustStatus("investigate", h.Task(r.ID, "investigate").Status, "SKIPPED")
	h.mustStatus("publish", h.Task(r.ID, "publish").Status, "SUCCEEDED")
	if all := h.Tasks(r.ID, true); len(all) != 6 {
		t.Errorf("run has %d task rows with superseded ones, want 6 (3 + 3 rerun)", len(all))
	}
	h.AssertHealthy()
}

// TestTemplatingChain: run input flows through two tasks' results into the third's config.
func TestTemplatingChain(t *testing.T) {
	h := New(t)
	h.StartAll(1, 2)
	r := h.StartRun(h.CreateWorkflow(fixture("templating")), map[string]any{"symbol": "AAPL", "window": 15})
	h.mustStatus("run", h.WaitRun(r.ID, runTimeout).Status, "SUCCEEDED")
	var res struct {
		Input struct {
			Headline string `json:"headline"`
			Window   int    `json:"window"`
			Status   string `json:"status"`
		} `json:"input"`
	}
	if err := json.Unmarshal(h.Task(r.ID, "report").Result, &res); err != nil {
		t.Fatal(err)
	}
	if res.Input.Headline != "quote AAPL" || res.Input.Window != 15 || res.Input.Status != "SUCCEEDED" {
		t.Errorf("report saw %+v, want {quote AAPL 15 SUCCEEDED}", res.Input)
	}
	h.AssertHealthy()
}

// TestCronTriggerFires: a trigger created with the CLI fires at the next minute boundary and its
// run completes.
func TestCronTriggerFires(t *testing.T) {
	h := New(t)
	h.StartAll(1, 1)
	h.CreateWorkflow(fixture("diamond"))
	code, out, stderr := h.CLI("-o", "json", "triggers", "create", "--name", "every-minute", "--workflow", "diamond", "--schedule", "* * * * *")
	if code != 0 {
		t.Fatalf("triggers create: exit %d: %s", code, stderr)
	}
	var trg struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal([]byte(out), &trg); err != nil {
		t.Fatal(err)
	}
	var runID string
	testutil.WaitFor(t, func() bool {
		return h.DB.QueryRow(context.Background(), `SELECT id FROM workflow_runs WHERE trigger_id = $1`, trg.ID).Scan(&runID) == nil
	}, 75*time.Second)
	h.mustStatus("cron run", h.WaitRun(runID, runTimeout).Status, "SUCCEEDED")
	h.AssertHealthy()
}

// TestIdempotentStart: two starts with one Idempotency-Key (REST, then CLI) make one run.
func TestIdempotentStart(t *testing.T) {
	h := New(t)
	h.StartAPI()
	wf := h.CreateWorkflow(fixture("diamond"))
	var r1, r2 Run
	h.MustDo("POST", "/api/v1/workflows/"+wf+"/runs", map[string]any{}, &r1, http.StatusCreated, "Idempotency-Key", "nightly-2026-11-18")
	h.MustDo("POST", "/api/v1/workflows/"+wf+"/runs", map[string]any{}, &r2, http.StatusOK, "Idempotency-Key", "nightly-2026-11-18")
	code, out, stderr := h.CLI("-o", "json", "runs", "start", "diamond", "--idempotency-key", "nightly-2026-11-18")
	if code != 0 || !strings.Contains(out, r1.ID) {
		t.Errorf("CLI replay: exit %d, out %s, stderr %s; want run %s", code, out, stderr, r1.ID)
	}
	var n int
	if err := h.DB.QueryRow(context.Background(), `SELECT count(*) FROM workflow_runs`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if r1.ID != r2.ID || n != 1 {
		t.Errorf("runs %s and %s, %d rows; want one run", r1.ID, r2.ID, n)
	}
}

// TestSSEConsumption: `flowforgectl runs watch` follows the run over SSE from its first event to
// run.succeeded, in order.
func TestSSEConsumption(t *testing.T) {
	h := New(t)
	h.StartAll(2, 1)
	r := h.StartRun(h.CreateWorkflow(fixture("diamond")), nil)
	code, out, stderr := h.CLI("-o", "json", "runs", "watch", r.ID)
	if code != 0 {
		t.Fatalf("runs watch: exit %d: %s", code, stderr)
	}
	var types []string
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		var ev struct {
			EventType string `json:"event_type"`
		}
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("event %q: %v", line, err)
		}
		types = append(types, ev.EventType)
	}
	if len(types) == 0 || types[len(types)-1] != "run.succeeded" {
		t.Fatalf("watched %v, want a stream ending in run.succeeded", types)
	}
	if n := strings.Count(strings.Join(types, " "), "task.succeeded"); n != 4 {
		t.Errorf("watched %d task.succeeded events, want 4: %v", n, types)
	}
}

// TestCLIEndToEnd: the CLI drives a real deployment, and rejects a cyclic definition with CYCLE.
// Phase-1 acceptance: invalid DAGs are rejected.
func TestCLIEndToEnd(t *testing.T) {
	h := New(t)
	h.StartAll(1, 2)
	code, _, stderr := h.CLI("workflows", "create", "-f", "../fixtures/workflows/cyclic.json")
	if code != 1 || !strings.Contains(stderr, "CYCLE") {
		t.Errorf("cyclic definition: exit %d, stderr %s; want 1 and CYCLE", code, stderr)
	}
	t.Logf("cyclic definition rejected, exit %d:\n%s", code, stderr)
	if code, _, stderr = h.CLI("workflows", "create", "-f", "../fixtures/workflows/market-data-skeleton.json"); code != 0 {
		t.Fatalf("workflows create: exit %d: %s", code, stderr)
	}
	input := t.TempDir() + "/input.json"
	if err := os.WriteFile(input, []byte(`{"symbols":["AAPL","MSFT"]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	code, out, stderr := h.CLI("-o", "json", "runs", "start", "market-data-skeleton", "--input", input)
	var r Run
	if code != 0 || json.Unmarshal([]byte(out), &r) != nil {
		t.Fatalf("runs start: exit %d, out %s, stderr %s", code, out, stderr)
	}
	h.mustStatus("run", h.WaitRun(r.ID, runTimeout).Status, "SUCCEEDED")
	for _, args := range [][]string{{"runs", "get", r.ID}, {"runs", "tasks", r.ID}, {"runs", "events", r.ID},
		{"runs", "logs", r.ID}, {"workers", "list"}, {"workflows", "list"}, {"api-keys", "list"}} {
		if code, out, stderr := h.CLI(args...); code != 0 || out == "" {
			t.Errorf("%v: exit %d, stderr %s", args, code, stderr)
		}
	}
	if code, out, _ := h.CLI("runs", "tasks", r.ID); !strings.Contains(out, "quality_check") || code != 0 {
		t.Errorf("runs tasks table lacks quality_check:\n%s", out)
	}
	if code, _, stderr := h.CLI("runs", "pause", r.ID); code != 1 || !strings.Contains(stderr, "INVALID_STATE") {
		t.Errorf("pausing a finished run: exit %d, stderr %s; want 1 and INVALID_STATE", code, stderr)
	}
	h.AssertHealthy()
}

// TestDemoScript: scripts/demo/engine-demo.sh runs cleanly against a live deployment, twice
// (the second run registers new versions of the same workflows).
func TestDemoScript(t *testing.T) {
	h := New(t)
	h.StartAll(2, 2)
	for i := 1; i <= 2; i++ {
		cmd := exec.CommandContext(t.Context(), "bash", "../../scripts/demo/engine-demo.sh")
		cmd.Env = append(os.Environ(), "FF_API_URL="+h.APIURL, "FF_API_KEY="+h.AdminKey, "FLOWFORGECTL="+filepath.Join(binDir, "flowforgectl"))
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("engine-demo.sh, run %d: %v\n%s", i, err, out)
		}
		for _, want := range []string{"CYCLE", "run.succeeded", "quality_check", "worker-1"} {
			if !strings.Contains(string(out), want) {
				t.Errorf("run %d: demo output lacks %q:\n%s", i, want, out)
			}
		}
	}
	h.AssertHealthy()
}
