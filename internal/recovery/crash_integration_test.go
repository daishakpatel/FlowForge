//go:build integration && failpoint

package recovery

import (
	"fmt"
	"testing"
	"time"

	workerv1 "github.com/daishakpatel/flowforge/internal/gen/proto/worker/v1"
	"github.com/daishakpatel/flowforge/internal/scheduler"
	"github.com/daishakpatel/flowforge/internal/testutil"
	"github.com/daishakpatel/flowforge/internal/testutil/failpoint"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
)

// TestCrashConsistency arms each failpoint in turn, as an error (crash before commit) and as a
// panic (process crash), and drives a run through scheduling, claiming, completing and
// finishing. After the crash the database must satisfy every invariant (no half-committed
// decision); after "restarting" (fresh scheduler loops, the API served again) the run must
// complete with exactly one succeeded attempt.
//
// Not parallel: failpoints are process-wide, and Go runs every sequential test before resuming
// the parallel ones.
func TestCrashConsistency(t *testing.T) {
	for _, name := range failpoint.Names() {
		for _, action := range []string{"error", "panic"} {
			t.Run(name+"/"+action, func(t *testing.T) { crashCase(t, name, action) })
		}
	}
}

func crashCase(t *testing.T, name, action string) {
	e := newEnv(t)
	w := e.worker(t, "w-crash")
	run, ids := e.startRun(t, taskJSON("a", 3, 1000, 1000, 600), taskJSON("b", 3, 1000, 1000, 600))
	t.Cleanup(failpoint.Reset)

	hitsBefore := failpoint.Hits(name)
	failpoint.Enable(name, action)
	pipeline(t, e, w) // crashes somewhere along the way
	failpoint.Disable(name)
	if failpoint.Hits(name) == hitsBefore {
		t.Fatalf("failpoint %s never fired: the pipeline does not reach it", name)
	}
	testutil.AssertInvariants(t, e.srv.Pool)

	e.srv.Restart(t) // the worker API process restarts
	e.build()        // and so does the scheduler
	for i := 0; e.runStatus(t, run) != model.RunSucceeded; i++ {
		if i > 10 {
			t.Fatalf("run did not converge after the crash; tasks a=%s b=%s",
				e.task(t, ids["a"]).Status, e.task(t, ids["b"]).Status)
		}
		pipeline(t, e, w)
	}
	for key, id := range ids {
		if n := e.srv.Scalar(t, `SELECT count(*) FROM task_attempts WHERE task_id = $1 AND status = 'SUCCEEDED'`, id); n != "1" {
			t.Errorf("task %s has %s succeeded attempts, want 1", key, n)
		}
	}
	testutil.AssertInvariants(t, e.srv.Pool)
}

// pipeline runs one round of every step a run goes through, each guarded so a crash (an error
// or a panic) in one step does not stop the round: schedule, claim, complete, finish.
func pipeline(t *testing.T, e *env, w *sim) {
	t.Helper()
	guard := func(step func() error) {
		defer func() { _ = recover() }() // a panicking failpoint stands in for a crashed process
		_ = step()                       // a failing step is the crash; the invariants judge it
	}
	guard(func() error { return e.decide.Once(t.Context(), scheduler.AlwaysLeader{}) })
	var claimed []string
	guard(func() error {
		tasks, err := w.poll(t)
		for _, task := range tasks {
			claimed = append(claimed, task.GetAttemptId())
			guard(func() error { return w.complete(t, task, fmt.Sprintf(`{"attempt":%q}`, task.GetAttemptId())) })
		}
		return err
	})
	guard(func() error { return e.decide.Once(t.Context(), scheduler.AlwaysLeader{}) })
	if len(claimed) == 0 {
		// Nothing claimable: a crashed claim or report leaves the attempt RUNNING with its worker
		// alive, so complete what is RUNNING through a fresh poll on the next round, and let the
		// fake clock carry any retry delay.
		e.clock.Advance(1500 * time.Millisecond)
	}
	completeRunning(t, e, w)
}

// completeRunning reports success for every RUNNING attempt of this env's worker, as a worker
// whose earlier report was lost in a crash would retry it.
func completeRunning(t *testing.T, e *env, w *sim) {
	t.Helper()
	rows, err := e.srv.Pool.Query(t.Context(), `SELECT id, fencing_token FROM task_attempts WHERE status = 'RUNNING' AND worker_id = $1`, w.id)
	if err != nil {
		t.Fatal(err)
	}
	type att struct {
		id    string
		token int64
	}
	var running []att
	for rows.Next() {
		var a att
		if err := rows.Scan(&a.id, &a.token); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		running = append(running, a)
	}
	rows.Close()
	for _, a := range running {
		func() {
			defer func() { _ = recover() }() // still crashing is judged by the invariants
			_ = w.completeAttempt(t, a.id, a.token)
		}()
	}
}

// completeAttempt reports success for an attempt by id and token, as a worker retrying a report.
func (w *sim) completeAttempt(t *testing.T, attemptID string, token int64) error {
	t.Helper()
	_, err := w.c.CompleteTask(t.Context(), &workerv1.CompleteTaskRequest{AttemptId: attemptID, FencingToken: token, ResultJson: []byte(`{}`)})
	return err
}
