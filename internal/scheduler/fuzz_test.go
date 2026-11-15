//go:build integration

package scheduler

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
)

// farFuture claims regardless of available_at, which the database clock set.
var farFuture = time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)

// fuzzSeed fixes the generated DAGs; change it to explore, and quote it when a run fails.
const fuzzSeed = 20261114

// fuzzTask is one generated task and its predetermined outcome if it runs.
type fuzzTask struct {
	key   string
	deps  []string
	runIf model.RunIf
	fails bool
}

// fuzzRun is one generated workflow and the oracle's expectations for it.
type fuzzRun struct {
	tasks       []fuzzTask
	maxParallel int
	id          string
	ids         map[string]string // key → task id
	want        map[string]model.TaskStatus
	wantRun     model.RunStatus
}

// genRun builds a layered DAG of 4–6 layers, each 1–20 wide; every task past the first layer
// depends on 1–3 earlier tasks. Mostly on_success, with on_failure and always branches, 15 %
// predetermined failures, and a per-run limit on a third of the runs.
func genRun(rng *rand.Rand) fuzzRun {
	var r fuzzRun
	var earlier []string
	for layer := range 4 + rng.IntN(3) {
		width := 1 + rng.IntN(20)
		var this []string
		for i := range width {
			ft := fuzzTask{key: fmt.Sprintf("l%d_%d", layer, i), runIf: model.RunIfOnSuccess, fails: rng.IntN(100) < 15}
			if layer > 0 {
				seen := map[string]bool{}
				for range 1 + rng.IntN(3) {
					d := earlier[rng.IntN(len(earlier))]
					if !seen[d] {
						seen[d] = true
						ft.deps = append(ft.deps, d)
					}
				}
				switch p := rng.IntN(100); {
				case p < 10:
					ft.runIf = model.RunIfOnFailure
				case p < 20:
					ft.runIf = model.RunIfAlways
				}
			}
			r.tasks = append(r.tasks, ft)
			this = append(this, ft.key)
		}
		earlier = append(earlier, this...)
	}
	if rng.IntN(3) == 0 {
		r.maxParallel = 1 + rng.IntN(5)
	}
	r.oracle()
	return r
}

// oracle computes the expected final statuses straight from 00E §3.4, independently of the
// statemachine package: tasks are in topological order, so every dependency is decided first.
func (r *fuzzRun) oracle() {
	r.want = map[string]model.TaskStatus{}
	handled := map[string]bool{}
	for _, t := range r.tasks {
		allOK, anyFailed := true, false
		for _, d := range t.deps {
			allOK = allOK && r.want[d] == model.TaskSucceeded
			anyFailed = anyFailed || r.want[d] == model.TaskFailed
		}
		runs := t.runIf == model.RunIfAlways || (t.runIf == model.RunIfOnSuccess && allOK) ||
			(t.runIf == model.RunIfOnFailure && anyFailed)
		switch {
		case !runs:
			r.want[t.key] = model.TaskSkipped
		case t.fails:
			r.want[t.key] = model.TaskFailed
		default:
			r.want[t.key] = model.TaskSucceeded
			if t.runIf != model.RunIfOnSuccess {
				for _, d := range t.deps {
					handled[d] = true
				}
			}
		}
	}
	r.wantRun = model.RunSucceeded
	for _, t := range r.tasks {
		if r.want[t.key] == model.TaskFailed && !handled[t.key] {
			r.wantRun = model.RunFailed
		}
	}
}

// definition returns the createRun arguments: extra top-level fields and the task array.
func (r *fuzzRun) definition() (extra, tasks string) {
	parts := make([]string, len(r.tasks))
	for i, t := range r.tasks {
		deps := []byte("[]")
		if t.deps != nil {
			deps, _ = json.Marshal(t.deps) // []string always marshals
		}
		parts[i] = fmt.Sprintf(`{"id":%q,"type":"t","depends_on":%s,"run_if":%q}`, t.key, deps, t.runIf)
	}
	if r.maxParallel > 0 {
		extra = fmt.Sprintf(`"concurrency":{"max_parallel_tasks":%d},`, r.maxParallel)
	}
	return extra, "[" + strings.Join(parts, ",") + "]"
}

// TestScheduler_Fuzz drives 200 random DAGs (≈10 000 tasks) to completion with the real loop
// and a simulated executor, and checks: every executed task queued exactly once, no task queued
// before its dependencies were terminal, per-run limits held after every tick, and every task
// and run ends where the oracle says.
func TestScheduler_Fuzz(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	rng := rand.New(rand.NewPCG(fuzzSeed, 0)) //nolint:gosec // G404: reproducible test data, not security
	runs, total := make([]*fuzzRun, 200), 0
	for i := range runs {
		r := genRun(rng)
		extra, tasks := r.definition()
		r.id, r.ids = e.createRun(t, extra, tasks)
		runs[i], total = &r, total+len(r.tasks)
	}
	t.Logf("seed %d: %d runs, %d tasks", fuzzSeed, len(runs), total)
	if total < 9000 {
		t.Fatalf("generated %d tasks, want about 10 000", total)
	}

	rounds := 0
	for ; e.count(t, `SELECT count(*) FROM workflow_runs WHERE status IN ('CREATED','RUNNING')`) > 0; rounds++ {
		if rounds > 300 {
			t.Fatalf("seed %d: runs still active after %d rounds", fuzzSeed, rounds)
		}
		e.tick(t)
		checkRunLimits(t, e, runs)
		e.executeQueued(t, runs, rng)
	}

	executed := 0
	for _, r := range runs {
		got := e.statuses(t, r.id)
		for key, want := range r.want {
			if got[key] != want {
				t.Errorf("seed %d run %s task %s: %s, oracle %s", fuzzSeed, r.id, key, got[key], want)
			}
			if want != model.TaskSkipped {
				executed++
			}
		}
		if s := e.runStatus(t, r.id); s != r.wantRun {
			t.Errorf("seed %d run %s: %s, oracle %s", fuzzSeed, r.id, s, r.wantRun)
		}
	}
	dup := e.count(t, `SELECT count(*) FROM (SELECT task_id FROM workflow_events WHERE event_type = 'task.queued'
		GROUP BY task_id HAVING count(*) > 1) d`)
	queuedEvents := e.count(t, `SELECT count(*) FROM workflow_events WHERE event_type = 'task.queued'`)
	if dup != 0 || queuedEvents != executed {
		t.Errorf("seed %d: %d tasks queued more than once; %d task.queued events for %d executed tasks", fuzzSeed, dup, queuedEvents, executed)
	}
	t.Logf("%d rounds, %d tasks executed, %d skipped", rounds, executed, total-executed)
}

// checkRunLimits fails when a limited run holds more QUEUED+RUNNING tasks than it allows.
func checkRunLimits(t *testing.T, e *env, runs []*fuzzRun) {
	t.Helper()
	rows, err := e.pool.Query(t.Context(), `SELECT run_id, count(*) FROM workflow_tasks
		WHERE status IN ('QUEUED','RUNNING') GROUP BY run_id`)
	if err != nil {
		t.Fatal(err)
	}
	active := map[string]int{}
	for rows.Next() {
		var id string
		var n int
		if err := rows.Scan(&id, &n); err != nil {
			t.Fatal(err)
		}
		active[id] = n
	}
	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}
	for _, r := range runs {
		if r.maxParallel > 0 && active[r.id] > r.maxParallel {
			t.Fatalf("seed %d run %s: %d active tasks, max_parallel_tasks %d", fuzzSeed, r.id, active[r.id], r.maxParallel)
		}
	}
}

// executeQueued is the simulated executor: it claims every QUEUED task in one transaction, then
// checks that each one's dependencies are terminal (a dependency claimed in the same batch would
// be RUNNING, i.e. queued too early), then finishes them all with their predetermined outcome.
// The claim order is shuffled, so completions do not follow creation order.
func (e *env) executeQueued(t *testing.T, runs []*fuzzRun, rng *rand.Rand) {
	t.Helper()
	ctx := t.Context()
	var claimed []persistence.TaskView
	err := e.tm.WithTx(ctx, pgx.TxOptions{}, func(tx pgx.Tx) error {
		claimed = nil
		list, err := e.tasks.ClaimQueued(ctx, tx, persistence.ClaimFilter{
			Types: []string{"t"}, Queues: []string{model.DefaultQueue}, Now: farFuture, Limit: 20_000,
		})
		if err != nil {
			return err
		}
		for _, task := range list {
			if _, err := e.attempts.Create(ctx, tx, persistence.NewAttempt{
				TaskID: task.ID, RunID: task.RunID, AttemptNumber: task.AttemptCount + 1, WorkerID: "sim", TimeoutSeconds: 60,
			}); err != nil {
				return err
			}
			if err := e.tasks.Transition(ctx, tx, persistence.TaskTransitionParams{
				TaskID: task.ID, From: model.TaskQueued, To: model.TaskRunning,
				Mutate: func(m *persistence.TaskMutation) { m.IncrementAttempt = true },
			}); err != nil {
				return err
			}
		}
		claimed = list
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	byID := map[string]*fuzzRun{}
	for _, r := range runs {
		byID[r.id] = r
	}
	seen := map[string]map[string]model.TaskStatus{} // run → key → status, after the claim
	for _, task := range claimed {
		st, ok := seen[task.RunID]
		if !ok {
			st = e.statuses(t, task.RunID)
			seen[task.RunID] = st
		}
		for _, d := range task.DependsOn {
			if !terminal(st[d]) {
				t.Fatalf("seed %d run %s: task %s was queued while dependency %s was %s", fuzzSeed, task.RunID, task.Key, d, st[d])
			}
		}
	}
	rng.Shuffle(len(claimed), func(i, j int) { claimed[i], claimed[j] = claimed[j], claimed[i] })
	for _, task := range claimed {
		if err := e.finish(ctx, task.ID, !byID[task.RunID].fails(task.Key)); err != nil {
			t.Fatal(err)
		}
	}
}

func (r *fuzzRun) fails(key string) bool {
	for _, t := range r.tasks {
		if t.key == key {
			return t.fails
		}
	}
	return false
}

func terminal(s model.TaskStatus) bool {
	switch s {
	case model.TaskSucceeded, model.TaskFailed, model.TaskTimedOut, model.TaskCancelled, model.TaskSkipped:
		return true
	}
	return false
}
