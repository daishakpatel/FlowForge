//go:build e2e

package e2e

import (
	"context"
	"math/rand/v2"
	"os"
	"strconv"
	"testing"
	"time"
)

// envInt reads an integer knob of the suite (FF_E2E_*), or def.
func envInt(name string, def int64) int64 {
	if v, err := strconv.ParseInt(os.Getenv(name), 10, 64); err == nil {
		return v
	}
	return def
}

// TestSoakChaos: FF_E2E_SOAK_RUNS (default 500) runs of the 6-task soak DAG on 4 workers while
// chaos SIGKILLs a random worker every 10 s (restarting it under the same id) and SIGKILLs and
// restarts the scheduler every 30 s. Every run must succeed, no task may have two SUCCEEDED
// attempts, and the DB invariants must hold. The RNG seed is FF_E2E_SEED (default: the clock) and
// is logged, so a failing schedule can be replayed.
func TestSoakChaos(t *testing.T) {
	if testing.Short() {
		t.Skip("soak: skipped with -short")
	}
	runs := int(envInt("FF_E2E_SOAK_RUNS", 500))
	seed := envInt("FF_E2E_SEED", time.Now().UnixNano())
	t.Logf("SOAK seed=%d runs=%d", seed, runs)
	rng := rand.New(rand.NewPCG(uint64(seed), 0)) //nolint:gosec // chaos schedule, not security
	h := New(t)
	_, sched, workers := h.StartAll(4, 4)
	wf := h.CreateWorkflow(fixture("soak"))

	start := time.Now()
	for i := range runs {
		h.StartRun(wf, map[string]any{"i": i})
	}
	h.metric("soak_start_runs", time.Since(start))
	const budget = 15 * time.Minute
	nextKill, nextSched := start.Add(10*time.Second), start.Add(30*time.Second)
	kills, restarts := 0, 0
	for {
		var done int
		if err := h.DB.QueryRow(context.Background(), `SELECT count(*) FROM workflow_runs
			WHERE status IN ('SUCCEEDED', 'FAILED', 'CANCELLED', 'NEEDS_ATTENTION')`).Scan(&done); err != nil {
			t.Fatal(err)
		}
		if done == runs {
			break
		}
		now := time.Now()
		if now.Sub(start) > budget {
			t.Fatalf("soak: %d of %d runs terminal after %s (seed %d)", done, runs, budget, seed)
		}
		if now.After(nextKill) {
			w := workers[rng.IntN(len(workers))]
			t.Logf("chaos %s: SIGKILL and restart %s (%d/%d runs done)", now.Sub(start).Round(time.Second), w.Name, done, runs)
			h.Restart(w)
			kills++
			nextKill = nextKill.Add(10 * time.Second)
		}
		if now.After(nextSched) {
			t.Logf("chaos %s: SIGKILL and restart the scheduler (%d/%d runs done)", now.Sub(start).Round(time.Second), done, runs)
			h.Restart(sched)
			restarts++
			nextSched = nextSched.Add(30 * time.Second)
		}
		time.Sleep(200 * time.Millisecond)
	}
	h.metric("soak_duration", time.Since(start))
	t.Logf("SOAK seed=%d runs=%d worker_kills=%d scheduler_restarts=%d", seed, runs, kills, restarts)

	var succeeded, abandoned int
	if err := h.DB.QueryRow(context.Background(), `SELECT
		(SELECT count(*) FROM workflow_runs WHERE status = 'SUCCEEDED'),
		(SELECT count(*) FROM task_attempts WHERE status = 'ABANDONED')`).Scan(&succeeded, &abandoned); err != nil {
		t.Fatal(err)
	}
	t.Logf("SOAK succeeded_runs=%d abandoned_attempts=%d", succeeded, abandoned)
	if succeeded != runs {
		t.Errorf("%d of %d runs succeeded (seed %d); tasks:%s", succeeded, runs, seed, h.describeUnsucceeded())
	}
	h.AssertHealthy()
}

// describeUnsucceeded describes the first few runs that did not succeed.
func (h *Harness) describeUnsucceeded() string {
	rows, err := h.DB.Query(context.Background(), `SELECT id FROM workflow_runs WHERE status <> 'SUCCEEDED' ORDER BY created_at LIMIT 3`)
	if err != nil {
		return err.Error()
	}
	var ids []string
	for rows.Next() {
		var id string
		if rows.Scan(&id) == nil {
			ids = append(ids, id)
		}
	}
	rows.Close()
	out := ""
	for _, id := range ids {
		out += "\n run " + id + ":" + h.describe(id)
	}
	return out
}
