//go:build integration

package scheduler

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/persistence/pg"
	"github.com/daishakpatel/flowforge/internal/testutil/epochguard"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
)

// TestEpochGuard_CatchesAWriteWithoutAssertEpoch proves the AssertEpoch coverage guard (task
// 04.07) can fail: an injected scheduler code path that locks and moves a task without asserting
// the epoch first is reported, naming the statement; the same path asserting first is not. The
// guard wraps the leader loops of the scheduler, recovery, outbox and cron integration tests
// (their envs call Check in t.Cleanup), so a loop that skips AssertEpoch fails those suites.
func TestEpochGuard_CatchesAWriteWithoutAssertEpoch(t *testing.T) {
	e := newEnv(t)
	run, ids := e.createRun(t, "", tasksJSON("a"))
	queue := func(tx pgx.Tx) error {
		if _, err := (pg.SchedulerRepo{}).LockRun(t.Context(), tx, run); err != nil {
			return err
		}
		return e.tasks.Transition(t.Context(), tx, persistence.TaskTransitionParams{TaskID: ids["a"], From: model.TaskPending, To: model.TaskReady})
	}

	unfenced := epochguard.New(e.tm)
	if err := unfenced.WithTx(t.Context(), pgx.TxOptions{}, queue); err != nil {
		t.Fatal(err)
	}
	v := unfenced.Violations()
	if len(v) != 1 || !strings.Contains(v[0], "LockRun") {
		t.Fatalf("violations = %q, want one naming LockRun", v)
	}

	fenced := epochguard.New(e.tm)
	if err := fenced.WithTx(t.Context(), pgx.TxOptions{}, func(tx pgx.Tx) error {
		if err := (pg.ClusterRepo{}).AssertEpoch(t.Context(), tx, 0); err != nil {
			return err
		}
		_, err := (pg.SchedulerRepo{}).LockRun(t.Context(), tx, run)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if v := fenced.Violations(); len(v) != 0 || fenced.Transactions() != 1 {
		t.Fatalf("fenced path: violations %q, transactions %d", v, fenced.Transactions())
	}
}

// TestEpochGuard_SeesTheDecisionLoop: the env's guard watches the DecisionLoop's transactions,
// so the scheduler suite really runs under it.
func TestEpochGuard_SeesTheDecisionLoop(t *testing.T) {
	e := newEnv(t)
	e.createRun(t, "", tasksJSON("a", "b:a"))
	if err := e.loop.Once(t.Context(), AlwaysLeader{}); err != nil {
		t.Fatal(err)
	}
	if n := e.guard.Transactions(); n == 0 {
		t.Fatal("the guard saw no DecisionLoop transaction")
	}
}
