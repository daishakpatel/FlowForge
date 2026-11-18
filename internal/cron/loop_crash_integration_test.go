//go:build integration && failpoint

package cron_test

import (
	"errors"
	"testing"
	"time"

	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/scheduler"
	"github.com/daishakpatel/flowforge/internal/testutil/failpoint"
)

// TestCronPG_CrashBetweenStartAndAdvance (task 03.08 §8): the loop dies after starting a fire's
// run and before advancing the trigger. The restarted loop starts the same fire again, which
// replays the existing run, and then advances: one run, no lost fire.
func TestCronPG_CrashBetweenStartAndAdvance(t *testing.T) {
	e := newEnv(t)
	t.Cleanup(failpoint.Reset)
	tr := e.trigger(t, "* * * * *", persistence.MissedSkip)
	e.clock.Advance(time.Minute)
	failpoint.Enable(failpoint.CronAfterStart, "error")
	if err := e.loop().Once(t.Context(), scheduler.AlwaysLeader{}); !errors.Is(err, failpoint.ErrInjected) {
		t.Fatalf("crash tick: %v; want the injected failure", err)
	}
	v, err := e.triggers.Get(t.Context(), tr.ID)
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := e.runs(t, tr.ID); n != 1 || v.NextFireAt == nil || !v.NextFireAt.Equal(t0.Add(time.Minute)) {
		t.Fatalf("after the crash: %d runs, next_fire_at %v; want 1 run and the trigger not advanced", n, v.NextFireAt)
	}
	failpoint.Disable(failpoint.CronAfterStart)
	once(t, e.loop()) // the restarted loop
	v, err = e.triggers.Get(t.Context(), tr.ID)
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := e.runs(t, tr.ID); n != 1 || v.NextFireAt == nil || !v.NextFireAt.Equal(t0.Add(2*time.Minute)) {
		t.Errorf("after the restart: %d runs, next_fire_at %v; want 1 run and the trigger advanced", n, v.NextFireAt)
	}
}
