//go:build integration

package cron_test

import (
	crand "crypto/rand"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daishakpatel/flowforge/internal/api/service"
	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/config"
	"github.com/daishakpatel/flowforge/internal/cron"
	"github.com/daishakpatel/flowforge/internal/events"
	"github.com/daishakpatel/flowforge/internal/ids"
	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/persistence/pg"
	"github.com/daishakpatel/flowforge/internal/scheduler"
	"github.com/daishakpatel/flowforge/internal/testutil"
)

// env is a migrated database with one workflow "wf", the trigger service and a fake clock at
// t0, the production wiring of cron.Loop.
type env struct {
	pool     *pgxpool.Pool
	clock    *clock.Fake
	triggers *service.TriggerService
	tm       *pg.TxManager
	gen      ids.Generator
}

var t0 = time.Date(2026, 11, 17, 10, 0, 0, 0, time.UTC)

func newEnv(t *testing.T) *env {
	t.Helper()
	db := testutil.NewFlowforgeDB(t)
	pool, err := pg.NewPool(t.Context(), config.Secret(db.DSN))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	gen := ids.NewGenerator(clock.Real{}, crand.Reader)
	rec := events.NewRecorder(clock.Real{}, gen, "test")
	tm := pg.NewTxManager(pool, clock.Real{}, nil)
	defs, runs := pg.NewDefinitionRepo(gen), pg.NewRunRepo(gen, rec)
	fc := clock.NewFake(t0)
	e := &env{pool: pool, clock: fc, tm: tm, gen: gen, triggers: &service.TriggerService{
		DB: pool, Triggers: pg.NewTriggerRepo(gen), DefRepo: defs, RunRepo: runs, Clock: fc,
		Runs: &service.RunService{DB: pool, Tx: tm, DefRepo: defs, RunRepo: runs},
	}}
	if _, err := defs.Create(t.Context(), pool, persistence.NewDefinition{
		Name: "wf", Version: 1, Definition: []byte(`{"name":"wf","version":1,"tasks":[{"id":"a","type":"t"}]}`), CreatedBy: "t",
	}); err != nil {
		t.Fatal(err)
	}
	return e
}

func (e *env) loop() *cron.Loop {
	return &cron.Loop{Tx: e.tm, Cluster: pg.ClusterRepo{}, Triggers: pg.NewTriggerRepo(e.gen), Starter: e.triggers,
		Audit: pg.AuditRepo{}, IDs: e.gen, Clock: e.clock, MissedGrace: time.Minute}
}

func (e *env) trigger(t *testing.T, schedule, policy string) persistence.TriggerView {
	t.Helper()
	v, err := e.triggers.Create(t.Context(), service.CreateTriggerParams{
		Name: "every-minute", WorkflowName: "wf", Schedule: schedule, MissedPolicy: policy, Input: []byte(`{"from":"cron"}`), Actor: "t",
	})
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// runs returns how many runs the trigger started and how many distinct idempotency keys they have.
func (e *env) runs(t *testing.T, triggerID string) (int, int) {
	t.Helper()
	var n, keys int
	if err := e.pool.QueryRow(t.Context(), `SELECT count(*), count(DISTINCT idempotency_key) FROM workflow_runs WHERE trigger_id = $1`,
		triggerID).Scan(&n, &keys); err != nil {
		t.Fatal(err)
	}
	return n, keys
}

func once(t *testing.T, l *cron.Loop) {
	t.Helper()
	if err := l.Once(t.Context(), scheduler.AlwaysLeader{}); err != nil {
		t.Fatal(err)
	}
}

// TestCronPG_TenMinutesTenRuns: 10 minutes of a * * * * * trigger start exactly 10 runs, each with
// its own idempotency key and the trigger's id; the audit log records each fire as system:cron.
func TestCronPG_TenMinutesTenRuns(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	tr := e.trigger(t, "* * * * *", persistence.MissedSkip)
	l := e.loop()
	for range 10 {
		e.clock.Advance(time.Minute)
		once(t, l)
		once(t, l) // a second tick in the same minute fires nothing
	}
	if n, keys := e.runs(t, tr.ID); n != 10 || keys != 10 {
		t.Errorf("%d runs with %d distinct keys; want 10 and 10", n, keys)
	}
	var audited int
	if err := e.pool.QueryRow(t.Context(), `SELECT count(*) FROM audit_log WHERE actor_key_id = 'system:cron' AND action = 'cron.fire' AND resource_id = $1`,
		tr.ID).Scan(&audited); err != nil || audited != 10 {
		t.Errorf("%d cron.fire audit rows (%v); want 10", audited, err)
	}
}

// TestCronPG_TwoLoopsOneRunPerFire: two loops on the same database, ticking at the same time,
// still start exactly one run per fire.
func TestCronPG_TwoLoopsOneRunPerFire(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	tr := e.trigger(t, "* * * * *", persistence.MissedSkip)
	a, b := e.loop(), e.loop()
	for range 10 {
		e.clock.Advance(time.Minute)
		var wg sync.WaitGroup
		for _, l := range []*cron.Loop{a, b, a, b} {
			wg.Go(func() { _ = l.Once(t.Context(), scheduler.AlwaysLeader{}) })
		}
		wg.Wait()
	}
	if n, keys := e.runs(t, tr.ID); n != 10 || keys != 10 {
		t.Errorf("%d runs with %d distinct keys; want 10 and 10", n, keys)
	}
}

// TestCronPG_MissedPolicies: after a 10-minute outage, skip starts nothing and catch_up_one starts
// exactly one run; both move to the next future minute.
func TestCronPG_MissedPolicies(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		policy string
		want   int
	}{{persistence.MissedSkip, 0}, {persistence.MissedCatchUpOne, 1}} {
		e := newEnv(t)
		tr := e.trigger(t, "* * * * *", c.policy)
		e.clock.Advance(10*time.Minute + 30*time.Second)
		l := e.loop()
		once(t, l)
		once(t, l)
		if n, _ := e.runs(t, tr.ID); n != c.want {
			t.Errorf("%s: %d runs after the outage; want %d", c.policy, n, c.want)
		}
		v, err := e.triggers.Get(t.Context(), tr.ID)
		if err != nil {
			t.Fatal(err)
		}
		if want := t0.Add(11 * time.Minute); v.NextFireAt == nil || !v.NextFireAt.Equal(want) {
			t.Errorf("%s: next_fire_at %v; want %s", c.policy, v.NextFireAt, want)
		}
	}
}

// TestCronPG_DisableAndReschedule: a disabled trigger fires nothing; re-enabling with a new
// schedule recomputes next_fire_at from now.
func TestCronPG_DisableAndReschedule(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	tr := e.trigger(t, "* * * * *", persistence.MissedSkip)
	off := false
	if _, err := e.triggers.Update(t.Context(), tr.ID, service.TriggerPatch{Enabled: &off}); err != nil {
		t.Fatal(err)
	}
	e.clock.Advance(5 * time.Minute)
	once(t, e.loop())
	if n, _ := e.runs(t, tr.ID); n != 0 {
		t.Errorf("a disabled trigger started %d runs", n)
	}
	on, hourly := true, "0 * * * *"
	v, err := e.triggers.Update(t.Context(), tr.ID, service.TriggerPatch{Enabled: &on, Schedule: &hourly})
	if err != nil {
		t.Fatal(err)
	}
	if want := t0.Add(time.Hour); v.NextFireAt == nil || !v.NextFireAt.Equal(want) {
		t.Errorf("next_fire_at %v; want %s", v.NextFireAt, want)
	}
}

// TestCronPG_PatchKeepsTheLoopsAdvance: an update that does not reschedule keeps the stored
// next_fire_at, so it cannot revert an advance the loop made after the trigger was read (code
// review of 03.08); an empty timezone is normalized to UTC as on create.
func TestCronPG_PatchKeepsTheLoopsAdvance(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	tr := e.trigger(t, "* * * * *", persistence.MissedSkip)
	stale := *tr.NextFireAt
	e.clock.Advance(time.Minute)
	once(t, e.loop()) // advances to t0+2m
	repo := pg.NewTriggerRepo(e.gen)
	v, err := repo.Update(t.Context(), e.pool, tr.ID, persistence.TriggerUpdate{
		Schedule: tr.Schedule, Timezone: tr.Timezone, Input: tr.Input, MissedPolicy: persistence.MissedCatchUpOne,
		Enabled: true, NextFireAt: &stale, KeepNextFire: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := t0.Add(2 * time.Minute); v.NextFireAt == nil || !v.NextFireAt.Equal(want) {
		t.Errorf("next_fire_at %v after a non-rescheduling update; want the loop's %s", v.NextFireAt, want)
	}
	empty := ""
	if v, err = e.triggers.Update(t.Context(), tr.ID, service.TriggerPatch{Timezone: &empty}); err != nil || v.Timezone != "UTC" {
		t.Errorf("empty timezone: %v, %+v; want UTC", err, v)
	}
}
