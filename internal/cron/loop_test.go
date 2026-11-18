package cron

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/daishakpatel/flowforge/internal/apperrors"
	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/ids"
	"github.com/daishakpatel/flowforge/internal/obs"
	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/scheduler"
)

// memTriggers is an in-memory cron_triggers table (LockDue and Advance only).
type memTriggers struct {
	persistence.TriggerRepo
	mu   sync.Mutex
	rows map[string]*persistence.TriggerView
}

func (m *memTriggers) LockDue(_ context.Context, _ pgx.Tx, now time.Time, limit int) ([]persistence.TriggerView, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []persistence.TriggerView
	for _, t := range m.rows {
		if t.Enabled && t.NextFireAt != nil && !t.NextFireAt.After(now) && len(out) < limit {
			out = append(out, *t)
		}
	}
	return out, nil
}

func (m *memTriggers) Advance(_ context.Context, _ pgx.Tx, id string, old time.Time, next, firedAt *time.Time) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t := m.rows[id]
	if t.NextFireAt == nil || !t.NextFireAt.Equal(old) {
		return false, nil
	}
	t.NextFireAt = next
	if firedAt != nil {
		t.LastFiredAt = firedAt
	}
	return true, nil
}

func (m *memTriggers) next(id string) *time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.rows[id].NextFireAt
}

// starter records fires; err, when set, is returned instead.
type starter struct {
	mu    sync.Mutex
	keys  []string
	seen  map[string]bool
	err   error
	calls int
}

func (s *starter) StartFire(_ context.Context, f Fire) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.err != nil {
		return "", false, s.err
	}
	if s.seen == nil {
		s.seen = map[string]bool{}
	}
	replayed := s.seen[f.Key]
	if !replayed {
		s.keys = append(s.keys, f.Key)
	}
	s.seen[f.Key] = true
	return "run_" + f.Key, replayed, nil
}

type memAudit struct {
	mu      sync.Mutex
	entries []persistence.AuditEntry
}

func (m *memAudit) Record(_ context.Context, _ persistence.Querier, e persistence.AuditEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = append(m.entries, e)
	return nil
}

func (m *memAudit) actions() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for _, e := range m.entries {
		out = append(out, e.Action+"/"+e.Outcome)
	}
	return out
}

type memTx struct{}

func (memTx) WithTx(_ context.Context, _ pgx.TxOptions, fn func(pgx.Tx) error) error { return fn(nil) }

type fence struct{ persistence.ClusterRepo }

func (fence) AssertEpoch(context.Context, pgx.Tx, int64) error { return nil }

type notLeader struct{ scheduler.AlwaysLeader }

func (notLeader) IsLeader() bool { return false }

type fixture struct {
	trig  *memTriggers
	start *starter
	audit *memAudit
	clock *clock.Fake
	loop  *Loop
}

var t0 = time.Date(2026, 11, 17, 10, 0, 0, 0, time.UTC)

// newFixture has one trigger "trg_1" with the given schedule and policy, due at t0; the clock is
// at now.
func newFixture(t *testing.T, schedule, policy string, now time.Time) *fixture {
	t.Helper()
	due := t0
	f := &fixture{
		trig: &memTriggers{rows: map[string]*persistence.TriggerView{
			"trg_1": {ID: "trg_1", Name: "t", WorkflowName: "wf", Schedule: schedule, Timezone: "UTC", MissedPolicy: policy, Enabled: true, NextFireAt: &due},
		}},
		start: &starter{}, audit: &memAudit{}, clock: clock.NewFake(now),
	}
	fires, err := NewFires(obs.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	f.loop = &Loop{Tx: memTx{}, Cluster: fence{}, Triggers: f.trig, Starter: f.start, Audit: f.audit, IDs: &ids.Fake{},
		Clock: f.clock, MissedGrace: time.Minute, Fires: fires}
	return f
}

func (f *fixture) once(t *testing.T) error {
	t.Helper()
	return f.loop.Once(t.Context(), scheduler.AlwaysLeader{})
}

func key(at time.Time) string { return fmt.Sprintf("cron:trg_1:%d", at.Unix()) }

func TestLoop_FiresDueTriggerAndAdvances(t *testing.T) {
	t.Parallel()
	f := newFixture(t, "*/5 * * * *", persistence.MissedSkip, t0.Add(10*time.Second))
	if err := f.once(t); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(f.start.keys, []string{key(t0)}) {
		t.Errorf("fired %v; want [%s]", f.start.keys, key(t0))
	}
	if next := f.trig.next("trg_1"); next == nil || !next.Equal(t0.Add(5*time.Minute)) {
		t.Errorf("next_fire_at %v; want %s", next, t0.Add(5*time.Minute))
	}
	if got := f.audit.actions(); !slices.Equal(got, []string{"cron.fire/ok"}) {
		t.Errorf("audit %v", got)
	}
	if err := f.once(t); err != nil || f.start.calls != 1 {
		t.Errorf("second tick before the next fire: err %v, %d calls", err, f.start.calls)
	}
	if got := promtest.ToFloat64(f.loop.Fires.WithLabelValues("fired")); got != 1 {
		t.Errorf("fired metric %v", got)
	}
}

// TestLoop_MissedFiresSkip: after a 10-minute outage, skip fires nothing and moves to the next
// future fire time.
func TestLoop_MissedFiresSkip(t *testing.T) {
	t.Parallel()
	f := newFixture(t, "* * * * *", persistence.MissedSkip, t0.Add(10*time.Minute+30*time.Second))
	if err := f.once(t); err != nil {
		t.Fatal(err)
	}
	if f.start.calls != 0 {
		t.Errorf("skip fired %v", f.start.keys)
	}
	if next := f.trig.next("trg_1"); next == nil || !next.Equal(t0.Add(11*time.Minute)) {
		t.Errorf("next_fire_at %v; want the next future minute %s", next, t0.Add(11*time.Minute))
	}
	if got := f.audit.actions(); !slices.Equal(got, []string{"cron.skip/ok"}) {
		t.Errorf("audit %v", got)
	}
}

// TestLoop_MissedFiresCatchUpOne: after a 10-minute outage, catch_up_one fires once, for the
// oldest missed time, and moves to the next future fire time.
func TestLoop_MissedFiresCatchUpOne(t *testing.T) {
	t.Parallel()
	f := newFixture(t, "* * * * *", persistence.MissedCatchUpOne, t0.Add(10*time.Minute+30*time.Second))
	for range 3 {
		if err := f.once(t); err != nil {
			t.Fatal(err)
		}
	}
	if !slices.Equal(f.start.keys, []string{key(t0)}) {
		t.Errorf("catch_up_one fired %v; want exactly the oldest, %s", f.start.keys, key(t0))
	}
	if next := f.trig.next("trg_1"); next == nil || !next.Equal(t0.Add(11*time.Minute)) {
		t.Errorf("next_fire_at %v; want %s", next, t0.Add(11*time.Minute))
	}
}

// TestLoop_CatchUpOlderThan24hSkips: Idempotency-Keys are cleared after 24 h (03.07), so an older
// missed fire is skipped rather than risk a duplicate run.
func TestLoop_CatchUpOlderThan24hSkips(t *testing.T) {
	t.Parallel()
	f := newFixture(t, "0 * * * *", persistence.MissedCatchUpOne, t0.Add(25*time.Hour))
	if err := f.once(t); err != nil || f.start.calls != 0 {
		t.Fatalf("err %v, fired %v; want a skip", err, f.start.keys)
	}
	if got := f.audit.actions(); !slices.Equal(got, []string{"cron.skip/ok"}) {
		t.Errorf("audit %v", got)
	}
}

func TestLoop_StartFailures(t *testing.T) {
	t.Parallel()
	// Transient: not advanced and retried next tick, without failing the tick (which would back
	// the whole loop off and make other triggers' fires late).
	f := newFixture(t, "*/5 * * * *", persistence.MissedSkip, t0)
	f.start.err = errors.New("database is down")
	if err := f.once(t); err != nil {
		t.Fatalf("a transient start failure failed the tick: %v", err)
	}
	if got := promtest.ToFloat64(f.loop.Fires.WithLabelValues(OutcomeRetried)); got != 1 {
		t.Errorf("retried metric %v; want 1", got)
	}
	if next := f.trig.next("trg_1"); next == nil || !next.Equal(t0) {
		t.Errorf("transient failure advanced the trigger to %v", next)
	}
	f.start.err = nil
	if err := f.once(t); err != nil || !slices.Equal(f.start.keys, []string{key(t0)}) {
		t.Errorf("retry: err %v, fired %v", err, f.start.keys)
	}

	// Permanent (the workflow is gone): audited as an error and advanced, not retried forever.
	f = newFixture(t, "*/5 * * * *", persistence.MissedSkip, t0)
	f.start.err = apperrors.NotFound("workflow not found")
	if err := f.once(t); err != nil {
		t.Fatal(err)
	}
	if next := f.trig.next("trg_1"); next == nil || !next.Equal(t0.Add(5*time.Minute)) {
		t.Errorf("permanent failure: next_fire_at %v; want advanced", next)
	}
	if got := f.audit.actions(); !slices.Equal(got, []string{"cron.fire/error"}) {
		t.Errorf("audit %v", got)
	}
}

func TestLoop_ReplayedFireIsCountedAndAdvanced(t *testing.T) {
	t.Parallel()
	f := newFixture(t, "*/5 * * * *", persistence.MissedSkip, t0)
	f.start.seen = map[string]bool{key(t0): true} // a previous leader started this fire, then crashed
	if err := f.once(t); err != nil {
		t.Fatal(err)
	}
	if next := f.trig.next("trg_1"); next == nil || !next.Equal(t0.Add(5*time.Minute)) {
		t.Errorf("next_fire_at %v", next)
	}
	if got := promtest.ToFloat64(f.loop.Fires.WithLabelValues("replayed")); got != 1 {
		t.Errorf("replayed metric %v", got)
	}
}

func TestLoop_NotLeaderDoesNothing(t *testing.T) {
	t.Parallel()
	f := newFixture(t, "*/5 * * * *", persistence.MissedSkip, t0)
	if err := f.loop.Once(t.Context(), notLeader{}); err != nil || f.start.calls != 0 {
		t.Errorf("not leader: err %v, %d starts", err, f.start.calls)
	}
}

func TestFireKey(t *testing.T) {
	t.Parallel()
	if got := FireKey("trg_1", t0); got != "cron:trg_1:1794909600" {
		t.Errorf("FireKey = %s", got)
	}
}

// TestLoop_CatchUpCapFollowsKeyRetention: the catch-up cap is the Idempotency-Key lifetime
// (FF_RETENTION_IDEMPOTENCY), not a fixed 24 h.
func TestLoop_CatchUpCapFollowsKeyRetention(t *testing.T) {
	t.Parallel()
	f := newFixture(t, "0 * * * *", persistence.MissedCatchUpOne, t0.Add(3*time.Hour))
	f.loop.MaxCatchUp = time.Hour
	if err := f.once(t); err != nil || f.start.calls != 0 {
		t.Fatalf("err %v, fired %v; a 3 h old fire with keys kept 1 h must be skipped", err, f.start.keys)
	}
}
