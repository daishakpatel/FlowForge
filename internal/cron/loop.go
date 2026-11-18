package cron

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/daishakpatel/flowforge/internal/apperrors"
	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/ids"
	"github.com/daishakpatel/flowforge/internal/logging"
	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/scheduler"
	"github.com/daishakpatel/flowforge/internal/testutil/failpoint"
)

// Loop defaults (task 03.08 §4).
const (
	DefaultTick        = 5 * time.Second
	DefaultMissedGrace = time.Minute // FF_CRON_MISSED_GRACE
	// MaxCatchUpAge is the default bound of catch_up_one: Idempotency-Keys are cleared after
	// FF_RETENTION_IDEMPOTENCY (24 h, 03.07), so an older fire could no longer be told apart from a
	// new one and is skipped instead. Loop.MaxCatchUp follows the configured retention.
	MaxCatchUpAge = 24 * time.Hour
	dueBatch      = 50
	// Actor is the audit actor of everything the loop does.
	Actor = "system:cron"
)

// Fire is one fire of a trigger: the run to start.
type Fire struct {
	Trigger persistence.TriggerView
	At      time.Time // the scheduled fire time: with the trigger id, the fire's identity
	Key     string    // the run's Idempotency-Key, FireKey(trigger id, At)
}

// FireKey is the Idempotency-Key of a fire: cron:<trigger_id>:<fire time, Unix seconds>.
func FireKey(triggerID string, at time.Time) string {
	return fmt.Sprintf("cron:%s:%d", triggerID, at.Unix())
}

// Starter starts the run of a fire through the idempotent run-start path, so starting the same
// fire again returns the existing run with replayed = true (service.TriggerService in production).
// A permanent failure (NOT_FOUND, INVALID_ARGUMENT, INVALID_WORKFLOW, PAYLOAD_TOO_LARGE) is
// reported as such; any other error is retried.
type Starter interface {
	StartFire(ctx context.Context, f Fire) (runID string, replayed bool, err error)
}

// Loop is the leader-gated cron loop. Each tick, in one transaction that asserts the epoch, it
// locks the due triggers (FOR UPDATE SKIP LOCKED, so two loops never take the same one) and for
// each one handles the fire at next_fire_at:
//
//   - on time (at most MissedGrace late): start its run, then advance to the next fire after it;
//   - later, with missed_policy skip (or catch_up_one older than MaxCatchUpAge): start nothing
//     and advance to the next fire after now;
//   - later, with catch_up_one: start the oldest missed fire's run once, then advance to the next
//     fire after now.
//
// No double fire: a fire's run is started with Idempotency-Key FireKey, so a retry after a crash
// between start and advance, or another loop, finds the run instead of creating a second one.
// No double advance: Advance is conditional on next_fire_at still being the handled fire.
// Fires are audited (actor system:cron) in the same transaction as the advance.
type Loop struct {
	Tx          persistence.TxManager
	Cluster     persistence.ClusterRepo
	Triggers    persistence.TriggerRepo
	Starter     Starter
	Audit       persistence.AuditRepo
	IDs         ids.Generator // audit request ids
	Clock       clock.Clock
	Tick        time.Duration          // DefaultTick
	MissedGrace time.Duration          // DefaultMissedGrace
	MaxCatchUp  time.Duration          // MaxCatchUpAge; set to FF_RETENTION_IDEMPOTENCY
	Fires       *prometheus.CounterVec // flowforge_cron_fires_total{outcome}; optional
	Errors      *prometheus.CounterVec // flowforge_loop_errors_total; optional
}

var _ scheduler.Loop = (*Loop)(nil)

// Name implements scheduler.Loop.
func (l *Loop) Name() string { return "cron" }

// Run implements scheduler.Loop.
func (l *Loop) Run(ctx context.Context, gate scheduler.Gate) error {
	tick := l.Tick
	if tick <= 0 {
		tick = DefaultTick
	}
	return scheduler.RunTicking(ctx, l.Name(), l.Clock, tick, nil, l.Errors, func(ctx context.Context) error { return l.Once(ctx, gate) })
}

// Once handles every due trigger once. A stale epoch or a failed read or write is returned (the
// loop backs off). A trigger whose run could not be started for a transient reason is left due,
// logged and counted as retried, and tried again next tick; it does not fail the tick, so one
// broken trigger cannot back off the loop and make other triggers' fires late.
func (l *Loop) Once(ctx context.Context, gate scheduler.Gate) error {
	if !gate.IsLeader() {
		return nil
	}
	now := l.Clock.Now()
	var outcomes []string
	err := l.Tx.WithTx(ctx, pgx.TxOptions{}, func(tx pgx.Tx) error {
		outcomes = nil
		if err := l.Cluster.AssertEpoch(ctx, tx, gate.Epoch()); err != nil { // ADR-003
			return err
		}
		due, err := l.Triggers.LockDue(ctx, tx, now, dueBatch)
		if err != nil {
			return err
		}
		for _, t := range due {
			outcome, err := l.handle(ctx, tx, t, now)
			switch {
			case err != nil && isTxError(err):
				return err
			case err != nil:
				logging.FromContext(ctx).WarnContext(ctx, "cron fire not started; retrying next tick", "trigger_id", t.ID, logging.Err(err))
				outcomes = append(outcomes, OutcomeRetried)
			default:
				outcomes = append(outcomes, outcome)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, o := range outcomes {
		if l.Fires != nil {
			l.Fires.WithLabelValues(o).Inc()
		}
	}
	return nil
}

// handle handles one due trigger and returns the outcome. A transient start failure is returned
// as a startError and leaves the trigger as it was.
func (l *Loop) handle(ctx context.Context, tx pgx.Tx, t persistence.TriggerView, now time.Time) (string, error) {
	at := *t.NextFireAt
	sched, err := Parse(t.Schedule, t.Timezone)
	if err != nil { // stored schedules are validated; a broken one stops rather than failing every tick
		if err := l.audit(ctx, tx, t, "cron.fire", map[string]any{"fire_time": at, "error": err.Error()}, "error"); err != nil {
			return "", err
		}
		_, err := l.Triggers.Advance(ctx, tx, t.ID, at, nil, nil)
		return OutcomeFailed, err
	}
	grace := l.MissedGrace
	if grace <= 0 {
		grace = DefaultMissedGrace
	}
	late := now.Sub(at)
	switch {
	case late <= grace:
		return l.fire(ctx, tx, t, at, sched.Next(at))
	case t.MissedPolicy == persistence.MissedCatchUpOne && late <= l.maxCatchUp():
		return l.fire(ctx, tx, t, at, sched.Next(now))
	}
	next := sched.Next(now)
	details := map[string]any{"oldest_missed": at, "next_fire_at": next, "policy": t.MissedPolicy}
	stopIfLast(ctx, t, next, details)
	logging.FromContext(ctx).InfoContext(ctx, "cron fires missed; skipped", "trigger_id", t.ID, "oldest_missed", at, "next_fire_at", next)
	if err := l.audit(ctx, tx, t, "cron.skip", details, "ok"); err != nil {
		return "", err
	}
	_, err = l.Triggers.Advance(ctx, tx, t.ID, at, timeOrNil(next), nil)
	return OutcomeSkipped, err
}

// fire starts the run of the fire at at and advances the trigger to next.
func (l *Loop) fire(ctx context.Context, tx pgx.Tx, t persistence.TriggerView, at, next time.Time) (string, error) {
	key := FireKey(t.ID, at)
	runID, replayed, err := l.Starter.StartFire(ctx, Fire{Trigger: t, At: at, Key: key})
	outcome, auditOutcome := OutcomeFired, "ok"
	switch {
	case err != nil && !permanent(err):
		return "", startError{fmt.Errorf("start run for trigger %s at %s: %w", t.ID, at.UTC().Format(time.RFC3339), err)}
	case err != nil:
		outcome, auditOutcome = OutcomeFailed, "error"
	case replayed:
		outcome = OutcomeReplayed
	}
	if err := failpoint.Inject(failpoint.CronAfterStart); err != nil { // crash between start and advance
		return "", err
	}
	details := map[string]any{"fire_time": at, "idempotency_key": key, "run_id": runID, "outcome": outcome}
	if err != nil {
		details["error"] = err.Error()
	}
	stopIfLast(ctx, t, next, details)
	logging.FromContext(ctx).InfoContext(ctx, "cron trigger fired", "trigger_id", t.ID, "fire_time", at, "run_id", runID, "outcome", outcome)
	if err := l.audit(ctx, tx, t, "cron.fire", details, auditOutcome); err != nil {
		return "", err
	}
	_, err = l.Triggers.Advance(ctx, tx, t.ID, at, timeOrNil(next), &at)
	return outcome, err
}

func (l *Loop) maxCatchUp() time.Duration {
	if l.MaxCatchUp > 0 {
		return l.MaxCatchUp
	}
	return MaxCatchUpAge
}

// stopIfLast notes in details, and logs, that next is zero: the schedule has no further fire, so
// the advance clears next_fire_at and the trigger stops.
func stopIfLast(ctx context.Context, t persistence.TriggerView, next time.Time, details map[string]any) {
	if next.IsZero() {
		details["stopped"] = "the schedule has no further fire"
		logging.FromContext(ctx).WarnContext(ctx, "cron trigger stops: its schedule has no further fire", "trigger_id", t.ID)
	}
}

func (l *Loop) audit(ctx context.Context, tx pgx.Tx, t persistence.TriggerView, action string, details map[string]any, outcome string) error {
	return l.Audit.Record(ctx, tx, persistence.AuditEntry{
		ActorKeyID: Actor, ActorRole: "system", Action: action, ResourceType: "trigger", ResourceID: t.ID,
		RequestID: l.IDs.New(ids.PrefixRequest), Details: details, Outcome: outcome,
	})
}

// startError marks a transient failure to start one trigger's run: the rest of the batch goes on.
type startError struct{ error }

func (e startError) Unwrap() error { return e.error }

// isTxError reports whether err ends the batch's transaction (anything but a startError).
func isTxError(err error) bool {
	var se startError
	return !errors.As(err, &se)
}

// permanent reports whether a start failure will not go away by retrying the same fire.
func permanent(err error) bool {
	switch apperrors.CodeOf(err) {
	case apperrors.CodeNotFound, apperrors.CodeInvalidArgument, apperrors.CodeInvalidWorkflow, apperrors.CodePayloadTooLarge:
		return true
	}
	return false
}

func timeOrNil(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}
