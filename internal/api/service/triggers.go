package service

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"time"

	"github.com/daishakpatel/flowforge/internal/apperrors"
	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/cron"
	"github.com/daishakpatel/flowforge/internal/persistence"
)

// triggerNamePattern is the workflow-name pattern, reused for trigger names.
var triggerNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,127}$`)

// TriggerService manages cron triggers and starts their runs (task 03.08, FR-WF-14). It is shared
// by the REST API (CRUD) and the scheduler's cron.Loop (StartFire).
type TriggerService struct {
	DB       persistence.Querier
	Triggers persistence.TriggerRepo
	DefRepo  persistence.DefinitionRepo
	RunRepo  persistence.RunRepo
	Runs     *RunService
	Clock    clock.Clock
}

var _ cron.Starter = (*TriggerService)(nil)

// CreateTriggerParams is the input of TriggerService.Create. Empty Timezone is UTC, empty
// MissedPolicy is skip, nil Enabled is true, nil Input is {}.
type CreateTriggerParams struct {
	Name            string
	WorkflowName    string
	WorkflowVersion *int // nil: the latest version at each fire
	Schedule        string
	Timezone        string
	Input           json.RawMessage
	MissedPolicy    string
	Enabled         *bool
	Actor           string
}

// TriggerPatch is the input of TriggerService.Update; nil fields are left as they are.
type TriggerPatch struct {
	Enabled      *bool
	Schedule     *string
	Timezone     *string
	Input        json.RawMessage
	MissedPolicy *string
}

// Create validates and stores a trigger; its first fire is the schedule's next time after now
// when enabled. The workflow (and version, when given) must exist, the schedule must parse in the
// timezone, and the input must be a JSON object. A duplicate name is CONFLICT.
func (s *TriggerService) Create(ctx context.Context, p CreateTriggerParams) (persistence.TriggerView, error) {
	if !triggerNamePattern.MatchString(p.Name) {
		return persistence.TriggerView{}, invalidField("name", fmt.Sprintf("name must match %s", triggerNamePattern))
	}
	if p.Timezone == "" {
		p.Timezone = "UTC"
	}
	if p.MissedPolicy == "" {
		p.MissedPolicy = persistence.MissedSkip
	}
	enabled := p.Enabled == nil || *p.Enabled
	input, sched, err := s.validate(p.Schedule, p.Timezone, p.MissedPolicy, p.Input)
	if err != nil {
		return persistence.TriggerView{}, err
	}
	if p.WorkflowVersion != nil && *p.WorkflowVersion < 1 {
		return persistence.TriggerView{}, invalidField("workflow_version", "workflow_version must be at least 1")
	}
	if _, err := s.definition(ctx, p.WorkflowName, p.WorkflowVersion); err != nil {
		if apperrors.CodeOf(err) == apperrors.CodeNotFound {
			return persistence.TriggerView{}, invalidField("workflow_name", "no such workflow (or version): "+p.WorkflowName)
		}
		return persistence.TriggerView{}, err
	}
	return s.Triggers.Create(ctx, s.DB, persistence.NewTrigger{
		Name: p.Name, WorkflowName: p.WorkflowName, WorkflowVersion: p.WorkflowVersion, Schedule: p.Schedule,
		Timezone: p.Timezone, Input: input, MissedPolicy: p.MissedPolicy, Enabled: enabled,
		NextFireAt: s.firstFire(enabled, sched), CreatedBy: p.Actor,
	})
}

// Get returns a trigger, or NOT_FOUND.
func (s *TriggerService) Get(ctx context.Context, id string) (persistence.TriggerView, error) {
	return s.Triggers.Get(ctx, s.DB, id)
}

// List returns one page of triggers, newest first, and the next cursor.
func (s *TriggerService) List(ctx context.Context, page persistence.Page) ([]persistence.TriggerView, string, error) {
	return s.Triggers.List(ctx, s.DB, page)
}

// Delete removes a trigger, or returns NOT_FOUND. Its runs keep their trigger_id.
func (s *TriggerService) Delete(ctx context.Context, id string) error {
	return s.Triggers.Delete(ctx, s.DB, id)
}

// Update applies p. next_fire_at is recomputed from now when the trigger is (re-)enabled or its
// schedule or timezone changes, cleared when it is disabled (which stops it), and otherwise left
// as stored, so an advance the cron loop made meanwhile is never reverted. An empty timezone is
// UTC and an empty missed_policy is skip, as on create.
func (s *TriggerService) Update(ctx context.Context, id string, p TriggerPatch) (persistence.TriggerView, error) {
	cur, err := s.Triggers.Get(ctx, s.DB, id)
	if err != nil {
		return persistence.TriggerView{}, err
	}
	u := persistence.TriggerUpdate{
		WorkflowVersion: cur.WorkflowVersion, Schedule: cur.Schedule, Timezone: cur.Timezone, Input: cur.Input,
		MissedPolicy: cur.MissedPolicy, Enabled: cur.Enabled, NextFireAt: cur.NextFireAt,
	}
	reschedule := !cur.Enabled || cur.NextFireAt == nil
	if p.Schedule != nil && *p.Schedule != cur.Schedule {
		u.Schedule, reschedule = *p.Schedule, true
	}
	if p.Timezone != nil && *p.Timezone == "" {
		utc := "UTC" // as on create
		p.Timezone = &utc
	}
	if p.Timezone != nil && *p.Timezone != cur.Timezone {
		u.Timezone, reschedule = *p.Timezone, true
	}
	if p.MissedPolicy != nil {
		u.MissedPolicy = *p.MissedPolicy
		if u.MissedPolicy == "" {
			u.MissedPolicy = persistence.MissedSkip // as on create
		}
	}
	if p.Enabled != nil {
		u.Enabled = *p.Enabled
	}
	input, sched, err := s.validate(u.Schedule, u.Timezone, u.MissedPolicy, orDefault(p.Input, u.Input))
	if err != nil {
		return persistence.TriggerView{}, err
	}
	u.Input = input
	switch {
	case !u.Enabled:
		u.NextFireAt = nil
	case reschedule:
		u.NextFireAt = s.firstFire(true, sched)
	default:
		u.KeepNextFire = true // the cron loop may have advanced it since cur was read
	}
	return s.Triggers.Update(ctx, s.DB, id, u)
}

// StartFire implements cron.Starter: it starts the fire's run through RunService.Start with the
// fire's Idempotency-Key and the trigger id, resolving the latest workflow version when the
// trigger names none. A fire started before, possibly against a since-superseded latest version
// (the key then conflicts), is returned as replayed.
func (s *TriggerService) StartFire(ctx context.Context, f cron.Fire) (string, bool, error) {
	def, err := s.definition(ctx, f.Trigger.WorkflowName, f.Trigger.WorkflowVersion)
	if err != nil {
		return "", false, err
	}
	run, replayed, err := s.Runs.Start(ctx, StartRunParams{
		DefinitionID: def.ID, Input: f.Trigger.Input, IdempotencyKey: f.Key, TriggerID: f.Trigger.ID, Actor: cron.Actor,
	})
	if apperrors.CodeOf(err) == apperrors.CodeConflict {
		existing, gerr := s.RunRepo.GetByIdempotencyKey(ctx, s.DB, f.Key)
		switch {
		case apperrors.CodeOf(gerr) == apperrors.CodeNotFound:
			return "", false, err // not the fire's key: keep the CONFLICT, which the loop retries
		case gerr != nil:
			return "", false, gerr
		}
		return existing.ID, true, nil
	}
	return run.ID, replayed, err
}

func (s *TriggerService) definition(ctx context.Context, name string, version *int) (persistence.DefinitionView, error) {
	if version != nil {
		return s.DefRepo.GetByNameVersion(ctx, s.DB, name, *version)
	}
	return s.DefRepo.Latest(ctx, s.DB, name)
}

// validate checks a trigger's schedule, timezone, missed policy and input, and returns the
// normalized input and the parsed schedule.
func (s *TriggerService) validate(schedule, tz, policy string, input json.RawMessage) (json.RawMessage, cron.Schedule, error) {
	sched, err := cron.Parse(schedule, tz)
	if err != nil {
		return nil, cron.Schedule{}, invalidField("schedule", err.Error())
	}
	if policy != persistence.MissedSkip && policy != persistence.MissedCatchUpOne {
		return nil, cron.Schedule{}, invalidField("missed_policy", "missed_policy must be skip or catch_up_one")
	}
	input, err = validateRunInput(input)
	if err != nil {
		return nil, cron.Schedule{}, err
	}
	return input, sched, nil
}

func (s *TriggerService) firstFire(enabled bool, sched cron.Schedule) *time.Time {
	if !enabled {
		return nil
	}
	next := sched.Next(s.Clock.Now())
	if next.IsZero() {
		return nil
	}
	return &next
}

func invalidField(field, msg string) error {
	return apperrors.InvalidArgument(msg, apperrors.Detail{Field: field, Message: msg})
}

func orDefault(v, def json.RawMessage) json.RawMessage {
	if v != nil {
		return v
	}
	return def
}
