package pg

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/daishakpatel/flowforge/internal/apperrors"
	"github.com/daishakpatel/flowforge/internal/ids"
	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/persistence/sqlcgen"
)

// TriggerRepo implements persistence.TriggerRepo.
type TriggerRepo struct {
	ids ids.Generator
}

var _ persistence.TriggerRepo = (*TriggerRepo)(nil)

// NewTriggerRepo returns a TriggerRepo that takes trigger ids from g.
func NewTriggerRepo(g ids.Generator) *TriggerRepo { return &TriggerRepo{ids: g} }

// Create implements persistence.TriggerRepo.
func (r *TriggerRepo) Create(ctx context.Context, q persistence.Querier, t persistence.NewTrigger) (persistence.TriggerView, error) {
	row, err := sqlcgen.New(q).InsertTrigger(ctx, sqlcgen.InsertTriggerParams{
		ID: r.ids.New(ids.PrefixTrigger), Name: t.Name, WorkflowName: t.WorkflowName, WorkflowVersion: int4(t.WorkflowVersion),
		Schedule: t.Schedule, Timezone: t.Timezone, Input: t.Input, MissedPolicy: t.MissedPolicy, Enabled: t.Enabled,
		NextFireAt: tsPtr(t.NextFireAt), CreatedBy: t.CreatedBy,
	})
	if err != nil {
		return persistence.TriggerView{}, conflictOr(err, "a trigger named "+t.Name+" already exists", "insert trigger")
	}
	return triggerView(row), nil
}

// Get implements persistence.TriggerRepo.
func (r *TriggerRepo) Get(ctx context.Context, q persistence.Querier, id string) (persistence.TriggerView, error) {
	row, err := sqlcgen.New(q).GetTrigger(ctx, id)
	if err != nil {
		return persistence.TriggerView{}, notFoundOr(err, "trigger")
	}
	return triggerView(row), nil
}

// List implements persistence.TriggerRepo.
func (r *TriggerRepo) List(ctx context.Context, q persistence.Querier, p persistence.Page) ([]persistence.TriggerView, string, error) {
	at, id, err := cursorArgs(p.Cursor)
	if err != nil {
		return nil, "", err
	}
	limit := p.EffectiveLimit()
	rows, err := sqlcgen.New(q).ListTriggers(ctx, sqlcgen.ListTriggersParams{CursorCreatedAt: at, CursorID: id, Lim: limitOf(limit + 1)})
	if err != nil {
		return nil, "", fmt.Errorf("list triggers: %w", err)
	}
	out := make([]persistence.TriggerView, len(rows))
	for i, row := range rows {
		out[i] = triggerView(row)
	}
	out, next := page(out, limit, func(t persistence.TriggerView) (time.Time, string) { return t.CreatedAt, t.ID })
	return out, next, nil
}

// Update implements persistence.TriggerRepo.
func (r *TriggerRepo) Update(ctx context.Context, q persistence.Querier, id string, u persistence.TriggerUpdate) (persistence.TriggerView, error) {
	row, err := sqlcgen.New(q).UpdateTrigger(ctx, sqlcgen.UpdateTriggerParams{
		ID: id, WorkflowVersion: int4(u.WorkflowVersion), Schedule: u.Schedule, Timezone: u.Timezone, Input: u.Input,
		MissedPolicy: u.MissedPolicy, Enabled: u.Enabled, NextFireAt: tsPtr(u.NextFireAt), KeepNext: u.KeepNextFire,
	})
	if err != nil {
		return persistence.TriggerView{}, notFoundOr(err, "trigger")
	}
	return triggerView(row), nil
}

// Delete implements persistence.TriggerRepo.
func (r *TriggerRepo) Delete(ctx context.Context, q persistence.Querier, id string) error {
	n, err := sqlcgen.New(q).DeleteTrigger(ctx, id)
	switch {
	case err != nil:
		return fmt.Errorf("delete trigger %s: %w", id, err)
	case n == 0:
		return apperrors.NotFound("trigger not found")
	}
	return nil
}

// LockDue implements persistence.TriggerRepo.
func (r *TriggerRepo) LockDue(ctx context.Context, tx pgx.Tx, now time.Time, limit int) ([]persistence.TriggerView, error) {
	rows, err := sqlcgen.New(tx).LockDueTriggers(ctx, sqlcgen.LockDueTriggersParams{Now: timestamptz(now), Lim: limitOf(limit)})
	if err != nil {
		return nil, fmt.Errorf("lock due triggers: %w", err)
	}
	out := make([]persistence.TriggerView, len(rows))
	for i, row := range rows {
		out[i] = triggerView(row)
	}
	return out, nil
}

// Advance implements persistence.TriggerRepo.
func (r *TriggerRepo) Advance(ctx context.Context, tx pgx.Tx, id string, old time.Time, next, firedAt *time.Time) (bool, error) {
	n, err := sqlcgen.New(tx).AdvanceTrigger(ctx, sqlcgen.AdvanceTriggerParams{
		ID: id, OldNext: timestamptz(old), NextFireAt: tsPtr(next), FiredAt: tsPtr(firedAt),
	})
	if err != nil {
		return false, fmt.Errorf("advance trigger %s: %w", id, err)
	}
	return n == 1, nil
}

func triggerView(r sqlcgen.CronTrigger) persistence.TriggerView {
	var version *int
	if r.WorkflowVersion.Valid {
		v := int(r.WorkflowVersion.Int32)
		version = &v
	}
	return persistence.TriggerView{
		ID: r.ID, Name: r.Name, WorkflowName: r.WorkflowName, WorkflowVersion: version, Schedule: r.Schedule,
		Timezone: r.Timezone, Input: r.Input, MissedPolicy: r.MissedPolicy, Enabled: r.Enabled,
		NextFireAt: timePtr(r.NextFireAt), LastFiredAt: timePtr(r.LastFiredAt), CreatedBy: r.CreatedBy, CreatedAt: r.CreatedAt.Time,
	}
}

func int4(v *int) pgtype.Int4 {
	if v == nil || *v < math.MinInt32 || *v > math.MaxInt32 {
		return pgtype.Int4{}
	}
	return pgtype.Int4{Int32: int32(*v), Valid: true} //nolint:gosec // G115: range checked above
}

func tsPtr(t *time.Time) pgtype.Timestamptz {
	if t == nil {
		return pgtype.Timestamptz{}
	}
	return timestamptz(*t)
}
