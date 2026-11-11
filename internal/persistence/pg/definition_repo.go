package pg

import (
	"context"
	"fmt"
	"time"

	"github.com/daishakpatel/flowforge/internal/ids"
	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/persistence/sqlcgen"
)

// DefinitionRepo implements persistence.DefinitionRepo.
type DefinitionRepo struct {
	ids ids.Generator
}

var _ persistence.DefinitionRepo = (*DefinitionRepo)(nil)

// NewDefinitionRepo returns a DefinitionRepo that takes definition IDs from g.
func NewDefinitionRepo(g ids.Generator) *DefinitionRepo { return &DefinitionRepo{ids: g} }

// Create implements persistence.DefinitionRepo.
func (r *DefinitionRepo) Create(ctx context.Context, q persistence.Querier, d persistence.NewDefinition) (persistence.DefinitionView, error) {
	version, err := int32Of("version", d.Version)
	if err != nil {
		return persistence.DefinitionView{}, err
	}
	row, err := sqlcgen.New(q).InsertDefinition(ctx, sqlcgen.InsertDefinitionParams{
		ID: r.ids.New(ids.PrefixWorkflow), Name: d.Name, Version: version,
		Definition: d.Definition, CreatedBy: d.CreatedBy,
	})
	if err != nil {
		msg := fmt.Sprintf("workflow %q version %d already exists", d.Name, d.Version)
		return persistence.DefinitionView{}, conflictOr(err, msg, "insert workflow definition")
	}
	return definitionView(row), nil
}

// Get implements persistence.DefinitionRepo.
func (r *DefinitionRepo) Get(ctx context.Context, q persistence.Querier, id string) (persistence.DefinitionView, error) {
	row, err := sqlcgen.New(q).GetDefinition(ctx, id)
	if err != nil {
		return persistence.DefinitionView{}, notFoundOr(err, "workflow definition")
	}
	return definitionView(row), nil
}

// GetByNameVersion implements persistence.DefinitionRepo.
func (r *DefinitionRepo) GetByNameVersion(ctx context.Context, q persistence.Querier, name string, version int) (persistence.DefinitionView, error) {
	v, err := int32Of("version", version)
	if err != nil {
		return persistence.DefinitionView{}, err
	}
	row, err := sqlcgen.New(q).GetDefinitionByNameVersion(ctx, sqlcgen.GetDefinitionByNameVersionParams{Name: name, Version: v})
	if err != nil {
		return persistence.DefinitionView{}, notFoundOr(err, "workflow definition")
	}
	return definitionView(row), nil
}

// Latest implements persistence.DefinitionRepo.
func (r *DefinitionRepo) Latest(ctx context.Context, q persistence.Querier, name string) (persistence.DefinitionView, error) {
	row, err := sqlcgen.New(q).GetLatestDefinition(ctx, name)
	if err != nil {
		return persistence.DefinitionView{}, notFoundOr(err, "workflow definition")
	}
	return definitionView(row), nil
}

// List implements persistence.DefinitionRepo.
func (r *DefinitionRepo) List(ctx context.Context, q persistence.Querier, p persistence.Page) ([]persistence.DefinitionView, string, error) {
	at, id, err := cursorArgs(p.Cursor)
	if err != nil {
		return nil, "", err
	}
	limit := p.EffectiveLimit()
	rows, err := sqlcgen.New(q).ListDefinitions(ctx, sqlcgen.ListDefinitionsParams{
		CursorCreatedAt: at, CursorID: id, Lim: limitOf(limit + 1),
	})
	if err != nil {
		return nil, "", fmt.Errorf("list workflow definitions: %w", err)
	}
	out := make([]persistence.DefinitionView, len(rows))
	for i, row := range rows {
		out[i] = definitionView(row)
	}
	out, next := page(out, limit, func(d persistence.DefinitionView) (time.Time, string) { return d.CreatedAt, d.ID })
	return out, next, nil
}
