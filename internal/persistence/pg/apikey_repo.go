package pg

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daishakpatel/flowforge/internal/apperrors"
	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/persistence/sqlcgen"
)

// APIKeyRepo implements persistence.APIKeyRepo.
type APIKeyRepo struct{}

var _ persistence.APIKeyRepo = APIKeyRepo{}

func apiKeyView(r sqlcgen.ApiKey) persistence.APIKeyView {
	return persistence.APIKeyView{
		ID: r.ID, Name: r.Name, Role: r.Role, SecretHash: r.SecretHash, Salt: r.Salt, CreatedBy: r.CreatedBy,
		CreatedAt: r.CreatedAt.Time, ExpiresAt: timePtr(r.ExpiresAt), RevokedAt: timePtr(r.RevokedAt),
	}
}

// Create implements persistence.APIKeyRepo.
func (APIKeyRepo) Create(ctx context.Context, q persistence.Querier, k persistence.NewAPIKey) (persistence.APIKeyView, error) {
	var expires time.Time
	if k.ExpiresAt != nil {
		expires = *k.ExpiresAt
	}
	row, err := sqlcgen.New(q).InsertAPIKey(ctx, sqlcgen.InsertAPIKeyParams{
		ID: k.ID, Name: k.Name, Role: k.Role, SecretHash: k.SecretHash, Salt: k.Salt,
		CreatedBy: k.CreatedBy, ExpiresAt: timestamptz(expires),
	})
	if err != nil {
		return persistence.APIKeyView{}, conflictOr(err, "an API key with this id already exists", "insert api key")
	}
	return apiKeyView(row), nil
}

// CreateIfAbsent implements persistence.APIKeyRepo.
func (APIKeyRepo) CreateIfAbsent(ctx context.Context, q persistence.Querier, k persistence.NewAPIKey) (bool, error) {
	n, err := sqlcgen.New(q).InsertAPIKeyIfAbsent(ctx, sqlcgen.InsertAPIKeyIfAbsentParams{
		ID: k.ID, Name: k.Name, Role: k.Role, SecretHash: k.SecretHash, Salt: k.Salt, CreatedBy: k.CreatedBy,
	})
	if err != nil {
		return false, fmt.Errorf("insert api key: %w", err)
	}
	return n == 1, nil
}

// Get implements persistence.APIKeyRepo.
func (APIKeyRepo) Get(ctx context.Context, q persistence.Querier, id string) (persistence.APIKeyView, error) {
	row, err := sqlcgen.New(q).GetAPIKey(ctx, id)
	if err != nil {
		return persistence.APIKeyView{}, notFoundOr(err, "api key")
	}
	return apiKeyView(row), nil
}

// List implements persistence.APIKeyRepo.
func (APIKeyRepo) List(ctx context.Context, q persistence.Querier, p persistence.Page) ([]persistence.APIKeyView, string, error) {
	at, id, err := cursorArgs(p.Cursor)
	if err != nil {
		return nil, "", err
	}
	limit := p.EffectiveLimit()
	rows, err := sqlcgen.New(q).ListAPIKeys(ctx, sqlcgen.ListAPIKeysParams{CursorCreatedAt: at, CursorID: id, Lim: limitOf(limit + 1)})
	if err != nil {
		return nil, "", fmt.Errorf("list api keys: %w", err)
	}
	out := make([]persistence.APIKeyView, len(rows))
	for i, r := range rows {
		out[i] = apiKeyView(r)
	}
	out, next := page(out, limit, func(k persistence.APIKeyView) (time.Time, string) { return k.CreatedAt, k.ID })
	return out, next, nil
}

// HasActiveAdmin implements persistence.APIKeyRepo.
func (APIKeyRepo) HasActiveAdmin(ctx context.Context, q persistence.Querier) (bool, error) {
	ok, err := sqlcgen.New(q).HasActiveAdmin(ctx)
	if err != nil {
		return false, fmt.Errorf("check for an admin key: %w", err)
	}
	return ok, nil
}

// Revoke implements persistence.APIKeyRepo.
func (r APIKeyRepo) Revoke(ctx context.Context, tx pgx.Tx, id string) error {
	k, err := r.Get(ctx, tx, id)
	if err != nil {
		return err
	}
	if k.RevokedAt != nil {
		return nil
	}
	q := sqlcgen.New(tx)
	if k.Role == "admin" {
		admins, err := q.LockActiveAdmins(ctx)
		if err != nil {
			return fmt.Errorf("lock admin keys: %w", err)
		}
		if slices.Contains(admins, id) && len(admins) == 1 {
			return apperrors.Conflict("cannot revoke the last active admin key")
		}
	}
	if _, err := q.RevokeAPIKey(ctx, id); err != nil {
		return fmt.Errorf("revoke api key %s: %w", id, err)
	}
	return nil
}

// AuditRepo implements persistence.AuditRepo.
type AuditRepo struct{}

var _ persistence.AuditRepo = AuditRepo{}

// Record implements persistence.AuditRepo.
func (AuditRepo) Record(ctx context.Context, q persistence.Querier, e persistence.AuditEntry) error {
	details, err := json.Marshal(e.Details)
	if err != nil {
		return fmt.Errorf("encode audit details: %w", err)
	}
	err = sqlcgen.New(q).InsertAuditLog(ctx, sqlcgen.InsertAuditLogParams{
		ActorKeyID: text(e.ActorKeyID), ActorRole: text(e.ActorRole), Action: e.Action,
		ResourceType: text(e.ResourceType), ResourceID: text(e.ResourceID), RequestID: e.RequestID,
		Details: details, Outcome: e.Outcome,
	})
	if err != nil {
		return fmt.Errorf("insert audit row: %w", err)
	}
	return nil
}
