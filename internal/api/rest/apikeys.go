package rest

import (
	"crypto/rand"
	"net/http"
	"slices"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"

	"github.com/daishakpatel/flowforge/internal/api/auth"
	"github.com/daishakpatel/flowforge/internal/apperrors"
	"github.com/daishakpatel/flowforge/internal/persistence"
)

const maxKeyNameLen = 100

// apiKeyResponse is the APIKey schema. The secret, salt and hash are never part of it.
type apiKeyResponse struct {
	ID        string     `json:"id"`
	Name      string     `json:"name"`
	Role      string     `json:"role"`
	CreatedBy string     `json:"created_by"`
	CreatedAt time.Time  `json:"created_at"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
	RevokedAt *time.Time `json:"revoked_at,omitempty"`
}

// createdAPIKeyResponse is the CreatedAPIKey schema: the only response that carries the key.
type createdAPIKeyResponse struct {
	apiKeyResponse
	Key string `json:"key"`
}

func toAPIKey(k persistence.APIKeyView) apiKeyResponse {
	return apiKeyResponse{
		ID: k.ID, Name: k.Name, Role: k.Role, CreatedBy: k.CreatedBy, CreatedAt: k.CreatedAt.UTC(),
		ExpiresAt: utc(k.ExpiresAt), RevokedAt: utc(k.RevokedAt),
	}
}

type createAPIKeyRequest struct {
	Name      string     `json:"name"`
	Role      string     `json:"role"`
	ExpiresAt *time.Time `json:"expires_at"`
}

// createAPIKey creates a key and returns its secret, this once (FR-SEC-01).
func (h *handlers) createAPIKey(w http.ResponseWriter, r *http.Request) {
	var req createAPIKeyRequest
	if err := decodeOptionalJSON(r, &req); err != nil {
		writeError(w, r, err)
		return
	}
	switch {
	case req.Name == "" || len(req.Name) > maxKeyNameLen:
		writeError(w, r, apperrors.InvalidArgument("name is required and at most 100 characters"))
		return
	case !slices.Contains(auth.Roles(), req.Role):
		writeError(w, r, apperrors.InvalidArgument("role must be one of viewer, operator, admin, worker, ai-readonly"))
		return
	case req.ExpiresAt != nil && !req.ExpiresAt.After(h.clock.Now()):
		writeError(w, r, apperrors.InvalidArgument("expires_at must be in the future"))
		return
	}
	k, err := auth.GenerateKey(rand.Reader)
	if err != nil {
		writeError(w, r, err)
		return
	}
	v, err := h.keys.Create(r.Context(), h.db, persistence.NewAPIKey{
		ID: k.ID, Name: req.Name, Role: req.Role, SecretHash: k.Hash, Salt: k.Salt,
		CreatedBy: actor(r), ExpiresAt: req.ExpiresAt,
	})
	if err != nil {
		writeError(w, r, err)
		return
	}
	w.Header().Set("Location", "/api/v1/api-keys/"+v.ID)
	writeJSON(w, http.StatusCreated, createdAPIKeyResponse{apiKeyResponse: toAPIKey(v), Key: k.Token})
}

func (h *handlers) listAPIKeys(w http.ResponseWriter, r *http.Request) {
	page, err := pageParams(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	keys, next, err := h.keys.List(r.Context(), h.db, page)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, newList(mapList(keys, toAPIKey), next))
}

// revokeAPIKey revokes a key (idempotently) and drops it from this instance's cache, so it stops
// working here at once; other instances stop accepting it within auth.CacheTTL.
func (h *handlers) revokeAPIKey(w http.ResponseWriter, r *http.Request) {
	ctx, id := r.Context(), chi.URLParam(r, "key_id")
	err := h.tx.WithTx(ctx, pgx.TxOptions{}, func(tx pgx.Tx) error { return h.keys.Revoke(ctx, tx, id) })
	if err != nil {
		writeError(w, r, err)
		return
	}
	if inv, ok := h.auth.(interface{ Invalidate(keyID string) }); ok {
		inv.Invalidate(id)
	}
	w.WriteHeader(http.StatusNoContent)
}
