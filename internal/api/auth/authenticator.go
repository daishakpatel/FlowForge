package auth

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/daishakpatel/flowforge/internal/apperrors"
	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/persistence"
)

// CacheTTL bounds how long a key lookup is reused: a key revoked through another API instance
// keeps working here for at most this long (revocation through this instance is immediate).
const CacheTTL = 30 * time.Second

const cacheEntries = 1024

// KeyAuthenticator authenticates API keys stored in api_keys (task 02.05, ADR-015). Safe for
// concurrent use.
type KeyAuthenticator struct {
	db    persistence.Querier
	keys  persistence.APIKeyRepo
	clock clock.Clock
	cache *keyCache
	// compare checks a secret against a stored salt and hash; tests replace it to observe calls.
	compare func(salt, hash []byte, secret string) bool
}

var _ Authenticator = (*KeyAuthenticator)(nil)

// NewKeyAuthenticator returns an authenticator reading keys through keys on db.
func NewKeyAuthenticator(db persistence.Querier, keys persistence.APIKeyRepo, c clock.Clock) *KeyAuthenticator {
	return &KeyAuthenticator{db: db, keys: keys, clock: c, cache: newKeyCache(c, CacheTTL, cacheEntries), compare: verifySecret}
}

// Authenticate reads the key from "Authorization: Bearer ff_…" and verifies it.
func (a *KeyAuthenticator) Authenticate(r *http.Request) (Principal, error) {
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || token == "" {
		return Principal{}, apperrors.Unauthenticated("missing API key: send Authorization: Bearer ff_…")
	}
	return a.VerifyToken(r.Context(), token)
}

// VerifyToken checks a raw ff_<id>_<secret> token and returns its principal. It is
// transport-free, so gRPC interceptors (03.01) use it too; they add their own role check.
//
// Every rejection is the same UNAUTHENTICATED error whatever the reason (malformed, unknown,
// wrong secret, revoked, expired), and an unknown key id still costs one hash comparison, so
// neither the message nor the timing tells which key ids exist. A database failure is returned
// as is (the caller maps it to UNAVAILABLE), never as UNAUTHENTICATED.
func (a *KeyAuthenticator) VerifyToken(ctx context.Context, token string) (Principal, error) {
	id, secret, err := ParseToken(token)
	if err != nil {
		return Principal{}, invalidKey()
	}
	k, err := a.lookup(ctx, id)
	switch {
	case apperrors.CodeOf(err) == apperrors.CodeNotFound:
		var salt [saltBytes]byte
		var hash [32]byte
		a.compare(salt[:], hash[:], secret) // same work as a real check; the result is ignored
		return Principal{}, invalidKey()
	case err != nil:
		return Principal{}, err
	}
	if !a.compare(k.Salt, k.SecretHash, secret) || !k.Active(a.clock.Now()) {
		return Principal{}, invalidKey()
	}
	return Principal{KeyID: k.ID, Name: k.Name, Role: k.Role}, nil
}

// Invalidate drops a key from the cache, so its revocation takes effect here immediately.
func (a *KeyAuthenticator) Invalidate(keyID string) { a.cache.remove(keyID) }

func (a *KeyAuthenticator) lookup(ctx context.Context, id string) (persistence.APIKeyView, error) {
	if k, ok := a.cache.get(id); ok {
		return k, nil
	}
	k, err := a.keys.Get(ctx, a.db, id)
	if err != nil {
		return persistence.APIKeyView{}, err
	}
	a.cache.put(k)
	return k, nil
}

func invalidKey() error { return apperrors.Unauthenticated("invalid API key") }
