// Package auth is the API's authentication seam (task 02.04). Handlers read the caller with
// PrincipalFrom; the REST middleware fills it in from an Authenticator. Task 02.05 adds the real
// API-key Authenticator and RBAC without changing handlers.
package auth

import (
	"context"
	"net/http"
)

// Principal is the authenticated caller.
type Principal struct {
	KeyID string // recorded as created_by on what the caller creates
	Name  string
	Role  string // viewer | operator | admin | worker | ai-readonly (ADR-015)
}

// Authenticator identifies callers. Both methods return an UNAUTHENTICATED *apperrors.Error for
// a missing or invalid credential.
type Authenticator interface {
	// Authenticate reads the credential from an HTTP request (Authorization: Bearer …).
	Authenticate(r *http.Request) (Principal, error)
	// VerifyToken checks a raw API key outside HTTP, for gRPC interceptors (03.01).
	VerifyToken(ctx context.Context, token string) (Principal, error)
}

type principalKey struct{}

// WithPrincipal returns ctx carrying p, for PrincipalFrom.
func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, p)
}

// PrincipalFrom returns the caller the auth middleware stored, and whether there is one.
func PrincipalFrom(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(Principal)
	return p, ok
}
