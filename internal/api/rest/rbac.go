package rest

import (
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/daishakpatel/flowforge/internal/api/auth"
	"github.com/daishakpatel/flowforge/internal/apperrors"
)

// rbacMiddleware rejects a caller whose role does not satisfy the matched route's ACL entry with
// 403 PERMISSION_DENIED (FR-SEC-02). It runs per route, after authentication, so the route
// pattern is known.
func rbacMiddleware(acl map[string]string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			p, _ := auth.PrincipalFrom(r.Context())
			need := requiredRole(acl, r.Method, chi.RouteContext(r.Context()).RoutePattern())
			if !auth.Allowed(p.Role, need) {
				writeError(w, r, apperrors.PermissionDenied(fmt.Sprintf("role %q may not call this route (requires %s)", p.Role, need)))
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
