package rest

import "github.com/daishakpatel/flowforge/internal/api/auth"

// routeACL is the minimum role of every /api/v1 route, keyed "METHOD pattern" (00E §9). It is
// the authorization source of truth together with that table. Every route a task adds MUST get
// an entry here: TestACL_EveryRouteHasAnEntry fails the build otherwise, and a route that slips
// through anyway is admin-only (requiredRole).
func routeACL() map[string]string {
	return map[string]string{
		"POST /api/v1/workflows":                 auth.RoleOperator,
		"GET /api/v1/workflows":                  auth.RoleViewer,
		"GET /api/v1/workflows/{id}":             auth.RoleViewer,
		"GET /api/v1/workflows/by-name/{name}":   auth.RoleViewer,
		"POST /api/v1/workflows/{id}/runs":       auth.RoleOperator,
		"GET /api/v1/runs":                       auth.RoleViewer,
		"GET /api/v1/runs/{run_id}":              auth.RoleViewer,
		"GET /api/v1/runs/{run_id}/tasks":        auth.RoleViewer,
		"GET /api/v1/runs/{run_id}/events":       auth.RoleViewer,
		"GET /api/v1/tasks/{task_id}":            auth.RoleViewer,
		"GET /api/v1/tasks/{task_id}/attempts":   auth.RoleViewer,
		"POST /api/v1/api-keys":                  auth.RoleAdmin,
		"GET /api/v1/api-keys":                   auth.RoleAdmin,
		"DELETE /api/v1/api-keys/{key_id}":       auth.RoleAdmin,
		"GET /api/v1/workers":                    auth.RoleViewer,
		"GET /api/v1/attempts/{attempt_id}/logs": auth.RoleViewer,
	}
}

// requiredRole returns the minimum role for a route; a route without an entry is admin-only.
func requiredRole(acl map[string]string, method, pattern string) string {
	if role, ok := acl[method+" "+pattern]; ok {
		return role
	}
	return auth.RoleAdmin
}
