package auth

import "slices"

// The roles of ADR-015 / FR-SEC-02, as stored in api_keys.role.
const (
	RoleViewer     = "viewer"
	RoleOperator   = "operator"
	RoleAdmin      = "admin"
	RoleWorker     = "worker"      // service role: the worker gRPC API (03.01) only
	RoleAIReadonly = "ai-readonly" // service role: the internal read API (06.02) only
)

// Roles lists every valid role.
func Roles() []string {
	return []string{RoleViewer, RoleOperator, RoleAdmin, RoleWorker, RoleAIReadonly}
}

// humanRank orders the human roles: viewer < operator < admin.
func humanRank(role string) int {
	return slices.Index([]string{RoleViewer, RoleOperator, RoleAdmin}, role) + 1 // 0 = not a human role
}

// IsServiceRole reports whether role is a service role (worker, ai-readonly).
func IsServiceRole(role string) bool { return role == RoleWorker || role == RoleAIReadonly }

// Allowed reports whether a caller with role may use something that requires required. Human
// roles are ordered (viewer < operator < admin); a service role is valid only where it is the
// requirement itself, so worker and ai-readonly keys can never reach a human route and no human
// key can reach a service route.
func Allowed(role, required string) bool {
	if IsServiceRole(required) || IsServiceRole(role) {
		return role == required
	}
	return humanRank(role) > 0 && humanRank(role) >= humanRank(required)
}
