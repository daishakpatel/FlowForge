package apperrors

import "net/http"

// HTTPStatus returns the 00E §4 HTTP status for c. NOT_LEADER and NO_QUORUM are internal codes
// with no HTTP mapping in 00E; they return 503 because to an HTTP client they mean "try again".
// Unknown codes return 500.
func HTTPStatus(c Code) int {
	switch c {
	case CodeInvalidArgument:
		return http.StatusBadRequest
	case CodeInvalidWorkflow:
		return http.StatusUnprocessableEntity
	case CodeUnauthenticated:
		return http.StatusUnauthorized
	case CodePermissionDenied:
		return http.StatusForbidden
	case CodeNotFound:
		return http.StatusNotFound
	case CodeConflict, CodeInvalidState, CodeStaleAttempt:
		return http.StatusConflict
	case CodePayloadTooLarge:
		return http.StatusRequestEntityTooLarge
	case CodeRateLimited:
		return http.StatusTooManyRequests
	case CodeUnavailable, CodeNotLeader, CodeNoQuorum:
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}
