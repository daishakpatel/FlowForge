package apperrors

// Code is an error code from the 00E §4 catalog. The values are part of the REST and gRPC
// contract: never rename one, only add new codes (and update 00E §4 first).
type Code string

// The 00E §4 error codes.
const (
	CodeInvalidArgument  Code = "INVALID_ARGUMENT"
	CodeInvalidWorkflow  Code = "INVALID_WORKFLOW"
	CodeUnauthenticated  Code = "UNAUTHENTICATED"
	CodePermissionDenied Code = "PERMISSION_DENIED"
	CodeNotFound         Code = "NOT_FOUND"
	CodeConflict         Code = "CONFLICT"
	CodeInvalidState     Code = "INVALID_STATE"
	CodeStaleAttempt     Code = "STALE_ATTEMPT"
	CodePayloadTooLarge  Code = "PAYLOAD_TOO_LARGE"
	CodeRateLimited      Code = "RATE_LIMITED"
	CodeUnavailable      Code = "UNAVAILABLE"
	CodeInternal         Code = "INTERNAL"
	CodeNotLeader        Code = "NOT_LEADER"
	CodeNoQuorum         Code = "NO_QUORUM"
)

// Codes lists every code in 00E §4 order.
func Codes() []Code {
	return []Code{
		CodeInvalidArgument, CodeInvalidWorkflow, CodeUnauthenticated, CodePermissionDenied,
		CodeNotFound, CodeConflict, CodeInvalidState, CodeStaleAttempt, CodePayloadTooLarge,
		CodeRateLimited, CodeUnavailable, CodeInternal, CodeNotLeader, CodeNoQuorum,
	}
}

// Retryable reports whether an operation that failed with c may succeed if retried unchanged
// (00C §10.1). Retry only idempotent operations, and honour Error.RetryAfter when set.
func (c Code) Retryable() bool {
	switch c {
	case CodeUnavailable, CodeRateLimited, CodeNotLeader, CodeNoQuorum:
		return true
	default:
		return false
	}
}
