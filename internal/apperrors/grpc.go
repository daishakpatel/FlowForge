package apperrors

import "google.golang.org/grpc/codes"

// GRPCCode returns the 00E §4 gRPC code for c. CONFLICT maps to AlreadyExists; 00E also allows
// Aborted for version conflicts, which a gRPC handler can choose explicitly. Unknown codes
// return Internal.
func GRPCCode(c Code) codes.Code {
	switch c {
	case CodeInvalidArgument, CodeInvalidWorkflow:
		return codes.InvalidArgument
	case CodeUnauthenticated:
		return codes.Unauthenticated
	case CodePermissionDenied:
		return codes.PermissionDenied
	case CodeNotFound:
		return codes.NotFound
	case CodeConflict:
		return codes.AlreadyExists
	case CodeInvalidState, CodeStaleAttempt, CodeNotLeader:
		return codes.FailedPrecondition
	case CodePayloadTooLarge, CodeRateLimited:
		return codes.ResourceExhausted
	case CodeUnavailable, CodeNoQuorum:
		return codes.Unavailable
	default:
		return codes.Internal
	}
}
