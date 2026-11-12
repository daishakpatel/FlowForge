// Package apperrors is FlowForge's application error type (task 01.04, 00E §4).
//
// Domain and repository code returns *Error values (or wraps them with %w); the API layer turns
// them into the REST envelope with HTTPStatus and into gRPC statuses with GRPCCode. Callers
// branch with errors.As or CodeOf, never on message text.
package apperrors

import (
	"errors"
	"time"
)

// Detail is one entry of the error envelope's details list, for example one failed DAG check.
// Code and TaskID carry a DAG validation issue's stable code and task (task 02.04).
type Detail struct {
	Field   string `json:"field,omitempty"`
	Code    string `json:"code,omitempty"`
	TaskID  string `json:"task_id,omitempty"`
	Message string `json:"message"`
}

// Error is an application error. Message and Details are safe to show to clients; Cause is for
// logs only and must never reach a client (00C §10.1).
type Error struct {
	Code       Code
	Message    string
	Details    []Detail
	Cause      error
	RetryAfter time.Duration // sets the Retry-After header for RATE_LIMITED and UNAVAILABLE
}

func (e *Error) Error() string {
	s := string(e.Code)
	if e.Message != "" {
		s += ": " + e.Message
	}
	if e.Cause != nil {
		s += ": " + e.Cause.Error()
	}
	return s
}

// Unwrap returns the cause, so errors.Is/As see through an *Error.
func (e *Error) Unwrap() error { return e.Cause }

// Is matches another *Error with the same code, so errors.Is(err, &Error{Code: CodeNotFound})
// tests for a code anywhere in the chain.
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && t.Code == e.Code
}

// New returns an *Error with the given code and client-safe message.
func New(code Code, message string) *Error { return &Error{Code: code, Message: message} }

// Wrap returns an *Error with the given code and message that wraps cause.
func Wrap(code Code, message string, cause error) *Error {
	return &Error{Code: code, Message: message, Cause: cause}
}

// InvalidArgument reports a malformed or invalid request.
func InvalidArgument(message string, details ...Detail) *Error {
	return &Error{Code: CodeInvalidArgument, Message: message, Details: details}
}

// InvalidWorkflow reports failed DAG validation; each detail is one problem.
func InvalidWorkflow(message string, details ...Detail) *Error {
	return &Error{Code: CodeInvalidWorkflow, Message: message, Details: details}
}

// Unauthenticated reports a missing or invalid credential.
func Unauthenticated(message string) *Error { return New(CodeUnauthenticated, message) }

// PermissionDenied reports a role that may not perform the action.
func PermissionDenied(message string) *Error { return New(CodePermissionDenied, message) }

// NotFound reports a missing resource.
func NotFound(message string) *Error { return New(CodeNotFound, message) }

// Conflict reports a duplicate or a version conflict.
func Conflict(message string) *Error { return New(CodeConflict, message) }

// InvalidState reports an illegal state transition, including a conditional UPDATE that
// affected 0 rows (ADR-020).
func InvalidState(message string) *Error { return New(CodeInvalidState, message) }

// StaleAttempt reports a fencing-token mismatch or an attempt that is no longer RUNNING (ADR-003).
func StaleAttempt(message string) *Error { return New(CodeStaleAttempt, message) }

// PayloadTooLarge reports a body over its size limit.
func PayloadTooLarge(message string) *Error { return New(CodePayloadTooLarge, message) }

// RateLimited reports a throttled caller; retryAfter sets Retry-After.
func RateLimited(message string, retryAfter time.Duration) *Error {
	return &Error{Code: CodeRateLimited, Message: message, RetryAfter: retryAfter}
}

// Unavailable reports a dependency that is down; retryAfter sets Retry-After.
func Unavailable(message string, retryAfter time.Duration, cause error) *Error {
	return &Error{Code: CodeUnavailable, Message: message, RetryAfter: retryAfter, Cause: cause}
}

// Internal reports a bug. The client sees only "internal error"; cause is kept for the log.
func Internal(cause error) *Error { return Wrap(CodeInternal, internalMessage, cause) }

// NotLeader redirects a Raft or leader-only call; leaderHint names the current leader if known.
func NotLeader(leaderHint string) *Error {
	e := New(CodeNotLeader, "not the leader")
	if leaderHint != "" {
		e.Details = []Detail{{Field: "leader_hint", Message: leaderHint}}
	}
	return e
}

// NoQuorum reports a Raft write that cannot reach a quorum.
func NoQuorum(message string) *Error { return New(CodeNoQuorum, message) }

const internalMessage = "internal error"

// FromError returns err as an *Error: the first *Error in its chain, or INTERNAL with the
// client-safe message "internal error" and err as the cause. It returns nil for nil.
func FromError(err error) *Error {
	if err == nil {
		return nil
	}
	if e, ok := errors.AsType[*Error](err); ok {
		return e
	}
	return Internal(err)
}

// CodeOf returns the code of the first *Error in err's chain, INTERNAL for any other error,
// and "" for nil.
func CodeOf(err error) Code {
	if err == nil {
		return ""
	}
	return FromError(err).Code
}

// Retryable reports whether err carries a retryable code (see Code.Retryable).
func Retryable(err error) bool { return err != nil && CodeOf(err).Retryable() }
