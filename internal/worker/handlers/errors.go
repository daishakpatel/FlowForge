package handlers

import (
	"context"
	"errors"
)

// Error codes the worker reports for outcomes a handler does not classify itself.
const (
	CodeHandlerError   = "HANDLER_ERROR"   // an unclassified handler error (retryable)
	CodePanic          = "PANIC"           // the handler panicked (retryable)
	CodeCancelled      = "CANCELLED"       // the server cancelled the attempt (not retryable)
	CodeDeadline       = "DEADLINE"        // the handler overran the attempt deadline (retryable)
	CodeWorkerShutdown = "WORKER_SHUTDOWN" // the worker shut down before the handler finished (retryable)
	CodeInvalidInput   = "INVALID_INPUT"   // the task input does not fit the handler (not retryable)
	CodeUnknownType    = "UNKNOWN_TASK_TYPE"
)

// maxCodeLen is the server's limit on error_code (worker protocol).
const maxCodeLen = 64

// Error is a classified handler error. Build it with Retryable or NonRetryable.
type Error struct {
	Code      string
	Retryable bool
	Err       error
}

func (e *Error) Error() string {
	if e.Err == nil {
		return e.Code
	}
	return e.Code + ": " + e.Err.Error()
}

func (e *Error) Unwrap() error { return e.Err }

// Retryable classifies err as a failure worth another attempt, with code as its error code.
func Retryable(code string, err error) error { return &Error{Code: code, Retryable: true, Err: err} }

// NonRetryable classifies err as a failure another attempt cannot fix (bad input, a permanent
// rejection), with code as its error code. Task 03.04's retry policy never retries it.
func NonRetryable(code string, err error) error {
	return &Error{Code: code, Retryable: false, Err: err}
}

// Classify returns the error code and retryability the worker reports for a handler error:
// the classification of the outermost *Error in err's chain, else a retryable HANDLER_ERROR.
// context.Canceled is CANCELLED (not retryable); context.DeadlineExceeded is DEADLINE
// (retryable). An empty or over-long code becomes HANDLER_ERROR, keeping its retryability.
func Classify(err error) (code string, retryable bool) {
	var e *Error
	switch {
	case errors.As(err, &e):
		code, retryable = e.Code, e.Retryable
	case errors.Is(err, context.Canceled):
		return CodeCancelled, false
	case errors.Is(err, context.DeadlineExceeded):
		return CodeDeadline, true
	default:
		return CodeHandlerError, true
	}
	if code == "" || len(code) > maxCodeLen {
		code = CodeHandlerError
	}
	return code, retryable
}
