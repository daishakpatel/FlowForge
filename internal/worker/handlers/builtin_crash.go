package handlers

import (
	"context"
	"encoding/json"
	"errors"
)

// Crash kills the worker process mid-attempt (Exit(1)) for chaos tests: the attempt is left
// RUNNING for recovery (03.04) to find. Register it only with FF_WORKER_ENABLE_CRASH_HANDLER=true.
type Crash struct {
	Exit func(code int) // os.Exit in the binary; a recorder in tests
}

// Type implements Handler.
func (Crash) Type() string { return "crash" }

// Execute implements Handler. It returns only when Exit returns, which happens in tests.
func (c Crash) Execute(ctx context.Context, tc TaskContext) (json.RawMessage, error) {
	if tc.Logger != nil {
		tc.Logger.WarnContext(ctx, "crash handler exiting the worker process")
	}
	c.Exit(1)
	return nil, NonRetryable("CRASH_RETURNED", errors.New("crash handler: exit returned"))
}
