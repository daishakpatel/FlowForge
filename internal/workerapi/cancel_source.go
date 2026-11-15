package workerapi

import (
	"context"

	"github.com/daishakpatel/flowforge/internal/persistence"
)

// CodeCancelled is the error code a worker reports with FailTask when it stopped an attempt
// because the server cancelled it; the attempt is recorded as CANCELLED, not FAILED (03.05).
const CodeCancelled = "CANCELLED"

// RunCancelSource is the CancelSource from task 03.05: of the attempts a worker heartbeats, the
// RUNNING ones in a cancel-requested run. The worker cancels each handler's context and reports
// FailTask(CANCELLED); attempts it never stops are force-cancelled when the run's cancel grace
// runs out (scheduler.TimeoutLoop).
type RunCancelSource struct {
	DB   persistence.Querier
	Runs persistence.RunControlRepo
}

var _ CancelSource = RunCancelSource{}

// CancelledAttempts implements CancelSource.
func (c RunCancelSource) CancelledAttempts(ctx context.Context, attemptIDs []string) ([]string, error) {
	return c.Runs.CancelRequestedAttempts(ctx, c.DB, attemptIDs)
}
