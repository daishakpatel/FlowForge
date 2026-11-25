package events

import (
	"strings"
	"testing"

	"github.com/daishakpatel/flowforge/internal/workflow/model"
	"github.com/daishakpatel/flowforge/internal/workflow/statemachine"
	pkgevents "github.com/daishakpatel/flowforge/pkg/events"
)

// TestRecord_EnvelopesMatchSchema is the contract between the Go producer and the published schema
// (05.01): the outbox envelope of every 00E §7 event type validates against flowforge_event.v1,
// which the Kafka relay (05.08) checks before publishing.
func TestRecord_EnvelopesMatchSchema(t *testing.T) {
	t.Parallel()
	reg, err := pkgevents.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	retryable := true
	for _, typ := range statemachine.EventTypes() {
		var ev Event
		switch s := string(typ); {
		case strings.HasPrefix(s, "run."):
			ev = NewRunEvent(typ, RunPayload{
				RunID: "run_1", DefinitionID: "def_1", Status: model.RunRunning, PreviousStatus: model.RunCreated,
				Reason: "re-running after the data fix", Actor: "key_1", SupersededTaskIDs: []string{"task_1"}, RerunTaskIDs: []string{"task_2"},
			})
		case strings.HasPrefix(s, "task.attempt."):
			ev = NewAttemptEvent(typ, AttemptPayload{
				RunID: "run_1", TaskID: "task_1", AttemptID: "att_1", AttemptNumber: 2, WorkerID: "wrk_1",
				FencingToken: 9, Status: model.AttemptFailed, ErrorCode: "TASK_TIMEOUT", Retryable: &retryable,
			})
		case strings.HasPrefix(s, "task."):
			ev = NewTaskEvent(typ, TaskPayload{
				RunID: "run_1", TaskID: "task_1", TaskKey: "fetch", Status: model.TaskQueued, PreviousStatus: model.TaskReady,
				AttemptCount: 1, RetryDelayMS: 2000, ErrorCode: "INVALID_INPUT",
			})
		case strings.HasPrefix(s, "approval."):
			ev = NewApprovalEvent(typ, ApprovalPayload{ApprovalID: "apr_1", RunID: "run_1", ActionType: "retry_task", Risk: "low", Status: "PENDING"})
		case strings.HasPrefix(s, "worker."):
			ev = NewWorkerEvent(typ, WorkerPayload{WorkerID: "wrk_1", Reason: "lease_expired"})
		default:
			ev = NewLeaderChanged(LeaderChangedPayload{NodeID: "scheduler-1", Epoch: 4})
		}
		tx := &execTx{}
		if err := newRecorder().Record(t.Context(), tx, ev); err != nil {
			t.Fatalf("%s: %v", typ, err)
		}
		out, ok := tx.insert("outbox")
		if !ok {
			t.Fatalf("%s: no outbox insert", typ)
		}
		if err := reg.Validate("flowforge_event.v1", out.args[3].([]byte)); err != nil {
			t.Errorf("%s: recorded envelope does not match flowforge_event.v1: %v", typ, err)
		}
	}
}
