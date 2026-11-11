package events

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/daishakpatel/flowforge/internal/apperrors"
	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/ids"
	"github.com/daishakpatel/flowforge/internal/logging"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
	"github.com/daishakpatel/flowforge/internal/workflow/statemachine"
)

// execTx records the Exec calls sqlc makes; every other pgx.Tx method panics.
type execTx struct {
	pgx.Tx
	execs []execCall
}

type execCall struct {
	sql  string
	args []any
}

func (t *execTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	t.execs = append(t.execs, execCall{sql, args})
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

func (t *execTx) insert(table string) (execCall, bool) {
	for _, c := range t.execs {
		if strings.Contains(c.sql, "INSERT INTO "+table+" ") {
			return c, true
		}
	}
	return execCall{}, false
}

var occurred = time.Date(2026, 11, 2, 14, 30, 0, 0, time.UTC)

func newRecorder() *Recorder {
	return NewRecorder(clock.NewFake(occurred), &ids.Fake{}, "flowforge-test")
}

func TestRecord_EnvelopeMatches00E(t *testing.T) {
	t.Parallel()
	tx := &execTx{}
	ev := NewTaskEvent(statemachine.EventTaskQueued, TaskPayload{
		RunID: "run_1", TaskID: "task_1", TaskKey: "fetch", Status: model.TaskQueued, PreviousStatus: model.TaskReady,
	})
	if err := newRecorder().Record(t.Context(), tx, ev); err != nil {
		t.Fatal(err)
	}
	out, ok := tx.insert("outbox")
	if !ok {
		t.Fatal("no outbox insert")
	}
	// InsertOutbox args: event_id, topic, key, envelope.
	if out.args[1] != TopicTaskEvents || out.args[2] != "run_1" {
		t.Errorf("topic, key = %v, %v; want %s, run_1", out.args[1], out.args[2], TopicTaskEvents)
	}
	var env map[string]any
	if err := json.Unmarshal(out.args[3].([]byte), &env); err != nil {
		t.Fatal(err)
	}
	// 00E §7 envelope keys; trace is omitted until tracing exists (08.01).
	want := []string{"event_id", "event_type", "event_version", "occurred_at", "producer", "correlation_id", "aggregate_id", "payload"}
	for _, k := range want {
		if _, ok := env[k]; !ok {
			t.Errorf("envelope lacks %q: %v", k, env)
		}
	}
	for k := range env {
		if !slices.Contains(want, k) {
			t.Errorf("envelope has unexpected key %q", k)
		}
	}
	checks := map[string]any{
		"event_id": out.args[0], "event_type": "task.queued", "event_version": 1.0,
		"occurred_at": "2026-11-02T14:30:00Z", "producer": "flowforge-test",
		"correlation_id": "run_1", "aggregate_id": "task_1",
	}
	for k, v := range checks {
		if env[k] != v {
			t.Errorf("envelope %s = %v, want %v", k, env[k], v)
		}
	}
	if !strings.HasPrefix(env["event_id"].(string), "evt_") {
		t.Errorf("event_id %v lacks the evt_ prefix", env["event_id"])
	}
	if p := env["payload"].(map[string]any); p["task_key"] != "fetch" || p["previous_status"] != "READY" {
		t.Errorf("payload = %v", p)
	}
	if _, ok := tx.insert("workflow_events"); !ok {
		t.Error("run-scoped event did not insert workflow_events")
	}
}

func TestRecord_CorrelationIDFromRequest(t *testing.T) {
	t.Parallel()
	tx := &execTx{}
	ctx := logging.WithFields(t.Context(), logging.KeyRequestID, "req_42")
	ev := NewRunEvent(statemachine.EventRunCreated, RunPayload{RunID: "run_1", DefinitionID: "wf_1", Status: model.RunCreated})
	if err := newRecorder().Record(ctx, tx, ev); err != nil {
		t.Fatal(err)
	}
	we, _ := tx.insert("workflow_events")
	// InsertWorkflowEvent args: event_id, run_id, task_id, event_type, event_version, payload, correlation_id.
	if we.args[6] != "req_42" {
		t.Errorf("correlation_id = %v, want req_42", we.args[6])
	}
}

func TestRecord_ClusterEventsGoToOutboxOnly(t *testing.T) {
	t.Parallel()
	for _, ev := range []Event{
		NewWorkerEvent(statemachine.EventWorkerLost, WorkerPayload{WorkerID: "w1", Reason: "lease_expired"}),
		NewLeaderChanged(LeaderChangedPayload{NodeID: "sched-1", Epoch: 7}),
	} {
		tx := &execTx{}
		if err := newRecorder().Record(t.Context(), tx, ev); err != nil {
			t.Fatalf("%s: %v", ev.Type, err)
		}
		out, ok := tx.insert("outbox")
		if _, we := tx.insert("workflow_events"); we || !ok {
			t.Errorf("%s: workflow_events=%v outbox=%v; want outbox only", ev.Type, we, ok)
		}
		if out.args[1] != TopicWorkflowEvents || out.args[2] != ev.AggregateID {
			t.Errorf("%s: topic, key = %v, %v", ev.Type, out.args[1], out.args[2])
		}
	}
}

func TestRecord_RejectsMismatchedOrIncompleteEvents(t *testing.T) {
	t.Parallel()
	bad := []Event{
		NewRunEvent(statemachine.EventTaskQueued, RunPayload{RunID: "run_1"}),                   // wrong family
		NewTaskEvent(statemachine.EventAttemptFailed, TaskPayload{RunID: "run_1", TaskID: "t"}), // attempt type on task payload
		NewTaskEvent("task.unknown", TaskPayload{RunID: "run_1", TaskID: "t"}),                  // not in 00E §7
		NewTaskEvent(statemachine.EventTaskReady, TaskPayload{TaskID: "t"}),                     // no run id
		{Type: statemachine.EventRunStarted, RunID: "run_1", AggregateID: "run_1"},              // no payload
	}
	for _, ev := range bad {
		tx := &execTx{}
		err := newRecorder().Record(t.Context(), tx, ev)
		if apperrors.CodeOf(err) != apperrors.CodeInternal || len(tx.execs) != 0 {
			t.Errorf("%s (%T): err=%v execs=%d; want INTERNAL and no writes", ev.Type, ev.Payload(), err, len(tx.execs))
		}
	}
}

// TestConstructors_CoverEvery00EType builds every 00E §7 type with its family constructor and
// checks it records and lands on the right topic.
func TestConstructors_CoverEvery00EType(t *testing.T) {
	t.Parallel()
	for _, typ := range statemachine.EventTypes() {
		var ev Event
		s := string(typ)
		switch {
		case strings.HasPrefix(s, "run."):
			ev = NewRunEvent(typ, RunPayload{RunID: "run_1"})
		case strings.HasPrefix(s, "task.attempt."):
			ev = NewAttemptEvent(typ, AttemptPayload{RunID: "run_1", TaskID: "task_1", AttemptID: "att_1"})
		case strings.HasPrefix(s, "task."):
			ev = NewTaskEvent(typ, TaskPayload{RunID: "run_1", TaskID: "task_1"})
		case strings.HasPrefix(s, "approval."):
			ev = NewApprovalEvent(typ, ApprovalPayload{ApprovalID: "apr_1", RunID: "run_1"})
		case strings.HasPrefix(s, "worker."):
			ev = NewWorkerEvent(typ, WorkerPayload{WorkerID: "w1"})
		default:
			ev = NewLeaderChanged(LeaderChangedPayload{NodeID: "n1"})
		}
		if ev.Type != typ {
			t.Errorf("constructor for %s built %s", typ, ev.Type)
		}
		if err := newRecorder().Record(t.Context(), &execTx{}, ev); err != nil {
			t.Errorf("%s: %v", typ, err)
		}
		wantTopic := TopicWorkflowEvents
		if strings.HasPrefix(s, "task.") {
			wantTopic = TopicTaskEvents
		}
		if Topic(typ) != wantTopic {
			t.Errorf("Topic(%s) = %s, want %s", typ, Topic(typ), wantTopic)
		}
	}
}
