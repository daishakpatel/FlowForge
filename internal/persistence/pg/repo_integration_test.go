//go:build integration

package pg

import (
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daishakpatel/flowforge/internal/apperrors"
	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
	"github.com/daishakpatel/flowforge/internal/workflow/statemachine"
)

func TestDefinitionRepo_ConflictLatestAndList(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()
	v1, _ := e.createDefinition(t, "etl", 1)
	v2, _ := e.createDefinition(t, "etl", 2)

	_, err := e.defs.Create(ctx, e.pool, persistence.NewDefinition{Name: "etl", Version: 1, Definition: []byte(`{}`), CreatedBy: "x"})
	if apperrors.CodeOf(err) != apperrors.CodeConflict {
		t.Errorf("duplicate (name, version) = %v, want CONFLICT", err)
	}
	if latest, err := e.defs.Latest(ctx, e.pool, "etl"); err != nil || latest.ID != v2.ID || latest.Version != 2 {
		t.Errorf("Latest = %+v, %v; want version 2", latest, err)
	}
	if got, err := e.defs.GetByNameVersion(ctx, e.pool, "etl", 1); err != nil || got.ID != v1.ID {
		t.Errorf("GetByNameVersion(1) = %+v, %v", got, err)
	}
	if got, err := e.defs.Get(ctx, e.pool, v1.ID); err != nil || string(got.Definition) == "" {
		t.Errorf("Get = %+v, %v", got, err)
	}
	for _, err := range []error{
		func() error { _, err := e.defs.Get(ctx, e.pool, "wf_missing"); return err }(),
		func() error { _, err := e.defs.Latest(ctx, e.pool, "nope"); return err }(),
	} {
		if apperrors.CodeOf(err) != apperrors.CodeNotFound {
			t.Errorf("missing definition = %v, want NOT_FOUND", err)
		}
	}

	first, next, err := e.defs.List(ctx, e.pool, persistence.Page{Limit: 1})
	if err != nil || len(first) != 1 || next == "" {
		t.Fatalf("page 1 = %d rows, next %q, %v", len(first), next, err)
	}
	second, next2, err := e.defs.List(ctx, e.pool, persistence.Page{Limit: 1, Cursor: next})
	if err != nil || len(second) != 1 || next2 != "" || second[0].ID == first[0].ID {
		t.Errorf("page 2 = %+v, next %q, %v; want the other definition and no next page", second, next2, err)
	}
}

func TestRunRepo_CreateWithTasks(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	run, tasks := e.createRun(t)

	if run.Status != model.RunCreated || run.DeadlineAt == nil || string(run.Input) != "{}" {
		t.Errorf("run = %+v; want CREATED with a deadline and {} input", run)
	}
	if len(tasks) != 3 {
		t.Fatalf("tasks = %d, want 3", len(tasks))
	}
	for key, task := range tasks {
		if task.Status != model.TaskPending || task.RetryPolicy.MaxAttempts < 1 || task.TimeoutSeconds < 1 {
			t.Errorf("task %s = %+v; want PENDING with the default retry policy and timeout", key, task)
		}
	}
	if c := tasks["c"]; !slices.Equal(c.DependsOn, []string{"a", "b"}) || c.RunIf != model.RunIfAlways {
		t.Errorf("task c = %+v", c)
	}
	if a := tasks["a"]; a.Queue != "q1" || string(a.Config) != `{"k": 1}` {
		t.Errorf("task a queue/config = %q %s", a.Queue, a.Config)
	}
	// Topological order: IDs (and so list order) put a before b before c.
	if tasks["a"].ID >= tasks["b"].ID || tasks["b"].ID >= tasks["c"].ID {
		t.Errorf("task ids not in topological order: %s %s %s", tasks["a"].ID, tasks["b"].ID, tasks["c"].ID)
	}
	evs, err := e.events.ListByRun(t.Context(), e.pool, run.ID, 0, 10)
	if err != nil || len(evs) != 1 || evs[0].Type != statemachine.EventRunCreated {
		t.Fatalf("events = %+v, %v; want exactly run.created", evs, err)
	}
	var p map[string]any
	if err := json.Unmarshal(evs[0].Payload, &p); err != nil || p["task_count"] != 3.0 {
		t.Errorf("run.created payload = %s", evs[0].Payload)
	}
	if n := e.count(t, `SELECT count(*) FROM outbox WHERE key = $1 AND topic = 'flowforge.workflow-events'`, run.ID); n != 1 {
		t.Errorf("outbox rows = %d, want 1", n)
	}
}

// TestRunRepo_CreateWithTasksIsAtomic injects a failure into the outbox insert, the last write,
// after the run and task rows: nothing may persist.
func TestRunRepo_CreateWithTasksIsAtomic(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	dv, def := e.createDefinition(t, "atomic", 1)
	err := e.tm.WithTx(t.Context(), pgx.TxOptions{}, func(tx pgx.Tx) error {
		_, err := e.runs.CreateWithTasks(t.Context(), &spyTx{Tx: tx, failOn: "INSERT INTO outbox"},
			persistence.NewRun{DefinitionID: dv.ID, CreatedBy: "test"}, def)
		return err
	})
	if !errors.Is(err, errInjected) {
		t.Fatalf("err = %v, want the injected failure", err)
	}
	for _, table := range []string{"workflow_runs", "workflow_tasks", "workflow_events", "outbox"} {
		if n := e.count(t, `SELECT count(*) FROM `+table); n != 0 {
			t.Errorf("%s has %d rows after a rolled-back create, want 0", table, n)
		}
	}
}

func TestRunRepo_DuplicateIdempotencyKeyIsConflict(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	dv, def := e.createDefinition(t, "idem", 1)
	create := func() error {
		return e.tm.WithTx(t.Context(), pgx.TxOptions{}, func(tx pgx.Tx) error {
			_, err := e.runs.CreateWithTasks(t.Context(), tx, persistence.NewRun{DefinitionID: dv.ID, IdempotencyKey: "k1", CreatedBy: "t"}, def)
			return err
		})
	}
	if err := create(); err != nil {
		t.Fatal(err)
	}
	if err := create(); apperrors.CodeOf(err) != apperrors.CodeConflict {
		t.Errorf("second create = %v, want CONFLICT", err)
	}
	if got, err := e.runs.GetByIdempotencyKey(t.Context(), e.pool, "k1"); err != nil || got.IdempotencyKey != "k1" || got.DefinitionID != dv.ID {
		t.Errorf("GetByIdempotencyKey(k1) = %+v, %v", got, err)
	}
	if _, err := e.runs.GetByIdempotencyKey(t.Context(), e.pool, "nope"); apperrors.CodeOf(err) != apperrors.CodeNotFound {
		t.Errorf("GetByIdempotencyKey(nope) = %v, want NOT_FOUND", err)
	}
}

// TestTaskRepo_ConcurrentTransitionHasOneWinner: 20 transactions race READY → QUEUED on one task.
func TestTaskRepo_ConcurrentTransitionHasOneWinner(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	run, tasks := e.createRun(t)
	id := tasks["a"].ID
	e.moveTask(t, id, model.TaskPending, model.TaskReady, nil)

	const n = 20
	errs := make([]error, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range n {
		wg.Go(func() {
			<-start
			errs[i] = e.tm.WithTx(t.Context(), pgx.TxOptions{}, func(tx pgx.Tx) error {
				return e.tasks.Transition(t.Context(), tx, persistence.TaskTransitionParams{TaskID: id, From: model.TaskReady, To: model.TaskQueued})
			})
		})
	}
	close(start)
	wg.Wait()

	won, lost := 0, 0
	for _, err := range errs {
		switch {
		case err == nil:
			won++
		case apperrors.CodeOf(err) == apperrors.CodeInvalidState:
			lost++
		default:
			t.Errorf("unexpected error: %v", err)
		}
	}
	if won != 1 || lost != n-1 {
		t.Errorf("winners = %d, INVALID_STATE = %d; want 1 and %d", won, lost, n-1)
	}
	if c := e.count(t, `SELECT count(*) FROM workflow_events WHERE run_id = $1 AND event_type = 'task.queued'`, run.ID); c != 1 {
		t.Errorf("task.queued events = %d, want 1", c)
	}
	if c := e.count(t, `SELECT count(*) FROM outbox WHERE key = $1 AND envelope->>'event_type' = 'task.queued'`, run.ID); c != 1 {
		t.Errorf("task.queued outbox rows = %d, want 1", c)
	}
}

// TestTransition_IllegalRejectedBeforeSQL: transitions the state machine forbids never reach the
// database.
func TestTransition_IllegalRejectedBeforeSQL(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	err := e.tm.WithTx(t.Context(), pgx.TxOptions{}, func(tx pgx.Tx) error {
		spy := &spyTx{Tx: tx}
		ctx := t.Context()
		for name, err := range map[string]error{
			"task":    e.tasks.Transition(ctx, spy, persistence.TaskTransitionParams{TaskID: "task_x", From: model.TaskSucceeded, To: model.TaskRunning}),
			"run":     e.runs.Transition(ctx, spy, persistence.RunTransitionParams{RunID: "run_x", From: model.RunSucceeded, To: model.RunRunning}),
			"attempt": e.attempts.Transition(ctx, spy, persistence.AttemptTransitionParams{AttemptID: "att_x", From: model.AttemptSucceeded, To: model.AttemptFailed}),
		} {
			if apperrors.CodeOf(err) != apperrors.CodeInvalidState || !errors.Is(err, statemachine.ErrInvalidTransition) {
				t.Errorf("%s: err = %v, want INVALID_STATE wrapping ErrInvalidTransition", name, err)
			}
		}
		if spy.calls != 0 {
			t.Errorf("illegal transitions ran %d statements, want 0", spy.calls)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestTaskRepo_TransitionAndEventCommitTogether: when recording the event fails, the status
// change rolls back with it.
func TestTaskRepo_TransitionAndEventCommitTogether(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	run, tasks := e.createRun(t)
	id := tasks["a"].ID
	err := e.tm.WithTx(t.Context(), pgx.TxOptions{}, func(tx pgx.Tx) error {
		return e.tasks.Transition(t.Context(), &spyTx{Tx: tx, failOn: "INSERT INTO outbox"},
			persistence.TaskTransitionParams{TaskID: id, From: model.TaskPending, To: model.TaskReady})
	})
	if !errors.Is(err, errInjected) {
		t.Fatalf("err = %v, want the injected failure", err)
	}
	if got := e.tasksByKey(t, run.ID)["a"]; got.Status != model.TaskPending || got.Version != 0 {
		t.Errorf("task after failed transition = %s v%d, want PENDING v0", got.Status, got.Version)
	}
	if c := e.count(t, `SELECT count(*) FROM workflow_events WHERE event_type = 'task.ready'`); c != 0 {
		t.Errorf("task.ready events = %d, want 0", c)
	}
}

func TestTaskRepo_MutationsAndMisses(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	run, tasks := e.createRun(t)
	id := tasks["a"].ID
	e.moveTask(t, id, model.TaskPending, model.TaskReady, nil)
	e.moveTask(t, id, model.TaskReady, model.TaskQueued, func(m *persistence.TaskMutation) { m.Input = []byte(`{"x":1}`) })
	e.moveTask(t, id, model.TaskQueued, model.TaskRunning, func(m *persistence.TaskMutation) { m.IncrementAttempt = true })
	var before time.Time // available_at uses the database clock (ADR-021), so compare with it
	if err := e.pool.QueryRow(t.Context(), `SELECT now()`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	delay := time.Hour
	e.moveTask(t, id, model.TaskRunning, model.TaskRetryWait, func(m *persistence.TaskMutation) { m.RetryDelay = &delay })

	got := e.tasksByKey(t, run.ID)["a"]
	if got.Status != model.TaskRetryWait || got.AttemptCount != 1 || got.Version != 4 || string(got.Input) != `{"x": 1}` {
		t.Errorf("task = %s attempts=%d v%d input=%s", got.Status, got.AttemptCount, got.Version, got.Input)
	}
	if got.AvailableAt.Before(before.Add(59 * time.Minute)) {
		t.Errorf("available_at = %v, want about an hour after %v", got.AvailableAt, before)
	}
	evs, err := e.events.ListByRun(t.Context(), e.pool, run.ID, 0, 50)
	if err != nil {
		t.Fatal(err)
	}
	var types []statemachine.EventType
	for _, ev := range evs {
		types = append(types, ev.Type)
	}
	want := []statemachine.EventType{statemachine.EventRunCreated, statemachine.EventTaskReady, statemachine.EventTaskQueued,
		statemachine.EventTaskStarted, statemachine.EventTaskRetryScheduled}
	if !slices.Equal(types, want) {
		t.Errorf("event types = %v, want %v", types, want)
	}

	err = e.tm.WithTx(t.Context(), pgx.TxOptions{}, func(tx pgx.Tx) error {
		ctx := t.Context()
		if err := e.tasks.Transition(ctx, tx, persistence.TaskTransitionParams{TaskID: id, From: model.TaskReady, To: model.TaskQueued}); apperrors.CodeOf(err) != apperrors.CodeInvalidState {
			t.Errorf("stale from = %v, want INVALID_STATE", err)
		}
		if err := e.tasks.Transition(ctx, tx, persistence.TaskTransitionParams{TaskID: "task_missing", From: model.TaskReady, To: model.TaskQueued}); apperrors.CodeOf(err) != apperrors.CodeNotFound {
			t.Errorf("missing task = %v, want NOT_FOUND", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if list, err := e.tasks.ListByRunStatus(t.Context(), e.pool, run.ID, model.TaskPending); err != nil || len(list) != 2 {
		t.Errorf("PENDING tasks = %d, %v; want 2", len(list), err)
	}
}

func TestRunRepo_Transition(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	run, _ := e.createRun(t)
	epoch := int64(3)
	move := func(p persistence.RunTransitionParams) error {
		return e.tm.WithTx(t.Context(), pgx.TxOptions{}, func(tx pgx.Tx) error { return e.runs.Transition(t.Context(), tx, p) })
	}
	if err := move(persistence.RunTransitionParams{RunID: run.ID, From: model.RunCreated, To: model.RunRunning, Version: 0, LeaderEpoch: &epoch}); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		p    persistence.RunTransitionParams
		want apperrors.Code
	}{
		{"stale version", persistence.RunTransitionParams{RunID: run.ID, From: model.RunRunning, To: model.RunPaused, Version: 0}, apperrors.CodeConflict},
		{"wrong from", persistence.RunTransitionParams{RunID: run.ID, From: model.RunPaused, To: model.RunRunning, Version: 1}, apperrors.CodeInvalidState},
		{"missing", persistence.RunTransitionParams{RunID: "run_missing", From: model.RunRunning, To: model.RunPaused}, apperrors.CodeNotFound},
	}
	for _, c := range cases {
		if err := move(c.p); apperrors.CodeOf(err) != c.want {
			t.Errorf("%s: %v, want %s", c.name, err, c.want)
		}
	}
	if err := move(persistence.RunTransitionParams{RunID: run.ID, From: model.RunRunning, To: model.RunSucceeded, Version: 1}); err != nil {
		t.Fatal(err)
	}
	got, err := e.runs.Get(t.Context(), e.pool, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != model.RunSucceeded || got.Version != 2 || got.StartedAt == nil || got.CompletedAt == nil ||
		got.LeaderEpoch == nil || *got.LeaderEpoch != 3 {
		t.Errorf("run = %+v", got)
	}
	n := e.count(t, `SELECT count(*) FROM workflow_events WHERE run_id = $1 AND event_type IN ('run.started','run.succeeded')`, run.ID)
	if n != 2 {
		t.Errorf("run.started + run.succeeded events = %d, want 2", n)
	}
}

func TestAttemptRepo_FencedLifecycle(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	run, tasks := e.createRun(t)
	taskID := tasks["a"].ID
	ctx := t.Context()
	inTx := func(fn func(tx pgx.Tx) error) error { return e.tm.WithTx(ctx, pgx.TxOptions{}, fn) }

	var att persistence.AttemptView
	err := inTx(func(tx pgx.Tx) error {
		var err error
		att, err = e.attempts.Create(ctx, tx, persistence.NewAttempt{TaskID: taskID, RunID: run.ID, AttemptNumber: 1, WorkerID: "w1", TimeoutSeconds: 60})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if att.Status != model.AttemptRunning || att.FencingToken <= 0 || att.IdempotencyKey != taskID+":1" || !att.DeadlineAt.After(att.StartedAt) {
		t.Errorf("attempt = %+v", att)
	}
	dup := inTx(func(tx pgx.Tx) error {
		_, err := e.attempts.Create(ctx, tx, persistence.NewAttempt{TaskID: taskID, RunID: run.ID, AttemptNumber: 1, TimeoutSeconds: 60})
		return err
	})
	if apperrors.CodeOf(dup) != apperrors.CodeConflict {
		t.Errorf("duplicate attempt number = %v, want CONFLICT", dup)
	}

	finish := func(id string, token int64, to model.AttemptStatus) error {
		return inTx(func(tx pgx.Tx) error {
			retry := true
			return e.attempts.Transition(ctx, tx, persistence.AttemptTransitionParams{
				AttemptID: id, From: model.AttemptRunning, To: to, FencingToken: token,
				Mutate: func(m *persistence.AttemptMutation) {
					m.ErrorCode, m.ErrorMessage, m.Retryable = "BOOM", "secret-free text", &retry
				},
			})
		})
	}
	if err := finish(att.ID, att.FencingToken+1, model.AttemptFailed); apperrors.CodeOf(err) != apperrors.CodeStaleAttempt {
		t.Errorf("wrong token = %v, want STALE_ATTEMPT", err)
	}
	if err := finish(att.ID, att.FencingToken, model.AttemptFailed); err != nil {
		t.Fatal(err)
	}
	if err := finish(att.ID, att.FencingToken, model.AttemptSucceeded); apperrors.CodeOf(err) != apperrors.CodeStaleAttempt {
		t.Errorf("finish twice = %v, want STALE_ATTEMPT", err)
	}
	if err := finish("att_missing", 1, model.AttemptFailed); apperrors.CodeOf(err) != apperrors.CodeNotFound {
		t.Errorf("missing attempt = %v, want NOT_FOUND", err)
	}
	got, err := e.attempts.Get(ctx, e.pool, att.ID)
	if err != nil || got.Status != model.AttemptFailed || got.FinishedAt == nil || got.ErrorCode != "BOOM" || got.Retryable == nil || !*got.Retryable {
		t.Errorf("finished attempt = %+v, %v", got, err)
	}
	if list, err := e.attempts.ListByTask(ctx, e.pool, taskID); err != nil || len(list) != 1 {
		t.Errorf("ListByTask = %d, %v", len(list), err)
	}
	for typ, want := range map[string]int{"task.attempt.started": 1, "task.attempt.failed": 1} {
		if n := e.count(t, `SELECT count(*) FROM workflow_events WHERE run_id = $1 AND event_type = $2`, run.ID, typ); n != want {
			t.Errorf("%s events = %d, want %d", typ, n, want)
		}
	}
	if n := e.count(t, `SELECT count(*) FROM outbox WHERE envelope::text LIKE '%secret-free%'`); n != 0 {
		t.Error("an attempt's error message reached the outbox")
	}

	// RUNNING → CANCELLED has no 00E §7 event; the transition still applies.
	err = inTx(func(tx pgx.Tx) error {
		a2, err := e.attempts.Create(ctx, tx, persistence.NewAttempt{TaskID: taskID, RunID: run.ID, AttemptNumber: 2, TimeoutSeconds: 60})
		if err != nil {
			return err
		}
		if a2.FencingToken <= att.FencingToken {
			t.Errorf("fencing token %d not above %d", a2.FencingToken, att.FencingToken)
		}
		return e.attempts.Transition(ctx, tx, persistence.AttemptTransitionParams{AttemptID: a2.ID, From: model.AttemptRunning, To: model.AttemptCancelled, FencingToken: a2.FencingToken})
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestClusterRepo_EpochFence(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()
	assert := func(epoch int64) error {
		return e.tm.WithTx(ctx, pgx.TxOptions{}, func(tx pgx.Tx) error { return e.cluster.AssertEpoch(ctx, tx, epoch) })
	}
	if err := assert(0); err != nil {
		t.Errorf("AssertEpoch(0) on a fresh database = %v, want nil (unfenced mode)", err)
	}
	if ok, err := e.cluster.SetEpochIfGreater(ctx, e.pool, 5, "sched-a"); !ok || err != nil {
		t.Fatalf("SetEpochIfGreater(5) = %v, %v; want true", ok, err)
	}
	if err := assert(5); err != nil {
		t.Errorf("AssertEpoch(5) = %v, want nil", err)
	}
	for _, stale := range []int64{0, 4, 6} {
		err := assert(stale)
		if apperrors.CodeOf(err) != apperrors.CodeNotLeader || !errors.Is(err, persistence.ErrStaleEpoch) {
			t.Errorf("AssertEpoch(%d) = %v, want NOT_LEADER wrapping ErrStaleEpoch", stale, err)
		}
	}
	if ok, err := e.cluster.SetEpochIfGreater(ctx, e.pool, 3, "sched-b"); ok || err != nil {
		t.Errorf("SetEpochIfGreater(3) after 5 = %v, %v; want a no-op", ok, err)
	}
	if epoch, leader, err := e.cluster.GetEpoch(ctx, e.pool); epoch != 5 || leader != "sched-a" || err != nil {
		t.Errorf("GetEpoch = %d, %q, %v; want 5, sched-a", epoch, leader, err)
	}
}

// TestRunRepo_KeysetPaginationWithEqualTimestamps creates 250 runs in one transaction, so they
// share created_at (now() is the transaction start), and pages through them.
func TestRunRepo_KeysetPaginationWithEqualTimestamps(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	dv, def := e.createDefinition(t, "paged", 1)
	const total = 250
	err := e.tm.WithTx(t.Context(), pgx.TxOptions{}, func(tx pgx.Tx) error {
		for range total {
			if _, err := e.runs.CreateWithTasks(t.Context(), tx, persistence.NewRun{DefinitionID: dv.ID, CreatedBy: "t"}, def); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if n := e.count(t, `SELECT count(DISTINCT created_at) FROM workflow_runs`); n != 1 {
		t.Fatalf("distinct created_at = %d, want 1", n)
	}

	var ids []string
	cursor, pages := "", 0
	for {
		page, next, err := e.runs.List(t.Context(), e.pool, persistence.RunFilter{Workflow: "paged"}, persistence.Page{Limit: 40, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		pages++
		for _, r := range page {
			ids = append(ids, r.ID)
		}
		if next == "" {
			break
		}
		cursor = next
	}
	if pages != 7 || len(ids) != total {
		t.Errorf("pages = %d, rows = %d; want 7 and %d", pages, len(ids), total)
	}
	if !slices.IsSortedFunc(ids, func(a, b string) int { return strings.Compare(b, a) }) {
		t.Error("rows are not ordered by id DESC within the equal timestamp")
	}
	if len(slices.Compact(slices.Clone(ids))) != total {
		t.Error("a row appeared on two pages")
	}

	for name, f := range map[string]persistence.RunFilter{
		"status":   {Status: model.RunRunning},
		"workflow": {Workflow: "other"},
		"until":    {Until: time.Unix(0, 0)},
	} {
		if page, _, err := e.runs.List(t.Context(), e.pool, f, persistence.Page{}); err != nil || len(page) != 0 {
			t.Errorf("filter %s: %d rows, %v; want none", name, len(page), err)
		}
	}
	if _, _, err := e.runs.List(t.Context(), e.pool, persistence.RunFilter{}, persistence.Page{Cursor: "garbage!"}); apperrors.CodeOf(err) != apperrors.CodeInvalidArgument {
		t.Errorf("bad cursor = %v, want INVALID_ARGUMENT", err)
	}
}

// TestAPIKeyRepo_ConcurrentRevocationKeepsAnAdmin: two transactions revoke the last two active
// admin keys at once; the row locks serialize the "last admin" check, so exactly one succeeds
// and one admin remains.
func TestAPIKeyRepo_ConcurrentRevocationKeepsAnAdmin(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	keys := APIKeyRepo{}
	ids := []string{"aaaaaaaaaaaa", "bbbbbbbbbbbb"}
	for _, id := range append(ids, "cccccccccccc") {
		role := "admin"
		if id == "cccccccccccc" {
			role = "viewer"
		}
		if _, err := keys.Create(t.Context(), e.pool, persistence.NewAPIKey{ID: id, Name: id, Role: role, SecretHash: []byte{1}, Salt: []byte{2}, CreatedBy: "t"}); err != nil {
			t.Fatal(err)
		}
	}
	errs := make([]error, 2)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range 2 {
		wg.Go(func() {
			<-start
			errs[i] = e.tm.WithTx(t.Context(), pgx.TxOptions{}, func(tx pgx.Tx) error { return keys.Revoke(t.Context(), tx, ids[i]) })
		})
	}
	close(start)
	wg.Wait()
	ok, conflict := 0, 0
	for _, err := range errs {
		switch apperrors.CodeOf(err) {
		case "":
			ok++
		case apperrors.CodeConflict:
			conflict++
		default:
			t.Errorf("revoke: %v", err)
		}
	}
	if ok != 1 || conflict != 1 {
		t.Errorf("revocations: %d ok, %d CONFLICT; want 1 and 1", ok, conflict)
	}
	if has, err := keys.HasActiveAdmin(t.Context(), e.pool); err != nil || !has {
		t.Errorf("HasActiveAdmin = %v, %v; want an admin left", has, err)
	}
	// Non-admin keys revoke freely, and revoking twice is a no-op.
	for range 2 {
		if err := e.tm.WithTx(t.Context(), pgx.TxOptions{}, func(tx pgx.Tx) error { return keys.Revoke(t.Context(), tx, "cccccccccccc") }); err != nil {
			t.Errorf("revoke viewer key: %v", err)
		}
	}
	if err := e.tm.WithTx(t.Context(), pgx.TxOptions{}, func(tx pgx.Tx) error { return keys.Revoke(t.Context(), tx, "dddddddddddd") }); apperrors.CodeOf(err) != apperrors.CodeNotFound {
		t.Errorf("revoke unknown key = %v, want NOT_FOUND", err)
	}
}
