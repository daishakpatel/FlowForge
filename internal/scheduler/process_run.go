package scheduler

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/jackc/pgx/v5"

	"github.com/daishakpatel/flowforge/internal/apperrors"
	"github.com/daishakpatel/flowforge/internal/events"
	"github.com/daishakpatel/flowforge/internal/logging"
	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
	"github.com/daishakpatel/flowforge/internal/workflow/statemachine"
)

const (
	// maxTasksPerRun bounds the head tasks loaded per run: the definition limit (00E §3.5).
	maxTasksPerRun = 500
	// maxPromotionsPerRun caps READY → QUEUED per run per tick, so a giant run cannot starve
	// the others (task 03.02 §5).
	maxPromotionsPerRun = 100
)

// decision is the state of one processRun: the locked run and its head tasks, updated in memory
// as transitions succeed.
type decision struct {
	l     *DecisionLoop
	tx    pgx.Tx
	epoch int64
	run   persistence.RunView
	def   *model.Definition
	heads []*persistence.TaskView
	byKey map[string]*persistence.TaskView
	res   result
}

// processRun makes every decision due for one run, in tx (task 03.02 §4):
//
//  0. assert the epoch first (ADR-003), then lock the run so decisions on it are serialized;
//  1. CREATED → RUNNING;
//  2. each PENDING head task → READY or SKIPPED (statemachine.EvaluateReadiness);
//  3. READY → QUEUED within the run, queue and per-tick limits, with resolved input;
//  4. once every head task is terminal, RUNNING → statemachine.DeriveRunStatus;
//  5. NOTIFY ff_task_queued (delivered at commit) when anything was queued.
//
// Idempotent by construction: every change is a conditional Transition, and one that finds the
// row already moved (INVALID_STATE, or CONFLICT for a run) means a previous or concurrent
// decision did it, so processRun carries on. Rerunning it after a crash redoes only what is
// still undone.
func (l *DecisionLoop) processRun(ctx context.Context, tx pgx.Tx, runID string, epoch int64) (result, error) {
	if err := l.Cluster.AssertEpoch(ctx, tx, epoch); err != nil {
		return result{}, err
	}
	run, err := l.Sched.LockRun(ctx, tx, runID)
	if err != nil {
		return result{}, err
	}
	if run.Status == model.RunPaused {
		return result{}, nil // seam for 03.05: a paused run gets no promotions and keeps its status
	}
	if run.Status != model.RunCreated && run.Status != model.RunRunning {
		return result{}, nil // finished, or awaiting attention, since it was listed
	}
	def, err := l.definition(ctx, tx, run.DefinitionID)
	if err != nil {
		return result{}, err
	}
	rows, err := l.Sched.HeadTasks(ctx, tx, run.ID, maxTasksPerRun)
	if err != nil {
		return result{}, err
	}
	d := &decision{l: l, tx: tx, epoch: epoch, run: run, def: def, byKey: make(map[string]*persistence.TaskView, len(rows))}
	d.res.tasks = map[model.TaskStatus]int{}
	for i := range rows {
		d.heads = append(d.heads, &rows[i])
		d.byKey[rows[i].Key] = &rows[i]
	}
	for _, step := range []func(context.Context) (bool, error){d.start, d.evaluate, d.promote, d.finish} {
		if ok, err := step(ctx); err != nil || !ok {
			return d.res, err
		}
	}
	if d.res.queued > 0 {
		if err := l.Notify.Notify(ctx, tx, persistence.TaskQueuedChannel); err != nil {
			return d.res, err
		}
	}
	return d.res, nil
}

// start moves a CREATED run to RUNNING. It returns false when another decision got there first.
func (d *decision) start(ctx context.Context) (bool, error) {
	if d.run.Status != model.RunCreated {
		return true, nil
	}
	moved, err := d.moveRun(ctx, model.RunRunning)
	d.res.started = moved
	return moved, err
}

// evaluate decides every PENDING head task. Heads come in creation order, which is topological,
// so a task skipped here is already terminal when its dependents are evaluated in the same pass.
func (d *decision) evaluate(ctx context.Context) (bool, error) {
	for _, t := range d.heads {
		if t.Status != model.TaskPending {
			continue
		}
		deps := d.deps(t)
		states := make([]statemachine.TaskView, len(deps))
		for i, dep := range deps {
			states[i] = dep.State()
		}
		decided, err := statemachine.EvaluateReadiness(t.State(), states, d.l.Eval)
		if err != nil {
			// run_if=expr without an evaluator (until 03.06) or a dependency missing from the
			// heads: leave the task PENDING and say why, every tick, until it is fixed.
			logging.FromContext(ctx).WarnContext(ctx, "task readiness undecidable; left PENDING",
				"run_id", d.run.ID, "task_id", t.ID, logging.Err(err))
			continue
		}
		to := map[statemachine.ReadinessDecision]model.TaskStatus{statemachine.Ready: model.TaskReady, statemachine.Skip: model.TaskSkipped}[decided]
		if to == "" {
			continue
		}
		if _, err := d.moveTask(ctx, t, to, nil, nil); err != nil {
			return false, err
		}
	}
	return true, nil
}

// promote queues READY tasks in promotionOrder while the budget allows. A task of a full queue
// is passed over, so tasks of other queues can still go.
func (d *decision) promote(ctx context.Context) (bool, error) {
	queueActive := map[string]int{}
	if len(d.l.QueueLimits) > 0 {
		var err error
		if queueActive, err = d.l.Sched.ActiveByQueue(ctx, d.tx, slices.Sorted(maps.Keys(d.l.QueueLimits))); err != nil {
			return false, err
		}
	}
	b := newBudget(d.def.Concurrency.MaxParallelTasks, activeCount(d.heads), d.l.QueueLimits, queueActive, maxPromotionsPerRun)
	for _, t := range promotionOrder(d.heads) {
		if b.exhausted() {
			break
		}
		if !b.fits(t.Queue) {
			continue
		}
		queued, err := d.queue(ctx, t)
		if err != nil {
			return false, err
		}
		if queued {
			b.use(t.Queue)
			d.res.queued++
		}
	}
	return true, nil
}

// queue resolves t's input and moves it READY → QUEUED, or READY → FAILED with
// ErrorCodeInvalidInput when resolution fails. It reports whether t was queued.
func (d *decision) queue(ctx context.Context, t *persistence.TaskView) (bool, error) {
	deps := d.deps(t)
	views := make([]persistence.TaskView, len(deps))
	for i, dep := range deps {
		views[i] = *dep
	}
	input, rerr := d.l.Inputs.Resolve(ctx, d.run, *t, views)
	if rerr != nil {
		logging.FromContext(ctx).WarnContext(ctx, "task input resolution failed; task FAILED",
			"run_id", d.run.ID, "task_id", t.ID, "error_code", ErrorCodeInvalidInput, logging.Err(rerr))
		ev := events.NewTaskEvent(statemachine.EventTaskFailed, events.TaskPayload{
			RunID: d.run.ID, TaskID: t.ID, TaskKey: t.Key, Status: model.TaskFailed, PreviousStatus: model.TaskReady,
			AttemptCount: t.AttemptCount, ErrorCode: ErrorCodeInvalidInput,
		})
		_, err := d.moveTask(ctx, t, model.TaskFailed, nil, &ev)
		return false, err
	}
	return d.moveTask(ctx, t, model.TaskQueued, func(m *persistence.TaskMutation) { m.Input = input }, nil)
}

// finish moves the run to its derived status once every head task is terminal.
func (d *decision) finish(ctx context.Context) (bool, error) {
	states := make([]statemachine.TaskView, len(d.heads))
	for i, t := range d.heads {
		states[i] = t.State()
	}
	status := statemachine.DeriveRunStatus(states, d.def.OnFailure)
	if status == model.RunRunning {
		return true, nil
	}
	moved, err := d.moveRun(ctx, status)
	if moved {
		d.res.completed = status
		if d.run.StartedAt != nil {
			d.res.duration = d.l.Clock.Now().Sub(*d.run.StartedAt)
		}
	}
	return true, err
}

// deps returns t's direct dependencies among the heads, skipping keys that are not there
// (EvaluateReadiness then reports the gap).
func (d *decision) deps(t *persistence.TaskView) []*persistence.TaskView {
	out := make([]*persistence.TaskView, 0, len(t.DependsOn))
	for _, key := range t.DependsOn {
		if dep, ok := d.byKey[key]; ok {
			out = append(out, dep)
		}
	}
	return out
}

// moveTask runs one task transition from t's current status. INVALID_STATE (the row moved
// under us) is benign and reported as false.
func (d *decision) moveTask(ctx context.Context, t *persistence.TaskView, to model.TaskStatus, mutate func(*persistence.TaskMutation), ev *events.Event) (bool, error) {
	err := d.l.Tasks.Transition(ctx, d.tx, persistence.TaskTransitionParams{TaskID: t.ID, From: t.Status, To: to, Mutate: mutate, Event: ev})
	if apperrors.CodeOf(err) == apperrors.CodeInvalidState {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("task %s %s → %s: %w", t.ID, t.Status, to, err)
	}
	t.Status = to
	d.res.tasks[to]++
	return true, nil
}

// moveRun runs one run transition from the locked status and version, stamping the epoch.
// INVALID_STATE and CONFLICT are benign and reported as false.
func (d *decision) moveRun(ctx context.Context, to model.RunStatus) (bool, error) {
	err := d.l.Runs.Transition(ctx, d.tx, persistence.RunTransitionParams{
		RunID: d.run.ID, From: d.run.Status, To: to, Version: d.run.Version, LeaderEpoch: &d.epoch,
	})
	switch apperrors.CodeOf(err) {
	case apperrors.CodeInvalidState, apperrors.CodeConflict:
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("run %s %s → %s: %w", d.run.ID, d.run.Status, to, err)
	}
	d.run.Status, d.run.Version = to, d.run.Version+1
	return true, nil
}
