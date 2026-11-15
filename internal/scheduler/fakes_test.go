package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daishakpatel/flowforge/internal/apperrors"
	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/events"
	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
	"github.com/daishakpatel/flowforge/internal/workflow/statemachine"
)

// memStore is an in-memory stand-in for the repositories the DecisionLoop uses, so its decision
// logic runs in unit tests (the coverage gate runs without the integration tag). It follows the
// same rules as the pg repositories: transitions check the state machine and the current status
// (and version for runs), INVALID_STATE / CONFLICT otherwise. The integration tests prove the SQL.
type memStore struct {
	mu       sync.Mutex
	epoch    int64
	runs     map[string]*persistence.RunView
	tasks    []*persistence.TaskView // creation order
	defs     map[string]persistence.DefinitionView
	events   []string // "<type> <task key or run id>"
	notified []string
	// rejectTask makes the next transition of that task id fail with INVALID_STATE, as if a
	// concurrent decision had moved it first.
	rejectTask string
	seq        int
}

func newMemStore() *memStore {
	return &memStore{runs: map[string]*persistence.RunView{}, defs: map[string]persistence.DefinitionView{}}
}

// addRun stores a definition and a CREATED run of it with PENDING tasks; it returns the run id.
func (s *memStore) addRun(raw string) string {
	def, err := model.Decode([]byte(raw))
	if err != nil {
		panic(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	defID, runID := fmt.Sprintf("wf_%03d", s.seq), fmt.Sprintf("run_%03d", s.seq)
	s.defs[defID] = persistence.DefinitionView{ID: defID, Name: def.Name, Version: 1, Definition: []byte(raw)}
	s.runs[runID] = &persistence.RunView{ID: runID, DefinitionID: defID, Status: model.RunCreated}
	t0 := time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC)
	for i, td := range def.Tasks { // test definitions list dependencies first
		s.tasks = append(s.tasks, &persistence.TaskView{
			ID: fmt.Sprintf("%s_%s", runID, td.ID), RunID: runID, Key: td.ID, Type: td.Type, Queue: td.Queue,
			Status: model.TaskPending, DependsOn: td.DependsOn, RunIf: td.RunIf, RunIfExpr: td.RunIfExpr, Config: taskConfig(td.Config),
			AvailableAt: t0, CreatedAt: t0.Add(time.Duration(i)),
		})
	}
	return runID
}

func (s *memStore) task(id string) *persistence.TaskView {
	for _, t := range s.tasks {
		if t.ID == id {
			return t
		}
	}
	return nil
}

// status returns a task's status by run and key.
func (s *memStore) status(runID, key string) model.TaskStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.task(runID + "_" + key).Status
}

func (s *memStore) runStatus(id string) model.RunStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.runs[id].Status
}

// execute plays the worker: every QUEUED task goes RUNNING and then SUCCEEDED, or FAILED for
// the keys in fail.
func (s *memStore) execute(fail ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, t := range s.tasks {
		if t.Status == model.TaskQueued {
			t.Status = model.TaskSucceeded
			if slices.Contains(fail, t.Key) {
				t.Status = model.TaskFailed
			}
		}
	}
}

func (s *memStore) count(event string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, e := range s.events {
		if len(e) >= len(event) && e[:len(event)] == event {
			n++
		}
	}
	return n
}

// loop returns a DecisionLoop wired to s.
func (s *memStore) loop(c clock.Clock) *DecisionLoop {
	return &DecisionLoop{
		Tx: memTx{}, Cluster: memCluster{s}, Sched: memSched{s}, Notify: memSched{s},
		Runs: memRuns{s: s}, Tasks: memTasks{s: s}, Defs: memDefs{s: s},
		Inputs: IdentityResolver{}, Clock: c, Tick: time.Second,
	}
}

// memTx runs fn with a nil transaction; the fakes ignore it.
type memTx struct{}

func (memTx) WithTx(_ context.Context, _ pgx.TxOptions, fn func(pgx.Tx) error) error { return fn(nil) }

type memCluster struct{ s *memStore }

func (c memCluster) GetEpoch(context.Context, persistence.Querier) (int64, string, error) {
	return c.s.epoch, "", nil
}

func (c memCluster) SetEpochIfGreater(_ context.Context, _ persistence.Querier, epoch int64, _ string) (bool, error) {
	c.s.mu.Lock()
	defer c.s.mu.Unlock()
	if epoch <= c.s.epoch {
		return false, nil
	}
	c.s.epoch = epoch
	return true, nil
}

func (c memCluster) AssertEpoch(_ context.Context, _ pgx.Tx, epoch int64) error {
	c.s.mu.Lock()
	defer c.s.mu.Unlock()
	if epoch != c.s.epoch {
		return &apperrors.Error{Code: apperrors.CodeNotLeader, Message: "stale epoch", Cause: persistence.ErrStaleEpoch}
	}
	return nil
}

type memSched struct{ s *memStore }

func (m memSched) ActiveRunIDs(_ context.Context, _ persistence.Querier, after string, limit int) ([]string, error) {
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	var ids []string
	for id, r := range m.s.runs {
		if (r.Status == model.RunCreated || r.Status == model.RunRunning) && id > after {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids[:min(limit, len(ids))], nil
}

func (m memSched) LockRun(_ context.Context, _ pgx.Tx, id string) (persistence.RunView, error) {
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	r, ok := m.s.runs[id]
	if !ok {
		return persistence.RunView{}, apperrors.NotFound("run")
	}
	return *r, nil
}

func (m memSched) HeadTasks(_ context.Context, _ persistence.Querier, runID string, limit int) ([]persistence.TaskView, error) {
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	superseded := map[string]bool{}
	for _, t := range m.s.tasks {
		superseded[t.SupersedesTaskID] = true
	}
	var out []persistence.TaskView
	for _, t := range m.s.tasks {
		if t.RunID == runID && !superseded[t.ID] && len(out) < limit {
			out = append(out, *t)
		}
	}
	return out, nil
}

func (m memSched) countByQueue(statuses ...model.TaskStatus) map[string]int {
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	out := map[string]int{}
	for _, t := range m.s.tasks {
		if slices.Contains(statuses, t.Status) {
			out[t.Queue]++
		}
	}
	return out
}

func (m memSched) ActiveByQueue(context.Context, persistence.Querier, []string) (map[string]int, error) {
	return m.countByQueue(model.TaskQueued, model.TaskRunning), nil
}

func (m memSched) QueuedByQueue(context.Context, persistence.Querier) (map[string]int, error) {
	return m.countByQueue(model.TaskQueued), nil
}

func (m memSched) Notify(_ context.Context, _ persistence.Querier, channel string) error {
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	m.s.notified = append(m.s.notified, channel)
	return nil
}

// memRuns implements RunRepo.Transition; the embedded nil interface panics on anything else.
type memRuns struct {
	persistence.RunRepo
	s *memStore
}

func (m memRuns) Transition(_ context.Context, _ pgx.Tx, p persistence.RunTransitionParams) error {
	if err := statemachine.RunTransition(p.From, p.To); err != nil {
		return err
	}
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	r := m.s.runs[p.RunID]
	switch {
	case r.Status != p.From:
		return apperrors.InvalidState("run moved")
	case r.Version != p.Version:
		return apperrors.Conflict("run changed")
	}
	r.Status, r.Version, r.LeaderEpoch = p.To, r.Version+1, p.LeaderEpoch
	if p.DeadlineAt != nil {
		r.DeadlineAt = p.DeadlineAt
	}
	if p.To == model.RunRunning && r.StartedAt == nil {
		started := time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC)
		r.StartedAt = &started
	}
	ev := statemachine.RunTransitionEvent(p.From, p.To)
	if p.Event != nil {
		ev = p.Event.Type
	}
	m.s.events = append(m.s.events, fmt.Sprintf("%s %s", ev, r.ID))
	return nil
}

// memTasks implements TaskRepo.Transition; the embedded nil interface panics on anything else.
type memTasks struct {
	persistence.TaskRepo
	s *memStore
}

func (m memTasks) Transition(_ context.Context, _ pgx.Tx, p persistence.TaskTransitionParams) error {
	if err := statemachine.TaskTransition(p.From, p.To); err != nil {
		return err
	}
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	t := m.s.task(p.TaskID)
	if t.Status != p.From || m.s.rejectTask == p.TaskID {
		m.s.rejectTask = ""
		return apperrors.InvalidState("task moved")
	}
	var mut persistence.TaskMutation
	if p.Mutate != nil {
		p.Mutate(&mut)
	}
	if mut.Input != nil {
		t.Input = mut.Input
	}
	t.Status = p.To
	ev := events.NewTaskEvent(statemachine.TaskTransitionEvent(p.From, p.To), events.TaskPayload{TaskKey: t.Key})
	if p.Event != nil {
		ev = *p.Event
	}
	m.s.events = append(m.s.events, fmt.Sprintf("%s %s", ev.Type, t.Key))
	return nil
}

// memDefs implements DefinitionRepo.Get.
type memDefs struct {
	persistence.DefinitionRepo
	s *memStore
}

func (m memDefs) Get(_ context.Context, _ persistence.Querier, id string) (persistence.DefinitionView, error) {
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	d, ok := m.s.defs[id]
	if !ok {
		return persistence.DefinitionView{}, apperrors.NotFound("definition")
	}
	return d, nil
}

// memRunControl implements RunControlRepo.OverdueRuns; the rest panics.
type memRunControl struct {
	persistence.RunControlRepo
	s *memStore
}

func (m memRunControl) OverdueRuns(_ context.Context, _ persistence.Querier, now time.Time, _ int) ([]string, error) {
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	var ids []string
	for id, r := range m.s.runs {
		if (r.Status == model.RunRunning || r.Status == model.RunPaused) && r.DeadlineAt != nil && !r.DeadlineAt.After(now) {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids, nil
}

// memRecovery has no overdue attempts.
type memRecovery struct{ persistence.RecoveryRepo }

func (memRecovery) OverdueAttempts(context.Context, persistence.Querier, time.Time, int) ([]persistence.AttemptView, error) {
	return nil, nil
}

// taskConfig is the stored config of a definition task: {"n":1} when it has none.
func taskConfig(c json.RawMessage) json.RawMessage {
	if c == nil {
		return json.RawMessage(`{"n":1}`)
	}
	return c
}
