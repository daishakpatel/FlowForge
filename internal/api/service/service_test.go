package service

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/daishakpatel/flowforge/internal/apperrors"
	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
)

// Fakes embed the repository interfaces, so a method a test does not set panics (nil).

type fakeDefs struct {
	persistence.DefinitionRepo
	latest  map[string]int
	created *persistence.NewDefinition
	stored  map[string]persistence.DefinitionView
}

func (f *fakeDefs) Create(_ context.Context, _ persistence.Querier, d persistence.NewDefinition) (persistence.DefinitionView, error) {
	f.created = &d
	return persistence.DefinitionView{ID: "wf_1", Name: d.Name, Version: d.Version, Definition: d.Definition}, nil
}

func (f *fakeDefs) Latest(_ context.Context, _ persistence.Querier, name string) (persistence.DefinitionView, error) {
	v, ok := f.latest[name]
	if !ok {
		return persistence.DefinitionView{}, apperrors.NotFound("workflow definition not found")
	}
	return persistence.DefinitionView{Name: name, Version: v}, nil
}

func (f *fakeDefs) Get(_ context.Context, _ persistence.Querier, id string) (persistence.DefinitionView, error) {
	v, ok := f.stored[id]
	if !ok {
		return persistence.DefinitionView{}, apperrors.NotFound("workflow definition not found")
	}
	return v, nil
}

type fakeRuns struct {
	persistence.RunRepo
	byKey     []persistence.RunView // successive GetByIdempotencyKey results; empty = NOT_FOUND
	createErr error
	created   []persistence.NewRun
}

func (f *fakeRuns) GetByIdempotencyKey(context.Context, persistence.Querier, string) (persistence.RunView, error) {
	if len(f.byKey) == 0 {
		return persistence.RunView{}, apperrors.NotFound("run not found")
	}
	v := f.byKey[0]
	f.byKey = f.byKey[1:]
	if v.ID == "" {
		return persistence.RunView{}, apperrors.NotFound("run not found")
	}
	return v, nil
}

func (f *fakeRuns) CreateWithTasks(_ context.Context, _ pgx.Tx, r persistence.NewRun, _ *model.Definition) (persistence.RunView, error) {
	f.created = append(f.created, r)
	if f.createErr != nil {
		return persistence.RunView{}, f.createErr
	}
	return persistence.RunView{ID: "run_new", DefinitionID: r.DefinitionID, Input: r.Input}, nil
}

func (f *fakeRuns) Get(_ context.Context, _ persistence.Querier, id string) (persistence.RunView, error) {
	return persistence.RunView{}, apperrors.NotFound("run " + id + " not found")
}

type fakeTasks struct{ persistence.TaskRepo }

func (fakeTasks) Get(context.Context, persistence.Querier, string) (persistence.TaskView, error) {
	return persistence.TaskView{}, apperrors.NotFound("task not found")
}

// directTx runs fn without a transaction; the fakes ignore tx.
type directTx struct{}

func (directTx) WithTx(_ context.Context, _ pgx.TxOptions, fn func(pgx.Tx) error) error {
	return fn(nil)
}

const validDef = `{"name":"etl","version":2,"tasks":[{"id":"a","type":"t"},{"id":"b","type":"t","depends_on":["a"]}]}`

func TestWorkflowService_CreateStoresNormalizedDefinition(t *testing.T) {
	t.Parallel()
	defs := &fakeDefs{}
	s := &WorkflowService{DefRepo: defs}
	if _, err := s.Create(t.Context(), []byte(validDef), "alice"); err != nil {
		t.Fatal(err)
	}
	if defs.created.Version != 2 || defs.created.CreatedBy != "alice" {
		t.Errorf("created = %+v", defs.created)
	}
	stored, err := model.Decode(defs.created.Definition)
	if err != nil {
		t.Fatalf("stored definition does not decode: %v", err)
	}
	if stored.Tasks[0].Retry.MaxAttempts < 1 || stored.Tasks[0].Queue == "" || stored.Tasks[0].TimeoutSeconds < 1 {
		t.Errorf("stored task lacks applied defaults: %+v", stored.Tasks[0])
	}
}

func TestWorkflowService_OmittedVersionIsNextVersion(t *testing.T) {
	t.Parallel()
	raw := []byte(`{"name":"etl","tasks":[{"id":"a","type":"t"}]}`)
	for latest, want := range map[int]int{0: 1, 3: 4} {
		defs := &fakeDefs{latest: map[string]int{}}
		if latest > 0 {
			defs.latest["etl"] = latest
		}
		if _, err := (&WorkflowService{DefRepo: defs}).Create(t.Context(), raw, "a"); err != nil {
			t.Fatal(err)
		}
		if defs.created.Version != want {
			t.Errorf("latest %d: version = %d, want %d", latest, defs.created.Version, want)
		}
	}
}

func TestWorkflowService_CreateRejectsInvalid(t *testing.T) {
	t.Parallel()
	s := &WorkflowService{DefRepo: &fakeDefs{}}
	cycle := `{"name":"c","version":1,"tasks":[{"id":"a","type":"t","depends_on":["b"]},{"id":"b","type":"t","depends_on":["a"]}]}`
	_, err := s.Create(t.Context(), []byte(cycle), "x")
	e := apperrors.FromError(err)
	if e.Code != apperrors.CodeInvalidWorkflow || len(e.Details) == 0 || e.Details[0].Code != "CYCLE" || e.Details[0].TaskID == "" {
		t.Errorf("cycle = %+v, want INVALID_WORKFLOW with a CYCLE detail naming a task", e)
	}
	if _, err := s.Create(t.Context(), []byte(`{"name":"x","bogus":1}`), "x"); apperrors.CodeOf(err) != apperrors.CodeInvalidArgument {
		t.Errorf("unknown field = %v, want INVALID_ARGUMENT", err)
	}
}

func newRunService(runs *fakeRuns) *RunService {
	defs := &fakeDefs{stored: map[string]persistence.DefinitionView{"wf_1": {ID: "wf_1", Definition: []byte(validDef)}}}
	return &RunService{Tx: directTx{}, DefRepo: defs, RunRepo: runs, TaskRepo: fakeTasks{}}
}

func TestRunService_StartCreatesRun(t *testing.T) {
	t.Parallel()
	runs := &fakeRuns{}
	run, replayed, err := newRunService(runs).Start(t.Context(), StartRunParams{DefinitionID: "wf_1", Actor: "dev"})
	if err != nil || replayed || run.ID != "run_new" {
		t.Fatalf("Start = %+v, %v, %v", run, replayed, err)
	}
	if got := runs.created[0]; string(got.Input) != "{}" || got.CreatedBy != "dev" {
		t.Errorf("created = %+v; want {} input and the actor", got)
	}
	if _, _, err := newRunService(&fakeRuns{}).Start(t.Context(), StartRunParams{DefinitionID: "wf_x"}); apperrors.CodeOf(err) != apperrors.CodeNotFound {
		t.Errorf("unknown definition = %v, want NOT_FOUND", err)
	}
}

func TestRunService_StartIdempotency(t *testing.T) {
	t.Parallel()
	existing := persistence.RunView{ID: "run_old", DefinitionID: "wf_1", Input: json.RawMessage(`{"b": 2, "a": [1, 2]}`)}
	cases := []struct {
		name  string
		def   string
		input string
		want  apperrors.Code
	}{
		{"same request replays (key order ignored)", "wf_1", `{"a":[1,2],"b":2}`, ""},
		{"different input", "wf_1", `{"a":[1,2],"b":3}`, apperrors.CodeConflict},
		{"different definition", "wf_2", `{"a":[1,2],"b":2}`, apperrors.CodeConflict},
	}
	for _, c := range cases {
		runs := &fakeRuns{byKey: []persistence.RunView{existing}}
		run, replayed, err := newRunService(runs).Start(t.Context(), StartRunParams{DefinitionID: c.def, Input: json.RawMessage(c.input), IdempotencyKey: "k"})
		if apperrors.CodeOf(err) != c.want {
			t.Errorf("%s: err = %v, want %q", c.name, err, c.want)
		}
		if c.want == "" && (!replayed || run.ID != "run_old") {
			t.Errorf("%s: run = %s replayed = %v; want the original run replayed", c.name, run.ID, replayed)
		}
		if len(runs.created) != 0 {
			t.Errorf("%s: created a run despite an existing key", c.name)
		}
	}
}

// TestRunService_StartRaceLoserReplays: the key is free at lookup time, but a concurrent request
// wins the unique index; the loser must re-read and replay instead of failing.
func TestRunService_StartRaceLoserReplays(t *testing.T) {
	t.Parallel()
	winner := persistence.RunView{ID: "run_winner", DefinitionID: "wf_1", Input: json.RawMessage(`{}`)}
	runs := &fakeRuns{byKey: []persistence.RunView{{}, winner}, createErr: apperrors.Conflict("duplicate key")}
	run, replayed, err := newRunService(runs).Start(t.Context(), StartRunParams{DefinitionID: "wf_1", IdempotencyKey: "k"})
	if err != nil || !replayed || run.ID != "run_winner" {
		t.Errorf("Start = %s, %v, %v; want the winner replayed", run.ID, replayed, err)
	}
}

func TestRunService_StartValidatesInput(t *testing.T) {
	t.Parallel()
	big := `{"x":"` + strings.Repeat("a", MaxRunInputBytes) + `"}`
	cases := map[string]struct {
		p    StartRunParams
		want apperrors.Code
	}{
		"array input":  {StartRunParams{DefinitionID: "wf_1", Input: json.RawMessage(`[1]`)}, apperrors.CodeInvalidArgument},
		"null input":   {StartRunParams{DefinitionID: "wf_1", Input: json.RawMessage(`null`)}, apperrors.CodeInvalidArgument},
		"oversize":     {StartRunParams{DefinitionID: "wf_1", Input: json.RawMessage(big)}, apperrors.CodePayloadTooLarge},
		"long key":     {StartRunParams{DefinitionID: "wf_1", IdempotencyKey: strings.Repeat("k", MaxIdempotencyKeyRune+1)}, apperrors.CodeInvalidArgument},
		"max-size key": {StartRunParams{DefinitionID: "wf_1", IdempotencyKey: strings.Repeat("é", MaxIdempotencyKeyRune)}, ""},
	}
	for name, c := range cases {
		if _, _, err := newRunService(&fakeRuns{}).Start(t.Context(), c.p); apperrors.CodeOf(err) != c.want {
			t.Errorf("%s: err = %v, want %q", name, err, c.want)
		}
	}
}

func TestRunService_ReadsOfUnknownParentAreNotFound(t *testing.T) {
	t.Parallel()
	s := newRunService(&fakeRuns{})
	ctx := t.Context()
	for name, err := range map[string]error{
		"tasks":    func() error { _, err := s.Tasks(ctx, "run_x"); return err }(),
		"events":   func() error { _, err := s.Events(ctx, "run_x", 0, 10); return err }(),
		"attempts": func() error { _, err := s.Attempts(ctx, "task_x"); return err }(),
	} {
		if apperrors.CodeOf(err) != apperrors.CodeNotFound {
			t.Errorf("%s: %v, want NOT_FOUND", name, err)
		}
	}
}
