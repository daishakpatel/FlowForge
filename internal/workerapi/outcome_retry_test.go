package workerapi

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
)

var policy = model.RetryPolicy{
	MaxAttempts: 3, Strategy: model.StrategyExponential, InitialDelayMS: 1000, MaxDelayMS: 3000,
	Multiplier: 2, NonRetryableErrors: []string{"BAD_INPUT"},
}

// TestDecide covers every row of the 03.04 decision table.
func TestDecide(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		attempts  int
		kind      AttemptKind
		code      string
		retryable bool
		want      retryDecision
	}{
		"failed, retryable, attempts left":    {1, AttemptFailed, "UPSTREAM", true, retryDecision{to: model.TaskRetryWait}},
		"failed, not retryable":               {1, AttemptFailed, "UPSTREAM", false, retryDecision{model.TaskFailed, "UPSTREAM"}},
		"failed, code listed non-retryable":   {1, AttemptFailed, "BAD_INPUT", true, retryDecision{model.TaskFailed, "BAD_INPUT"}},
		"failed, retryable, none left":        {3, AttemptFailed, "UPSTREAM", true, retryDecision{model.TaskFailed, "UPSTREAM"}},
		"timed out, attempts left":            {2, AttemptTimedOut, "", false, retryDecision{to: model.TaskRetryWait}},
		"timed out, none left":                {3, AttemptTimedOut, "TIMEOUT", false, retryDecision{model.TaskTimedOut, "TIMEOUT"}},
		"abandoned, attempts left":            {2, AttemptAbandoned, "", false, retryDecision{to: model.TaskRetryWait}},
		"abandoned, none left: WORKER_LOST":   {3, AttemptAbandoned, "", false, retryDecision{model.TaskFailed, CodeWorkerLost}},
		"abandoned attempts count toward max": {3, AttemptAbandoned, "", true, retryDecision{model.TaskFailed, CodeWorkerLost}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if got := decide(policy, tc.attempts, tc.kind, tc.code, tc.retryable); got != tc.want {
				t.Errorf("decide = %+v, want %+v", got, tc.want)
			}
		})
	}
	if got := decide(model.RetryPolicy{}, 1, AttemptFailed, "X", true); got.to != model.TaskFailed {
		t.Errorf("max_attempts 0 must allow exactly one attempt, got %+v", got)
	}
}

// fakeTasks records the transitions it is asked for.
type fakeTasks struct {
	persistence.TaskRepo
	got []persistence.TaskTransitionParams
	mut []persistence.TaskMutation
}

func (f *fakeTasks) Transition(_ context.Context, _ pgx.Tx, p persistence.TaskTransitionParams) error {
	var m persistence.TaskMutation
	if p.Mutate != nil {
		p.Mutate(&m)
	}
	f.got, f.mut = append(f.got, p), append(f.mut, m)
	return nil
}

type fakeRuns struct {
	persistence.RunRepo
	cancel bool
}

func (f fakeRuns) Get(context.Context, persistence.Querier, string) (persistence.RunView, error) {
	return persistence.RunView{CancelRequested: f.cancel}, nil
}

func TestRetryOutcomeHandler(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	outcome := func(kind AttemptKind, attemptNumber int) AttemptOutcome {
		return AttemptOutcome{
			Kind: kind, Retryable: true, ErrorCode: "UPSTREAM",
			Attempt: persistence.AttemptView{AttemptNumber: attemptNumber},
			Task:    persistence.TaskView{ID: "task_1", RunID: "run_1", Key: "k", AttemptCount: attemptNumber, RetryPolicy: policy},
		}
	}
	t.Run("backoff 1s, 2s, then capped at 3s; available_at from the injected clock", func(t *testing.T) {
		t.Parallel()
		tasks := &fakeTasks{}
		h := NewRetryOutcomeHandler(tasks, fakeRuns{}, clock.NewFake(now), nil)
		p := policy
		p.MaxAttempts = 10
		for n, want := range []time.Duration{time.Second, 2 * time.Second, 3 * time.Second, 3 * time.Second} {
			o := outcome(AttemptFailed, n+1)
			o.Task.RetryPolicy = p
			if err := h.OnAttemptFinished(t.Context(), nil, o); err != nil {
				t.Fatal(err)
			}
			m := tasks.mut[n]
			if tasks.got[n].To != model.TaskRetryWait || *m.RetryDelay != want || !m.AvailableAt.Equal(now.Add(want)) {
				t.Errorf("attempt %d: %s delay %v at %v; want RETRY_WAIT %v at now+%v", n+1, tasks.got[n].To, *m.RetryDelay, m.AvailableAt, want, want)
			}
		}
	})
	t.Run("exhausted abandon fails with WORKER_LOST on the event", func(t *testing.T) {
		t.Parallel()
		tasks := &fakeTasks{}
		h := NewRetryOutcomeHandler(tasks, fakeRuns{}, clock.NewFake(now), nil)
		if err := h.OnAttemptFinished(t.Context(), nil, outcome(AttemptAbandoned, 3)); err != nil {
			t.Fatal(err)
		}
		p := tasks.got[0]
		if p.To != model.TaskFailed || p.Event == nil || p.Event.Type != "task.failed" {
			t.Fatalf("transition %+v", p)
		}
	})
	t.Run("cancel-requested run is not retried", func(t *testing.T) {
		t.Parallel()
		tasks := &fakeTasks{}
		h := NewRetryOutcomeHandler(tasks, fakeRuns{cancel: true}, clock.NewFake(now), nil)
		if err := h.OnAttemptFinished(t.Context(), nil, outcome(AttemptTimedOut, 1)); err != nil {
			t.Fatal(err)
		}
		if tasks.got[0].To != model.TaskCancelled {
			t.Errorf("task moved to %s, want CANCELLED", tasks.got[0].To)
		}
	})
	t.Run("success and cancellation", func(t *testing.T) {
		t.Parallel()
		tasks := &fakeTasks{}
		h := NewRetryOutcomeHandler(tasks, fakeRuns{}, clock.NewFake(now), nil)
		ok := outcome(AttemptSucceeded, 1)
		ok.Result = []byte(`{"r":1}`)
		if err := h.OnAttemptFinished(t.Context(), nil, ok); err != nil {
			t.Fatal(err)
		}
		if err := h.OnAttemptFinished(t.Context(), nil, outcome(AttemptCancelled, 1)); err != nil {
			t.Fatal(err)
		}
		if tasks.got[0].To != model.TaskSucceeded || string(tasks.mut[0].Result) != `{"r":1}` || tasks.got[1].To != model.TaskCancelled {
			t.Errorf("got %+v", tasks.got)
		}
	})
}
