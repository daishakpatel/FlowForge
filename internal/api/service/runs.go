package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daishakpatel/flowforge/internal/apperrors"
	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
)

// Run start limits (FR-SEC-05, task 02.04).
const (
	MaxRunInputBytes      = 256 << 10
	MaxIdempotencyKeyRune = 128
)

// RunService starts runs and reads runs, tasks, attempts and events.
type RunService struct {
	DB          persistence.Querier
	Tx          persistence.TxManager
	DefRepo     persistence.DefinitionRepo
	RunRepo     persistence.RunRepo
	TaskRepo    persistence.TaskRepo
	AttemptRepo persistence.AttemptRepo
	EventRepo   persistence.EventRepo
	// Run control (task 03.05).
	RunControl  persistence.RunControlRepo
	Notify      persistence.Notifier // wakes the scheduler after cancel and resume
	Clock       clock.Clock
	CancelGrace time.Duration // FF_CANCEL_GRACE: how long workers get to stop RUNNING attempts
	// Reruns creates rerun rows (task 03.06); pg.RunRepo implements it.
	Reruns persistence.RerunRepo
}

// StartRunParams is the input of RunService.Start. Empty strings are "not set".
type StartRunParams struct {
	DefinitionID   string
	Input          json.RawMessage // a JSON object; nil means {}
	IdempotencyKey string
	TriggerID      string
	ParentRunID    string
	Actor          string // created_by
}

// Start creates a run of a stored definition with all its tasks PENDING (FR-WF-03). It is the
// one way to start a run, shared by the REST API, cron triggers and approved actions.
//
// With an idempotency key, a repeated request (same definition, equal input) returns the
// original run with replayed = true, and a request that reuses the key for anything else is
// CONFLICT. Concurrent identical requests race on the unique index; the loser re-reads and
// replays, so exactly one run is created.
func (s *RunService) Start(ctx context.Context, p StartRunParams) (run persistence.RunView, replayed bool, err error) {
	if p.Input, err = validateRunInput(p.Input); err != nil {
		return persistence.RunView{}, false, err
	}
	if len([]rune(p.IdempotencyKey)) > MaxIdempotencyKeyRune {
		return persistence.RunView{}, false, apperrors.InvalidArgument(fmt.Sprintf("Idempotency-Key is longer than %d characters", MaxIdempotencyKeyRune))
	}
	if p.IdempotencyKey != "" {
		existing, err := s.RunRepo.GetByIdempotencyKey(ctx, s.DB, p.IdempotencyKey)
		if err == nil {
			return replay(existing, p)
		}
		if apperrors.CodeOf(err) != apperrors.CodeNotFound {
			return persistence.RunView{}, false, err
		}
	}

	stored, err := s.DefRepo.Get(ctx, s.DB, p.DefinitionID)
	if err != nil {
		return persistence.RunView{}, false, err
	}
	def, err := model.Decode(stored.Definition)
	if err != nil {
		return persistence.RunView{}, false, apperrors.Internal(fmt.Errorf("decode stored definition %s: %w", stored.ID, err))
	}
	err = s.Tx.WithTx(ctx, pgx.TxOptions{}, func(tx pgx.Tx) error {
		run, err = s.RunRepo.CreateWithTasks(ctx, tx, persistence.NewRun{
			DefinitionID: stored.ID, Input: p.Input, IdempotencyKey: p.IdempotencyKey,
			TriggerID: p.TriggerID, ParentRunID: p.ParentRunID, CreatedBy: p.Actor,
		}, def)
		return err
	})
	if apperrors.CodeOf(err) == apperrors.CodeConflict && p.IdempotencyKey != "" {
		// A concurrent request with the same key won the unique index: answer as it would.
		existing, gerr := s.RunRepo.GetByIdempotencyKey(ctx, s.DB, p.IdempotencyKey)
		if gerr != nil {
			return persistence.RunView{}, false, gerr
		}
		return replay(existing, p)
	}
	return run, false, err
}

// replay returns existing when p asks for the same run, else CONFLICT.
func replay(existing persistence.RunView, p StartRunParams) (persistence.RunView, bool, error) {
	if existing.DefinitionID != p.DefinitionID || !jsonEqual(existing.Input, p.Input) {
		return persistence.RunView{}, false, apperrors.Conflict("Idempotency-Key was already used for a different run request")
	}
	return existing, true, nil
}

// validateRunInput checks input is a JSON object of at most MaxRunInputBytes; nil becomes {}.
func validateRunInput(input json.RawMessage) (json.RawMessage, error) {
	if len(input) == 0 {
		return json.RawMessage(`{}`), nil
	}
	if len(input) > MaxRunInputBytes {
		return nil, apperrors.PayloadTooLarge(fmt.Sprintf("run input is %d bytes; the limit is %d", len(input), MaxRunInputBytes))
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(input, &obj); err != nil || obj == nil {
		return nil, apperrors.InvalidArgument("run input must be a JSON object")
	}
	return input, nil
}

// jsonEqual compares two JSON documents by value, so key order and whitespace (which jsonb
// normalizes) do not matter.
func jsonEqual(a, b []byte) bool {
	var va, vb any
	da, db := json.NewDecoder(bytes.NewReader(a)), json.NewDecoder(bytes.NewReader(b))
	da.UseNumber()
	db.UseNumber()
	if da.Decode(&va) != nil || db.Decode(&vb) != nil {
		return false
	}
	return reflect.DeepEqual(va, vb)
}

// Get returns a run, or NOT_FOUND.
func (s *RunService) Get(ctx context.Context, id string) (persistence.RunView, error) {
	return s.RunRepo.Get(ctx, s.DB, id)
}

// List returns one page of runs matching f, newest first, and the next cursor.
func (s *RunService) List(ctx context.Context, f persistence.RunFilter, page persistence.Page) ([]persistence.RunView, string, error) {
	return s.RunRepo.List(ctx, s.DB, f, page)
}

// Tasks returns every task row of a run in insertion order, or NOT_FOUND for an unknown run.
func (s *RunService) Tasks(ctx context.Context, runID string) ([]persistence.TaskView, error) {
	if _, err := s.RunRepo.Get(ctx, s.DB, runID); err != nil {
		return nil, err
	}
	return s.TaskRepo.ListByRun(ctx, s.DB, runID)
}

// Events returns up to limit events of a run after workflow_events.id afterID, oldest first,
// or NOT_FOUND for an unknown run.
func (s *RunService) Events(ctx context.Context, runID string, afterID int64, limit int) ([]persistence.EventView, error) {
	if _, err := s.RunRepo.Get(ctx, s.DB, runID); err != nil {
		return nil, err
	}
	return s.EventRepo.ListByRun(ctx, s.DB, runID, afterID, limit)
}

// Task returns a task, or NOT_FOUND.
func (s *RunService) Task(ctx context.Context, id string) (persistence.TaskView, error) {
	return s.TaskRepo.Get(ctx, s.DB, id)
}

// Attempts returns a task's attempts by number, or NOT_FOUND for an unknown task.
func (s *RunService) Attempts(ctx context.Context, taskID string) ([]persistence.AttemptView, error) {
	if _, err := s.TaskRepo.Get(ctx, s.DB, taskID); err != nil {
		return nil, err
	}
	return s.AttemptRepo.ListByTask(ctx, s.DB, taskID)
}

// Cancel requests a run's cancellation (FR-WF-10, task 03.05) and returns the run. The scheduler
// cancels the tasks that have not started at once; RUNNING ones are cancelled by their workers
// (told in heartbeat answers) or, after CancelGrace (at once with force), force-cancelled, and
// then the run becomes CANCELLED. Cancelling a CANCELLED run, or one already cancel-requested,
// returns it unchanged; a SUCCEEDED or FAILED run is INVALID_STATE. Shared by REST and approved
// actions (06.06).
func (s *RunService) Cancel(ctx context.Context, id string, force bool) (persistence.RunView, error) {
	var out persistence.RunView
	err := s.Tx.WithTx(ctx, pgx.TxOptions{}, func(tx pgx.Tx) error {
		cur, err := s.RunRepo.Get(ctx, tx, id)
		if err != nil {
			return err
		}
		if cur.Status == model.RunCancelled {
			out = cur
			return nil
		}
		forceAt := s.Clock.Now()
		if !force {
			forceAt = forceAt.Add(s.CancelGrace)
		}
		if out, err = s.RunControl.RequestCancel(ctx, tx, id, forceAt); err != nil {
			return err
		}
		return s.Notify.Notify(ctx, tx, persistence.SchedulerWakeChannel)
	})
	return out, err
}

// Pause moves a RUNNING run to PAUSED (task 03.05): the scheduler stops queueing its tasks and
// requeueing its retries, and workers stop claiming them; running attempts finish normally. The
// workflow deadline keeps running. Pausing a PAUSED run returns it unchanged; any other status,
// or a run being cancelled, is INVALID_STATE.
func (s *RunService) Pause(ctx context.Context, id string) (persistence.RunView, error) {
	return s.move(ctx, id, model.RunRunning, model.RunPaused, true)
}

// Resume moves a PAUSED run back to RUNNING and wakes the scheduler (task 03.05). Any other
// status, a RUNNING run included, is INVALID_STATE.
func (s *RunService) Resume(ctx context.Context, id string) (persistence.RunView, error) {
	return s.move(ctx, id, model.RunPaused, model.RunRunning, false)
}

// move runs one run transition from → to with the run's current version, retrying when the run
// changed under it (CONFLICT). With idempotent, a run already in to is returned unchanged.
func (s *RunService) move(ctx context.Context, id string, from, to model.RunStatus, idempotent bool) (persistence.RunView, error) {
	var out persistence.RunView
	for range 3 {
		err := s.Tx.WithTx(ctx, pgx.TxOptions{}, func(tx pgx.Tx) error {
			cur, err := s.RunRepo.Get(ctx, tx, id)
			if err != nil {
				return err
			}
			if cur.Status == to && idempotent {
				out = cur
				return nil
			}
			if cur.Status != from || cur.CancelRequested {
				return apperrors.InvalidState(fmt.Sprintf("run %s is %s%s; it cannot become %s", id, cur.Status, cancelNote(cur), to))
			}
			if err := s.RunRepo.Transition(ctx, tx, persistence.RunTransitionParams{RunID: id, From: from, To: to, Version: cur.Version}); err != nil {
				return err
			}
			if to == model.RunRunning {
				if err := s.Notify.Notify(ctx, tx, persistence.SchedulerWakeChannel); err != nil {
					return err
				}
			}
			out, err = s.RunRepo.Get(ctx, tx, id)
			return err
		})
		if apperrors.CodeOf(err) != apperrors.CodeConflict {
			return out, err
		}
	}
	return out, apperrors.Conflict(fmt.Sprintf("run %s kept changing; retry", id))
}

func cancelNote(r persistence.RunView) string {
	if r.CancelRequested {
		return " and being cancelled"
	}
	return ""
}
