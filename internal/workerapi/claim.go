package workerapi

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/daishakpatel/flowforge/internal/apperrors"
	workerv1 "github.com/daishakpatel/flowforge/internal/gen/proto/worker/v1"
	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/workflow/model"
)

// Long-poll bounds (00E §8).
const (
	defaultWait  = 10 * time.Second
	maxWait      = 30 * time.Second
	recheckEvery = time.Second // fallback when a NOTIFY is missed or LISTEN is down
)

// PollTask implements workerv1.WorkerServiceServer: it claims up to free_capacity tasks of the
// worker's registered types and queues, waiting up to wait_ms for one to become claimable. The
// wait holds no database connection: it sleeps on the Waker (LISTEN ff_task_queued) and re-checks
// every second, and each check is one short claim transaction. A cancelled call returns at once.
func (s *Service) PollTask(ctx context.Context, req *workerv1.PollTaskRequest) (*workerv1.PollTaskResponse, error) {
	w, err := s.registeredWorker(ctx, req.GetWorkerId())
	if err != nil {
		return nil, err
	}
	if req.GetWaitMs() < 0 || req.GetFreeCapacity() < 0 {
		return nil, apperrors.InvalidArgument("wait_ms and free_capacity must not be negative")
	}
	free := min(int(req.GetFreeCapacity()), w.Capacity)
	if free == 0 {
		return &workerv1.PollTaskResponse{}, nil
	}
	wait := min(time.Duration(req.GetWaitMs())*time.Millisecond, maxWait)
	if wait == 0 {
		wait = defaultWait
	}
	deadline := s.Clock.NewTimer(wait)
	defer deadline.Stop()
	for {
		var wake <-chan struct{}
		if s.Waker != nil {
			wake = s.Waker.Wait() // before claiming, so a NOTIFY during the claim is not missed
		}
		tasks, err := s.claim(ctx, w, free)
		if err != nil || len(tasks) > 0 {
			return &workerv1.PollTaskResponse{Tasks: tasks}, err
		}
		recheck := s.Clock.NewTimer(recheckEvery)
		select {
		case <-ctx.Done():
			recheck.Stop()
			return nil, ctx.Err()
		case <-deadline.C():
			recheck.Stop()
			return &workerv1.PollTaskResponse{}, nil
		case <-wake:
		case <-recheck.C():
		}
		recheck.Stop()
	}
}

// claim locks up to free claimable tasks and, in the same transaction, moves each QUEUED →
// RUNNING (attempt_count + 1) and creates its attempt with a fresh fencing token. Both steps
// record their events (task.started, task.attempt.started). SKIP LOCKED gives each task to at
// most one concurrent poller (ADR-004).
func (s *Service) claim(ctx context.Context, w persistence.WorkerView, free int) ([]*workerv1.Task, error) {
	var out []*workerv1.Task
	err := s.Tx.WithTx(ctx, pgx.TxOptions{}, func(tx pgx.Tx) error {
		out = out[:0]
		claimed, err := s.Tasks.ClaimQueued(ctx, tx, persistence.ClaimFilter{
			Types: w.TaskTypes, Queues: w.Queues, Now: s.Clock.Now(), Limit: free,
		})
		if err != nil {
			return err
		}
		for _, t := range claimed {
			err := s.Tasks.Transition(ctx, tx, persistence.TaskTransitionParams{
				TaskID: t.ID, From: model.TaskQueued, To: model.TaskRunning,
				Mutate: func(m *persistence.TaskMutation) { m.IncrementAttempt = true },
			})
			if err != nil {
				return err
			}
			a, err := s.Attempts.Create(ctx, tx, persistence.NewAttempt{
				TaskID: t.ID, RunID: t.RunID, AttemptNumber: t.AttemptCount + 1, WorkerID: w.WorkerID,
				TimeoutSeconds: t.TimeoutSeconds,
			})
			if err != nil {
				return err
			}
			out = append(out, taskMessage(t, a))
		}
		return nil
	})
	return out, err
}

func taskMessage(t persistence.TaskView, a persistence.AttemptView) *workerv1.Task {
	input := []byte(t.Input)
	if len(input) == 0 || !json.Valid(input) {
		input = []byte(`{}`)
	}
	return &workerv1.Task{
		TaskId: t.ID, AttemptId: a.ID, AttemptNumber: int32(a.AttemptNumber), //nolint:gosec // G115: attempt numbers are small (max_attempts ≤ 20)
		TaskType: t.Type, InputJson: input, Deadline: timestamppb.New(a.DeadlineAt), FencingToken: a.FencingToken,
		IdempotencyKey: a.IdempotencyKey, CorrelationId: t.RunID,
	}
}
