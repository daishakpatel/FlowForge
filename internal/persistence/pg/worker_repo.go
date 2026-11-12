package pg

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/persistence/sqlcgen"
)

// ClaimQueued implements persistence.TaskRepo.
func (r *TaskRepo) ClaimQueued(ctx context.Context, tx pgx.Tx, f persistence.ClaimFilter) ([]persistence.TaskView, error) {
	rows, err := sqlcgen.New(tx).ClaimQueuedTasks(ctx, sqlcgen.ClaimQueuedTasksParams{
		TaskTypes: f.Types, Queues: f.Queues, Now: timestamptz(f.Now), Lim: limitOf(f.Limit),
	})
	if err != nil {
		return nil, fmt.Errorf("claim tasks: %w", err)
	}
	return taskViews(rows)
}

// TouchRunning implements persistence.AttemptRepo.
func (r *AttemptRepo) TouchRunning(ctx context.Context, q persistence.Querier, workerID string, ids []string, tokens []int64, now time.Time) ([]string, error) {
	touched, err := sqlcgen.New(q).TouchRunningAttempts(ctx, sqlcgen.TouchRunningAttemptsParams{
		Now: timestamptz(now), AttemptIds: ids, FencingTokens: tokens, WorkerID: workerID,
	})
	if err != nil {
		return nil, fmt.Errorf("record attempt heartbeats: %w", err)
	}
	return touched, nil
}

// WorkerRepo implements persistence.WorkerRepo.
type WorkerRepo struct{}

var _ persistence.WorkerRepo = WorkerRepo{}

type workerMetadata struct {
	KeyID string `json:"key_id"`
}

// Upsert implements persistence.WorkerRepo.
func (WorkerRepo) Upsert(ctx context.Context, q persistence.Querier, w persistence.WorkerRegistration, now, liveAfter time.Time) (bool, error) {
	capacity, err := int32Of("capacity", w.Capacity)
	if err != nil {
		return false, err
	}
	meta, err := json.Marshal(workerMetadata{KeyID: w.KeyID})
	if err != nil {
		return false, fmt.Errorf("encode worker metadata: %w", err)
	}
	n, err := sqlcgen.New(q).UpsertWorker(ctx, sqlcgen.UpsertWorkerParams{
		WorkerID: w.WorkerID, Capacity: capacity, TaskTypes: w.TaskTypes, Queues: w.Queues,
		Version: text(w.Version), Metadata: meta, Now: timestamptz(now), LiveAfter: timestamptz(liveAfter),
	})
	if err != nil {
		return false, fmt.Errorf("register worker %s: %w", w.WorkerID, err)
	}
	return n == 1, nil
}

// Touch implements persistence.WorkerRepo.
func (WorkerRepo) Touch(ctx context.Context, q persistence.Querier, workerID, keyID string, activeTasks int, now time.Time) (bool, error) {
	active, err := int32Of("active_tasks", activeTasks)
	if err != nil {
		return false, err
	}
	n, err := sqlcgen.New(q).TouchWorker(ctx, sqlcgen.TouchWorkerParams{Now: timestamptz(now), ActiveTasks: active, WorkerID: workerID, KeyID: keyID})
	if err != nil {
		return false, fmt.Errorf("touch worker %s: %w", workerID, err)
	}
	return n == 1, nil
}

// Release implements persistence.WorkerRepo.
func (WorkerRepo) Release(ctx context.Context, q persistence.Querier, workerID, keyID string, now time.Time) error {
	if _, err := sqlcgen.New(q).ReleaseWorker(ctx, sqlcgen.ReleaseWorkerParams{Now: timestamptz(now), WorkerID: workerID, KeyID: keyID}); err != nil {
		return fmt.Errorf("release worker %s: %w", workerID, err)
	}
	return nil
}

// Get implements persistence.WorkerRepo.
func (WorkerRepo) Get(ctx context.Context, q persistence.Querier, workerID string) (persistence.WorkerView, error) {
	row, err := sqlcgen.New(q).GetWorker(ctx, workerID)
	if err != nil {
		return persistence.WorkerView{}, notFoundOr(err, "worker")
	}
	return workerView(row), nil
}

// Expired implements persistence.WorkerRepo.
func (WorkerRepo) Expired(ctx context.Context, q persistence.Querier, before time.Time) ([]string, error) {
	ids, err := sqlcgen.New(q).ExpiredWorkers(ctx, timestamptz(before))
	if err != nil {
		return nil, fmt.Errorf("list expired workers: %w", err)
	}
	return ids, nil
}

// List implements persistence.WorkerRepo.
func (WorkerRepo) List(ctx context.Context, q persistence.Querier) ([]persistence.WorkerView, error) {
	rows, err := sqlcgen.New(q).ListWorkers(ctx)
	if err != nil {
		return nil, fmt.Errorf("list workers: %w", err)
	}
	out := make([]persistence.WorkerView, len(rows))
	for i, r := range rows {
		out[i] = workerView(r)
	}
	return out, nil
}

func workerView(r sqlcgen.WorkerObservation) persistence.WorkerView {
	var meta workerMetadata
	_ = json.Unmarshal(r.Metadata, &meta) // absent or foreign metadata leaves KeyID empty
	return persistence.WorkerView{
		WorkerID: r.WorkerID, Status: r.Status, KeyID: meta.KeyID, Capacity: int(r.Capacity),
		ActiveTasks: int(r.ActiveTasks), TaskTypes: r.TaskTypes, Queues: r.Queues, Version: r.Version.String,
		FirstSeenAt: r.FirstSeenAt.Time, LastSeenAt: r.LastSeenAt.Time,
	}
}

// TaskLogRepo implements persistence.TaskLogRepo.
type TaskLogRepo struct{}

var _ persistence.TaskLogRepo = TaskLogRepo{}

// Insert implements persistence.TaskLogRepo.
func (TaskLogRepo) Insert(ctx context.Context, q persistence.Querier, lines []persistence.NewTaskLog) error {
	rows := make([]sqlcgen.InsertTaskLogsParams, len(lines))
	for i, l := range lines {
		rows[i] = sqlcgen.InsertTaskLogsParams{
			AttemptID: l.AttemptID, TaskID: l.TaskID, RunID: l.RunID, Ts: timestamptz(l.TS),
			Level: l.Level, Message: l.Message, Fields: l.Fields,
		}
	}
	if _, err := sqlcgen.New(q).InsertTaskLogs(ctx, rows); err != nil {
		return fmt.Errorf("insert task logs: %w", err)
	}
	return nil
}

// Count implements persistence.TaskLogRepo.
func (TaskLogRepo) Count(ctx context.Context, q persistence.Querier, attemptID string) (int, error) {
	n, err := sqlcgen.New(q).CountTaskLogs(ctx, attemptID)
	if err != nil {
		return 0, fmt.Errorf("count task logs: %w", err)
	}
	return int(n), nil
}

// List implements persistence.TaskLogRepo.
func (TaskLogRepo) List(ctx context.Context, q persistence.Querier, attemptID string, afterID int64, limit int) ([]persistence.TaskLogView, error) {
	rows, err := sqlcgen.New(q).ListTaskLogs(ctx, sqlcgen.ListTaskLogsParams{
		AttemptID: attemptID, AfterID: afterID, Lim: limitOf(persistence.Page{Limit: limit}.EffectiveLimit()),
	})
	if err != nil {
		return nil, fmt.Errorf("list task logs: %w", err)
	}
	out := make([]persistence.TaskLogView, len(rows))
	for i, r := range rows {
		out[i] = persistence.TaskLogView{ID: r.ID, AttemptID: r.AttemptID, TS: r.Ts.Time, Level: r.Level, Message: r.Message, Fields: r.Fields}
	}
	return out, nil
}
