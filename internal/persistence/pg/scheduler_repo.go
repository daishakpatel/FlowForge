package pg

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/daishakpatel/flowforge/internal/persistence"
	"github.com/daishakpatel/flowforge/internal/persistence/sqlcgen"
)

// SchedulerRepo implements persistence.SchedulerRepo and persistence.Notifier (task 03.02).
type SchedulerRepo struct{}

var (
	_ persistence.SchedulerRepo = SchedulerRepo{}
	_ persistence.Notifier      = SchedulerRepo{}
)

// ActiveRunIDs implements persistence.SchedulerRepo.
func (SchedulerRepo) ActiveRunIDs(ctx context.Context, q persistence.Querier, afterID string, limit int) ([]string, error) {
	ids, err := sqlcgen.New(q).ListActiveRunIDs(ctx, sqlcgen.ListActiveRunIDsParams{AfterID: afterID, Lim: limitOf(limit)})
	if err != nil {
		return nil, fmt.Errorf("list active runs: %w", err)
	}
	return ids, nil
}

// LockRun implements persistence.SchedulerRepo.
func (SchedulerRepo) LockRun(ctx context.Context, tx pgx.Tx, id string) (persistence.RunView, error) {
	row, err := sqlcgen.New(tx).LockRun(ctx, id)
	if err != nil {
		return persistence.RunView{}, notFoundOr(err, "run")
	}
	return runView(row), nil
}

// HeadTasks implements persistence.SchedulerRepo.
func (SchedulerRepo) HeadTasks(ctx context.Context, q persistence.Querier, runID string, limit int) ([]persistence.TaskView, error) {
	rows, err := sqlcgen.New(q).ListHeadTasks(ctx, sqlcgen.ListHeadTasksParams{RunID: runID, Lim: limitOf(limit)})
	if err != nil {
		return nil, fmt.Errorf("list head tasks of run %s: %w", runID, err)
	}
	return taskViews(rows)
}

// ActiveByQueue implements persistence.SchedulerRepo.
func (SchedulerRepo) ActiveByQueue(ctx context.Context, q persistence.Querier, queues []string) (map[string]int, error) {
	rows, err := sqlcgen.New(q).CountActiveTasksByQueue(ctx, queues)
	if err != nil {
		return nil, fmt.Errorf("count active tasks by queue: %w", err)
	}
	out := make(map[string]int, len(rows))
	for _, r := range rows {
		out[r.QueueName] = int(r.Active)
	}
	return out, nil
}

// QueuedByQueue implements persistence.SchedulerRepo.
func (SchedulerRepo) QueuedByQueue(ctx context.Context, q persistence.Querier) (map[string]int, error) {
	rows, err := sqlcgen.New(q).CountQueuedTasksByQueue(ctx)
	if err != nil {
		return nil, fmt.Errorf("count queued tasks by queue: %w", err)
	}
	out := make(map[string]int, len(rows))
	for _, r := range rows {
		out[r.QueueName] = int(r.Queued)
	}
	return out, nil
}

// Notify implements persistence.Notifier.
func (SchedulerRepo) Notify(ctx context.Context, q persistence.Querier, channel string) error {
	if err := sqlcgen.New(q).Notify(ctx, channel); err != nil {
		return fmt.Errorf("notify %s: %w", channel, err)
	}
	return nil
}
