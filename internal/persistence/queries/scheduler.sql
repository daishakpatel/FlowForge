-- Scheduler reads (task 03.02, ADR-004). Status changes go through TransitionRun and
-- TransitionTask only (ADR-020); nothing here writes a row.

-- name: ListActiveRunIDs :many
-- One page of the scheduler's round-robin over the runs it drives: CREATED or RUNNING runs, and
-- PAUSED or NEEDS_ATTENTION runs with a cancel request (03.05), with an id after after_id, in
-- id order. The caller wraps to after_id = '' when a page comes back short, so every active run
-- is visited even when more than lim are active (fairness). Run ids are ULIDs, so id order is
-- creation order. ix_runs_status_created narrows to the statuses.
SELECT id FROM workflow_runs
WHERE (status IN ('CREATED', 'RUNNING') OR (cancel_requested AND status IN ('PAUSED', 'NEEDS_ATTENTION')))
  AND id > @after_id::text
ORDER BY id
LIMIT @lim;

-- name: LockRun :one
-- Reads the run and row-locks it until the transaction ends, so scheduler decisions on one run
-- are serialized. NO KEY UPDATE still lets task rows referencing the run be inserted (rerun).
SELECT * FROM workflow_runs
WHERE id = @id
LIMIT 1
FOR NO KEY UPDATE;

-- name: ListHeadTasks :many
-- The run's head task rows: those no other row supersedes (rerun, ADR-008), in creation order
-- (ids are generated in topological order). The subquery stays inside the run via ix_tasks_run.
SELECT t.* FROM workflow_tasks t
WHERE t.run_id = @run_id
  AND NOT EXISTS (
    SELECT 1 FROM workflow_tasks s
    WHERE s.run_id = t.run_id AND s.supersedes_task_id = t.id
  )
ORDER BY t.created_at, t.id
LIMIT @lim;

-- name: CountActiveTasksByQueue :many
-- QUEUED + RUNNING tasks of the given queues across all runs, for FF_QUEUE_LIMITS. Only run when
-- limits are configured; RUNNING rows have no queue index, so this scans the active tasks
-- (bounded by worker capacity plus the queue backlog).
SELECT queue_name, count(*)::int AS active
FROM workflow_tasks
WHERE status IN ('QUEUED', 'RUNNING') AND queue_name = ANY(@queues::text[])
GROUP BY queue_name
LIMIT 1000;

-- name: CountQueuedTasksByQueue :many
-- Backlog per queue for flowforge_scheduler_queue_depth, once per tick; uses the partial index
-- ix_tasks_dispatch (status = 'QUEUED').
SELECT queue_name, count(*)::int AS queued
FROM workflow_tasks
WHERE status = 'QUEUED'
GROUP BY queue_name
LIMIT 1000;

-- name: Notify :exec
-- Inside a transaction Postgres delivers the notification at commit, and not at all on rollback.
SELECT pg_notify(@channel::text, '');
