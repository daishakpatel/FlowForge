-- Run control: cancellation, cancel signals to workers, and workflow deadlines (task 03.05).
-- Run and task status change only through TransitionRun and TransitionTask (ADR-020).

-- name: RequestRunCancel :one
-- Marks a run cancel-requested and pulls its deadline in to force_at at the latest: the time its
-- RUNNING attempts are force-cancelled if their workers have not stopped them (TimeoutLoop).
-- A repeated request keeps the earlier deadline. No row when the run is not cancellable.
UPDATE workflow_runs SET
  cancel_requested = true,
  deadline_at      = LEAST(COALESCE(deadline_at, @force_at::timestamptz), @force_at::timestamptz),
  version          = version + 1
WHERE id = @id AND status IN ('CREATED', 'RUNNING', 'PAUSED', 'NEEDS_ATTENTION')
RETURNING *;

-- name: ListCancelRequestedAttempts :many
-- The given attempts that are RUNNING in a cancel-requested run: what CancelSource tells the
-- worker to stop in its next heartbeat answer. Bounded by the heartbeat's attempt list.
SELECT a.id FROM task_attempts a
JOIN workflow_tasks t ON t.id = a.task_id
JOIN workflow_runs r ON r.id = t.run_id
WHERE a.id = ANY(@attempt_ids::text[]) AND a.status = 'RUNNING' AND r.cancel_requested
LIMIT 1000;

-- name: ListOverdueRuns :many
-- RUNNING or PAUSED runs whose deadline has passed at now (the scheduler clock): a workflow
-- timeout, or a cancel whose grace ran out. Few runs are active, and ix_runs_status_created
-- narrows to these statuses.
SELECT id FROM workflow_runs
WHERE status IN ('RUNNING', 'PAUSED') AND deadline_at <= @now
ORDER BY deadline_at
LIMIT @lim;
