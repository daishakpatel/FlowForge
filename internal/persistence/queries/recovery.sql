-- Timeout and worker-loss recovery (task 03.04). Times are the scheduler's clock passed as
-- parameters (03.04 §5). Attempt and task status change only through FinishAttempt and
-- TransitionTask (ADR-020); the only write here is the worker observation's LOST mark.

-- name: ListOverdueAttempts :many
-- RUNNING attempts whose deadline has passed at now; uses ix_attempts_running (status, deadline_at).
SELECT * FROM task_attempts
WHERE status = 'RUNNING' AND deadline_at <= @now
ORDER BY deadline_at
LIMIT @lim;

-- name: ListRunningAttemptsOfWorker :many
-- A lost worker's RUNNING attempts; uses ix_attempts_worker (partial on RUNNING).
SELECT * FROM task_attempts
WHERE status = 'RUNNING' AND worker_id = @worker_id
ORDER BY started_at
LIMIT @lim;

-- name: ListOrphanedAttempts :many
-- RUNNING attempts whose worker holds no lease (not in alive) and that showed no sign of life
-- since before: the heartbeat-staleness fallback. It scans the RUNNING attempts through the
-- partial index ix_attempts_running, bounded by how much work is in flight.
SELECT * FROM task_attempts
WHERE status = 'RUNNING'
  AND NOT (COALESCE(worker_id, '') = ANY(@alive::text[]))
  AND COALESCE(last_heartbeat_at, started_at) < @before
ORDER BY started_at
LIMIT @lim;

-- name: MarkWorkerLost :execrows
-- Marks an expired worker LOST, only while it is still ACTIVE and unseen since before, so a
-- heartbeat that renewed the lease meanwhile wins. Re-registering makes it ACTIVE again.
UPDATE worker_observations SET status = 'LOST'
WHERE worker_id = @worker_id AND status = 'ACTIVE' AND last_seen_at < @before;
