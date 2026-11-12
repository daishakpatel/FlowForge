-- Worker claim and attempt heartbeats (task 03.01, ADR-004). Times are the API clock (ADR-021).

-- name: ClaimQueuedTasks :many
-- Locks up to lim claimable tasks for the calling transaction; concurrent pollers skip locked
-- rows, so each task goes to at most one of them. Tasks of paused or cancel-requested runs are
-- never claimable. The caller moves each task QUEUED → RUNNING in the same transaction.
SELECT t.* FROM workflow_tasks t
JOIN workflow_runs r ON r.id = t.run_id
WHERE t.status = 'QUEUED'
  AND t.task_type = ANY(@task_types::text[])
  AND t.queue_name = ANY(@queues::text[])
  AND t.available_at <= @now
  AND r.status <> 'PAUSED'
  AND NOT r.cancel_requested
ORDER BY t.available_at, t.created_at
LIMIT @lim
FOR UPDATE OF t SKIP LOCKED;

-- name: TouchRunningAttempts :many
-- Records a heartbeat for each (id, fencing token) still RUNNING on this worker and returns the
-- ids it touched; any id not returned is stale.
-- fencing_tokens[i] is the token of attempt_ids[i].
UPDATE task_attempts SET last_heartbeat_at = @now
WHERE id = ANY(@attempt_ids::text[])
  AND fencing_token = (@fencing_tokens::bigint[])[array_position(@attempt_ids::text[], id)]
  AND status = 'RUNNING' AND worker_id = @worker_id::text
RETURNING id;
