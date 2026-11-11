-- Task attempts (00E §2). An attempt is created RUNNING and leaves RUNNING only through
-- FinishAttempt, which checks the fencing token (ADR-003).

-- name: InsertAttempt :one
-- fencing_token comes from attempt_fencing_seq; deadline_at uses the database clock (ADR-021).
INSERT INTO task_attempts (id, task_id, attempt_number, worker_id, status, idempotency_key, deadline_at)
VALUES (@id, @task_id, @attempt_number, sqlc.narg(worker_id), 'RUNNING', @idempotency_key,
        now() + make_interval(secs => @timeout_seconds::int))
RETURNING *;

-- name: GetAttempt :one
SELECT * FROM task_attempts
WHERE id = @id
LIMIT 1;

-- name: ListAttemptsByTask :many
SELECT * FROM task_attempts
WHERE task_id = @task_id
ORDER BY attempt_number
LIMIT @lim;

-- name: FinishAttempt :one
-- Conditional finish: no row when the attempt is missing, not in from_status, or the fencing
-- token is stale. Returns the run id for the event.
UPDATE task_attempts a SET
  status        = @to_status::text,
  finished_at   = now(),
  error_code    = sqlc.narg(error_code),
  error_message = sqlc.narg(error_message),
  retryable     = sqlc.narg(retryable),
  result        = sqlc.narg(result)
FROM workflow_tasks t
WHERE a.id = @id AND a.status = @from_status::text AND a.fencing_token = @fencing_token
  AND t.id = a.task_id
RETURNING a.id, a.task_id, a.attempt_number, a.worker_id, a.fencing_token, t.run_id;
