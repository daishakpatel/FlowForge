-- Workflow tasks (00E §2). Status changes only through TransitionTask (ADR-020).

-- name: InsertTasks :copyfrom
-- Bulk insert of a run's PENDING task rows; created_at, updated_at and available_at take their
-- defaults.
INSERT INTO workflow_tasks (id, run_id, task_key, task_type, queue_name, status, depends_on,
                            run_if, run_if_expr, config, retry_policy, timeout_seconds)
VALUES (@id, @run_id, @task_key, @task_type, @queue_name, @status, @depends_on,
        @run_if, @run_if_expr, @config, @retry_policy, @timeout_seconds);

-- name: GetTask :one
SELECT * FROM workflow_tasks
WHERE id = @id
LIMIT 1;

-- name: ListTasksByRun :many
-- Insertion order (ids are generated in topological order); uses ix_tasks_run.
SELECT * FROM workflow_tasks
WHERE run_id = @run_id
ORDER BY created_at, id
LIMIT @lim;

-- name: ListTasksByRunStatus :many
SELECT * FROM workflow_tasks
WHERE run_id = @run_id AND status = @status
ORDER BY created_at, id
LIMIT @lim;

-- name: TransitionTask :one
-- Conditional transition (ADR-020): no row when the task is missing or not in from_status.
-- NULL mutation arguments leave the column unchanged. available_at uses the database clock
-- (ADR-021).
UPDATE workflow_tasks SET
  status        = @to_status::text,
  version       = version + 1,
  updated_at    = now(),
  input         = COALESCE(sqlc.narg(input)::jsonb, input),
  result        = COALESCE(sqlc.narg(result)::jsonb, result),
  attempt_count = attempt_count + @attempt_increment::int,
  available_at  = CASE WHEN sqlc.narg(retry_delay_ms)::bigint IS NULL THEN available_at
                       ELSE now() + sqlc.narg(retry_delay_ms)::bigint * interval '1 millisecond' END
WHERE id = @id AND status = @from_status::text
RETURNING *;
