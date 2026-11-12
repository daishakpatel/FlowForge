-- Workflow runs (00E §2). Status changes only through TransitionRun (ADR-020).

-- name: InsertRun :one
-- deadline_at uses the database clock (ADR-021); timeout_seconds 0 means no workflow timeout.
INSERT INTO workflow_runs (id, definition_id, status, input, idempotency_key, trigger_id,
                           parent_run_id, deadline_at, created_by)
VALUES (@id, @definition_id, 'CREATED', @input, sqlc.narg(idempotency_key), sqlc.narg(trigger_id),
        sqlc.narg(parent_run_id),
        CASE WHEN @timeout_seconds::int > 0 THEN now() + make_interval(secs => @timeout_seconds::int) END,
        @created_by)
RETURNING *;

-- name: GetRun :one
SELECT * FROM workflow_runs
WHERE id = @id
LIMIT 1;

-- name: GetRunByIdempotencyKey :one
-- Idempotent run start (task 02.04); uses the partial unique index uq_runs_idem.
SELECT * FROM workflow_runs
WHERE idempotency_key = @idempotency_key
LIMIT 1;

-- name: ListRuns :many
-- Keyset page ordered (created_at DESC, id DESC) with optional filters (00E §9); NULL means
-- "no filter". workflow filters on the definition name.
SELECT r.* FROM workflow_runs r
JOIN workflow_definitions d ON d.id = r.definition_id
WHERE (sqlc.narg(status)::text IS NULL OR r.status = sqlc.narg(status)::text)
  AND (sqlc.narg(workflow)::text IS NULL OR d.name = sqlc.narg(workflow)::text)
  AND (sqlc.narg(since)::timestamptz IS NULL OR r.created_at >= sqlc.narg(since)::timestamptz)
  AND (sqlc.narg(until)::timestamptz IS NULL OR r.created_at < sqlc.narg(until)::timestamptz)
  AND (sqlc.narg(cursor_created_at)::timestamptz IS NULL
       OR (r.created_at, r.id) < (sqlc.narg(cursor_created_at)::timestamptz, sqlc.narg(cursor_id)::text))
ORDER BY r.created_at DESC, r.id DESC
LIMIT @lim;

-- name: TransitionRun :one
-- Conditional transition (ADR-020): no row when the run is missing, not in from_status, or at
-- another version. started_at is set on the first move to RUNNING; completed_at when terminal.
UPDATE workflow_runs SET
  status       = @to_status::text,
  version      = version + 1,
  started_at   = CASE WHEN @to_status::text = 'RUNNING' THEN COALESCE(started_at, now()) ELSE started_at END,
  completed_at = CASE WHEN @terminal::bool THEN now() ELSE completed_at END,
  leader_epoch = COALESCE(sqlc.narg(leader_epoch)::bigint, leader_epoch)
WHERE id = @id AND status = @from_status::text AND version = @version
RETURNING *;
