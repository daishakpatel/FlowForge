-- Cron triggers (task 03.08, 00E §2 cron_triggers).

-- name: InsertTrigger :one
-- A duplicate name violates the UNIQUE constraint (CONFLICT).
INSERT INTO cron_triggers (id, name, workflow_name, workflow_version, schedule, timezone, input,
                           missed_policy, enabled, next_fire_at, created_by)
VALUES (@id, @name, @workflow_name, sqlc.narg(workflow_version), @schedule, @timezone, @input,
        @missed_policy, @enabled, sqlc.narg(next_fire_at), @created_by)
RETURNING *;

-- name: GetTrigger :one
SELECT * FROM cron_triggers WHERE id = @id LIMIT 1;

-- name: ListTriggers :many
-- Keyset page ordered (created_at DESC, id DESC); pass a NULL cursor for the first page.
SELECT * FROM cron_triggers
WHERE sqlc.narg(cursor_created_at)::timestamptz IS NULL
   OR (created_at, id) < (sqlc.narg(cursor_created_at)::timestamptz, sqlc.narg(cursor_id)::text)
ORDER BY created_at DESC, id DESC
LIMIT @lim;

-- name: UpdateTrigger :one
-- Replaces the mutable fields (PATCH merges in the service). With @keep_next the stored
-- next_fire_at is kept, so a fire the cron loop advanced meanwhile is not reverted. No row when the
-- trigger is missing.
UPDATE cron_triggers SET
  workflow_version = sqlc.narg(workflow_version),
  schedule = @schedule, timezone = @timezone, input = @input, missed_policy = @missed_policy,
  enabled = @enabled,
  next_fire_at = CASE WHEN @keep_next::bool THEN next_fire_at ELSE sqlc.narg(next_fire_at) END
WHERE id = @id
RETURNING *;

-- name: DeleteTrigger :execrows
DELETE FROM cron_triggers WHERE id = @id;

-- name: LockDueTriggers :many
-- Enabled triggers due at @now, oldest fire first, row-locked until the cron loop's transaction
-- ends; triggers another loop holds are skipped. A sequential scan: there are few triggers.
SELECT * FROM cron_triggers
WHERE enabled AND next_fire_at <= @now::timestamptz
ORDER BY next_fire_at
LIMIT @lim
FOR UPDATE SKIP LOCKED;

-- name: AdvanceTrigger :execrows
-- Moves a trigger past the fire it handled, only if no one moved it first (next_fire_at is still
-- @old_next): this conditional update is what stops a fire from being advanced twice.
UPDATE cron_triggers SET
  next_fire_at = sqlc.narg(next_fire_at),
  last_fired_at = COALESCE(sqlc.narg(fired_at)::timestamptz, last_fired_at)
WHERE id = @id AND next_fire_at = @old_next::timestamptz;
