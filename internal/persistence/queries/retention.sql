-- Retention (task 03.07): each statement removes one bounded batch, oldest first by id, so it never
-- holds many row locks on a hot table. retention.Loop sleeps between batches.

-- name: DeletePublishedOutbox :execrows
-- Published outbox rows created before @before. Unpublished rows are never deleted.
DELETE FROM outbox WHERE id IN (
  SELECT id FROM outbox
  WHERE published_at IS NOT NULL AND created_at < @before::timestamptz
  ORDER BY id LIMIT @lim);

-- name: DeleteOldEvents :execrows
DELETE FROM workflow_events WHERE id IN (
  SELECT id FROM workflow_events WHERE created_at < @before::timestamptz ORDER BY id LIMIT @lim);

-- name: DeleteOldTaskLogs :execrows
-- By the log line's own timestamp, the only time task_logs has.
DELETE FROM task_logs WHERE id IN (
  SELECT id FROM task_logs WHERE ts < @before::timestamptz ORDER BY id LIMIT @lim);

-- name: ClearOldIdempotencyKeys :execrows
-- Frees the Idempotency-Key of runs created before @before: a key is honoured for 24 h (02.04).
UPDATE workflow_runs SET idempotency_key = NULL WHERE id IN (
  SELECT id FROM workflow_runs
  WHERE idempotency_key IS NOT NULL AND created_at < @before::timestamptz
  ORDER BY id LIMIT @lim);
