-- The outbox relay's reads and marks (task 03.07, ADR-009). Rows are written by events.Recorder.

-- name: LockUnpublishedOutbox :many
-- The oldest unpublished rows in id order, row-locked until the relay's transaction ends; rows
-- another transaction holds are skipped (task 03.07 §4). Uses ix_outbox_unpublished.
SELECT * FROM outbox
WHERE published_at IS NULL
ORDER BY id
LIMIT @lim
FOR UPDATE SKIP LOCKED;

-- name: MarkOutboxPublished :exec
UPDATE outbox SET published_at = @published_at::timestamptz
WHERE id = ANY(@ids::bigint[]) AND published_at IS NULL;

-- name: OutboxBacklog :one
-- Unpublished rows and the creation time of the oldest (NULL when there are none).
SELECT count(*)::bigint AS backlog, min(created_at)::timestamptz AS oldest
FROM outbox
WHERE published_at IS NULL;
