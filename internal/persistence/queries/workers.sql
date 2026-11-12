-- Worker observations (00E §2, task 03.01): the Postgres LeaseProvider's state. metadata.key_id is
-- the API key that registered the worker. Times are the API clock (ADR-021).

-- name: UpsertWorker :execrows
-- 0 rows: the id is live under another key (the caller reports CONFLICT).
INSERT INTO worker_observations (worker_id, status, capacity, active_tasks, task_types, queues, version, metadata, first_seen_at, last_seen_at)
VALUES (@worker_id, 'ACTIVE', @capacity, 0, @task_types, @queues, sqlc.narg(version), @metadata, @now, @now)
ON CONFLICT (worker_id) DO UPDATE SET
  status = 'ACTIVE', capacity = EXCLUDED.capacity, active_tasks = 0, task_types = EXCLUDED.task_types,
  queues = EXCLUDED.queues, version = EXCLUDED.version, metadata = EXCLUDED.metadata,
  last_seen_at = EXCLUDED.last_seen_at
WHERE worker_observations.metadata->>'key_id' = EXCLUDED.metadata->>'key_id'
   OR worker_observations.status <> 'ACTIVE'
   OR worker_observations.last_seen_at < @live_after;

-- name: TouchWorker :execrows
UPDATE worker_observations SET last_seen_at = @now, active_tasks = @active_tasks
WHERE worker_id = @worker_id AND status = 'ACTIVE' AND metadata->>'key_id' = @key_id::text;

-- name: ReleaseWorker :execrows
UPDATE worker_observations SET status = 'DEREGISTERED', last_seen_at = @now
WHERE worker_id = @worker_id AND metadata->>'key_id' = @key_id::text;

-- name: GetWorker :one
SELECT * FROM worker_observations
WHERE worker_id = @worker_id
LIMIT 1;

-- name: ExpiredWorkers :many
SELECT worker_id FROM worker_observations
WHERE status = 'ACTIVE' AND last_seen_at < @before
ORDER BY worker_id
LIMIT 1000;

-- name: ListWorkers :many
SELECT * FROM worker_observations
ORDER BY worker_id
LIMIT 1000;
