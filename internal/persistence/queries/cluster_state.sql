-- name: GetClusterState :one
-- The single scheduler-epoch fence row (00E §2, ADR-003).
SELECT id, epoch, leader_id, updated_at
FROM cluster_state
WHERE id = 1
LIMIT 1;

-- name: SetEpochIfGreater :execrows
-- Raises the fence; 0 rows when the stored epoch is already >= epoch (master spec §3.5).
UPDATE cluster_state
SET epoch = @epoch, leader_id = @leader_id, updated_at = now()
WHERE id = 1 AND epoch < @epoch;

-- name: LockEpoch :one
-- Row-share lock held to commit, so SetEpochIfGreater waits for in-flight scheduler transactions
-- (ADR-003).
SELECT epoch
FROM cluster_state
WHERE id = 1
LIMIT 1
FOR SHARE;
