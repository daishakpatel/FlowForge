-- name: GetClusterState :one
-- The single scheduler-epoch fence row (00E §2, ADR-003). Proves the sqlc pipeline (task 02.01);
-- 02.03 adds the epoch queries.
SELECT id, epoch, leader_id, updated_at
FROM cluster_state
WHERE id = 1
LIMIT 1;
