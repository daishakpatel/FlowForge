-- Per-attempt task logs (00E §2, ADR-018, task 03.01).

-- name: InsertTaskLogs :copyfrom
INSERT INTO task_logs (attempt_id, task_id, run_id, ts, level, message, fields)
VALUES (@attempt_id, @task_id, @run_id, @ts, @level, @message, @fields);

-- name: CountTaskLogs :one
-- Bounded by the 1001-row cap per attempt (1000 lines plus the limit marker).
SELECT count(*) FROM task_logs
WHERE attempt_id = @attempt_id;

-- name: ListTaskLogs :many
-- Uses ix_logs_attempt.
SELECT * FROM task_logs
WHERE attempt_id = @attempt_id AND id > @after_id
ORDER BY id
LIMIT @lim;
