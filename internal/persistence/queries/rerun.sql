-- Remediation reruns (ADR-008, task 03.06).

-- name: InsertRerunTask :one
-- A new PENDING row that supersedes @supersedes_task_id: it copies the definition fields, with
-- attempt_count 0 and no input or result; the original row is untouched. UNIQUE (run_id,
-- task_key, supersedes_task_id) makes a second rerun of the same row a unique violation. No row
-- when the task is not in the run.
INSERT INTO workflow_tasks (id, run_id, task_key, task_type, queue_name, status, depends_on,
                            run_if, run_if_expr, config, retry_policy, timeout_seconds, supersedes_task_id)
SELECT @id::text, t.run_id, t.task_key, t.task_type, t.queue_name, 'PENDING', t.depends_on,
       t.run_if, t.run_if_expr, t.config, t.retry_policy, t.timeout_seconds, t.id
FROM workflow_tasks t
WHERE t.id = @supersedes_task_id::text AND t.run_id = @run_id::text
RETURNING id;
