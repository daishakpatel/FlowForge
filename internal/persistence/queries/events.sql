-- Run history and the transactional outbox (00E §2, ADR-009). Written by events.Recorder in the
-- transaction of the state change they describe.

-- name: InsertWorkflowEvent :exec
INSERT INTO workflow_events (event_id, run_id, task_id, event_type, event_version, payload, correlation_id)
VALUES (@event_id, @run_id, sqlc.narg(task_id), @event_type, @event_version, @payload, @correlation_id);

-- name: InsertOutbox :exec
INSERT INTO outbox (event_id, topic, key, envelope)
VALUES (@event_id, @topic, @key, @envelope);

-- name: ListEventsByRun :many
-- A run's history after a given workflow_events.id (0 for the start); uses ix_events_run.
SELECT * FROM workflow_events
WHERE run_id = @run_id AND id > @after_id
ORDER BY id
LIMIT @lim;
