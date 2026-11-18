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

-- name: StreamEvents :many
-- The SSE stream (03.07): events after a workflow_events.id, optionally of some types and created
-- since a time, up to an id the stream knows is settled (EventMarksAfter), with the outbox envelope
-- while its row exists (published rows are kept 3 days). Walks the primary key from after_id.
SELECT e.id, e.event_id, e.run_id, e.task_id, e.event_type, e.event_version, e.payload,
       e.correlation_id, e.created_at, o.envelope
FROM workflow_events e
LEFT JOIN outbox o ON o.event_id = e.event_id
WHERE e.id > @after_id AND e.id <= @up_to
  AND (sqlc.narg(types)::text[] IS NULL OR e.event_type = ANY(sqlc.narg(types)::text[]))
  AND (sqlc.narg(since)::timestamptz IS NULL OR e.created_at >= sqlc.narg(since)::timestamptz)
ORDER BY e.id
LIMIT @lim;

-- name: StreamRunEvents :many
-- StreamEvents for one run; uses ix_events_run.
SELECT e.id, e.event_id, e.run_id, e.task_id, e.event_type, e.event_version, e.payload,
       e.correlation_id, e.created_at, o.envelope
FROM workflow_events e
LEFT JOIN outbox o ON o.event_id = e.event_id
WHERE e.run_id = @run_id AND e.id > @after_id AND e.id <= @up_to
  AND (sqlc.narg(types)::text[] IS NULL OR e.event_type = ANY(sqlc.narg(types)::text[]))
  AND (sqlc.narg(since)::timestamptz IS NULL OR e.created_at >= sqlc.narg(since)::timestamptz)
ORDER BY e.id
LIMIT @lim;

-- name: LastEventID :one
-- The newest workflow_events.id, 0 when there is none: where a live-only stream starts.
SELECT COALESCE(max(id), 0)::bigint FROM workflow_events;

-- name: EventMarksAfter :many
-- The next ids after @after_id, unfiltered, with their creation time: the SSE stream finds the
-- commit horizon from them (ids are assigned at insert, so a missing id may still commit).
SELECT id, created_at FROM workflow_events
WHERE id > @after_id
ORDER BY id
LIMIT @lim;
