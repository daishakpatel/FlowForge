-- Workflow definitions (00E §2). Definitions are immutable: there is no UPDATE (FR-WF-01).

-- name: InsertDefinition :one
-- A duplicate (name, version) violates the UNIQUE constraint; the repository maps that to CONFLICT.
INSERT INTO workflow_definitions (id, name, version, definition, created_by)
VALUES (@id, @name, @version, @definition, @created_by)
RETURNING *;

-- name: GetDefinition :one
SELECT * FROM workflow_definitions
WHERE id = @id
LIMIT 1;

-- name: GetDefinitionByNameVersion :one
SELECT * FROM workflow_definitions
WHERE name = @name AND version = @version
LIMIT 1;

-- name: GetLatestDefinition :one
-- Uses the UNIQUE (name, version) index backwards.
SELECT * FROM workflow_definitions
WHERE name = @name
ORDER BY version DESC
LIMIT 1;

-- name: ListDefinitions :many
-- Keyset page ordered (created_at DESC, id DESC); pass a NULL cursor for the first page.
SELECT * FROM workflow_definitions
WHERE sqlc.narg(cursor_created_at)::timestamptz IS NULL
   OR (created_at, id) < (sqlc.narg(cursor_created_at)::timestamptz, sqlc.narg(cursor_id)::text)
ORDER BY created_at DESC, id DESC
LIMIT @lim;
