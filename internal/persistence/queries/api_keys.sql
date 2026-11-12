-- API keys (00E §2, ADR-015). Only the salted SHA-256 of a secret is stored; a key is revoked by
-- setting revoked_at, never deleted, so audit rows keep a resolvable actor.

-- name: InsertAPIKey :one
INSERT INTO api_keys (id, name, role, secret_hash, salt, created_by, expires_at)
VALUES (@id, @name, @role, @secret_hash, @salt, @created_by, sqlc.narg(expires_at))
RETURNING *;

-- name: InsertAPIKeyIfAbsent :execrows
-- Bootstrap: inserting the same configured key twice is a no-op.
INSERT INTO api_keys (id, name, role, secret_hash, salt, created_by)
VALUES (@id, @name, @role, @secret_hash, @salt, @created_by)
ON CONFLICT (id) DO NOTHING;

-- name: GetAPIKey :one
SELECT * FROM api_keys
WHERE id = @id
LIMIT 1;

-- name: ListAPIKeys :many
-- Keyset page ordered (created_at DESC, id DESC); pass a NULL cursor for the first page.
SELECT * FROM api_keys
WHERE sqlc.narg(cursor_created_at)::timestamptz IS NULL
   OR (created_at, id) < (sqlc.narg(cursor_created_at)::timestamptz, sqlc.narg(cursor_id)::text)
ORDER BY created_at DESC, id DESC
LIMIT @lim;

-- name: HasActiveAdmin :one
SELECT EXISTS (
  SELECT 1 FROM api_keys
  WHERE role = 'admin' AND revoked_at IS NULL AND (expires_at IS NULL OR expires_at > now())
);

-- name: LockActiveAdmins :many
-- Row-locks every active admin key until the transaction ends, so two concurrent revocations
-- cannot both see "another admin remains" and leave none (the waiter re-checks revoked_at).
SELECT id FROM api_keys
WHERE role = 'admin' AND revoked_at IS NULL AND (expires_at IS NULL OR expires_at > now())
ORDER BY id
LIMIT 10000
FOR UPDATE;

-- name: RevokeAPIKey :execrows
UPDATE api_keys SET revoked_at = now()
WHERE id = @id AND revoked_at IS NULL;
