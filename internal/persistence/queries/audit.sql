-- Audit log (00E §2, FR-SEC-03): one row per mutating or denied API request.

-- name: InsertAuditLog :exec
INSERT INTO audit_log (actor_key_id, actor_role, action, resource_type, resource_id, request_id, details, outcome)
VALUES (sqlc.narg(actor_key_id), sqlc.narg(actor_role), @action, sqlc.narg(resource_type), sqlc.narg(resource_id),
        @request_id, @details, @outcome);
