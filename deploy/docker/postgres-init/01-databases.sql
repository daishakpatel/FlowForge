-- Dev-only roles and databases for the `postgres` Compose service (task 01.02, ADR-013).
-- Runs once, when the data volume is first created; `make dev-reset` recreates the volume.
-- Passwords are dev-only constants (00C §10.4); never reuse them outside local development.
CREATE ROLE flowforge LOGIN PASSWORD 'flowforge';
CREATE DATABASE flowforge OWNER flowforge;

CREATE ROLE aiops LOGIN PASSWORD 'aiops';
CREATE DATABASE aiops OWNER aiops;
