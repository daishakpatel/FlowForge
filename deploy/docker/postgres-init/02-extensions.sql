-- pgvector for the AI knowledge base in `aiops` (ADR-013). CREATE EXTENSION needs a superuser,
-- so it runs here rather than in the AI service's own migrations.
\connect aiops
CREATE EXTENSION IF NOT EXISTS vector;
