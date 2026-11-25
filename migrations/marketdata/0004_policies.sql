-- 0004_policies.sql (task 05.02): TimescaleDB background policies for the tick store.
-- Ticks older than 2 days are compressed (segmented by symbol; event_id is in the ordering because it
-- is part of the unique key) and ticks older than 14 days are dropped, a chunk at a time. Aggregates
-- are kept: they are small and the record. Late or replayed ticks may still be inserted into
-- compressed chunks; the unique key is checked there too.
-- Rollback: forward-only (00C §9); remove_retention_policy / remove_compression_policy undo it by hand.
-- +goose Up
ALTER TABLE market_ticks SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'symbol',
  timescaledb.compress_orderby = 'event_time DESC, event_id'
);
SELECT add_compression_policy('market_ticks', INTERVAL '2 days', if_not_exists => true);
SELECT add_retention_policy('market_ticks', INTERVAL '14 days', if_not_exists => true);

-- +goose Down
-- Forward-only (00C §9): no down migration. Tests get a fresh database instead.
