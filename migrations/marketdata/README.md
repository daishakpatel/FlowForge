# marketdata migrations

goose migrations for the TimescaleDB `marketdata` database (`00E` §12). Task 05.03 adds the first one; until then the set is empty and `flowforge-migrate --db marketdata up` is a no-op. See [`docs/architecture/database.md`](../../docs/architecture/database.md).
