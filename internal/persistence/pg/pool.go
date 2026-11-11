// Package pg is the Postgres implementation of internal/persistence (task 02.03): the pgx pool
// factory, the retrying TxManager and the repositories. All SQL is sqlc-generated
// (internal/persistence/queries); state changes are conditional UPDATEs (ADR-020).
package pg

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daishakpatel/flowforge/internal/config"
)

// NewPool connects a pgx pool to dsn (config FF_PG_DSN) and pings it, so a bad DSN or an
// unreachable database fails at startup. The caller closes the pool. pgx redacts the password
// in parse errors.
func NewPool(ctx context.Context, dsn config.Secret) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn.Reveal())
	if err != nil {
		return nil, fmt.Errorf("parse postgres dsn: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return pool, nil
}
