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

// PoolOption adjusts a pool's configuration before NewPool connects.
type PoolOption func(*pgxpool.Config)

// SessionParam sets a Postgres run-time parameter on every session of the pool (task 04.07).
func SessionParam(name, value string) PoolOption {
	return func(c *pgxpool.Config) { c.ConnConfig.RuntimeParams[name] = value }
}

// NewPool connects a pgx pool to dsn (config FF_PG_DSN) and pings it, so a bad DSN or an
// unreachable database fails at startup. The caller closes the pool. pgx redacts the password
// in parse errors. opts adjust the configuration first (SessionParam).
func NewPool(ctx context.Context, dsn config.Secret, opts ...PoolOption) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn.Reveal())
	if err != nil {
		return nil, fmt.Errorf("parse postgres dsn: %w", err)
	}
	for _, o := range opts {
		o(cfg)
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
