// Package migrate applies FlowForge's embedded goose migrations (task 02.01, ADR-014).
//
// Migrations are forward-only. They run explicitly through cmd/flowforge-migrate, automatically
// in tests (testutil.NewFlowforgeDB), and from a Compose init job; services never migrate on
// their own. A Postgres advisory lock serialises concurrent runs against the same database, so
// two migrators can start at once safely.
package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

// MigrationStatus is the state of one migration in a database.
type MigrationStatus struct {
	Version   int64
	Name      string    // file name, e.g. 0001_core.sql
	Applied   bool      // false means pending
	AppliedAt time.Time // zero when pending
}

// Up applies every pending migration of set to the database at dsn and returns the resulting
// schema version. Running it again is a no-op. An empty set (marketdata before task 05.03)
// only checks the connection and returns 0.
func Up(ctx context.Context, dsn string, set Set) (int64, error) {
	var version int64
	err := withProvider(ctx, dsn, set, func(p *goose.Provider) error {
		if _, err := p.Up(ctx); err != nil {
			return fmt.Errorf("up: %w", err)
		}
		v, err := p.GetDBVersion(ctx)
		version = v
		return err
	})
	return version, err
}

// Status lists every migration of set with its state in the database at dsn, oldest first.
func Status(ctx context.Context, dsn string, set Set) ([]MigrationStatus, error) {
	var out []MigrationStatus
	err := withProvider(ctx, dsn, set, func(p *goose.Provider) error {
		list, err := p.Status(ctx)
		if err != nil {
			return fmt.Errorf("status: %w", err)
		}
		for _, s := range list {
			out = append(out, MigrationStatus{
				Version:   s.Source.Version,
				Name:      s.Source.Path,
				Applied:   s.State == goose.StateApplied,
				AppliedAt: s.AppliedAt,
			})
		}
		return nil
	})
	return out, err
}

// Version returns the schema version of set in the database at dsn: the highest applied
// migration, or 0 if none. 02.04 compares it with Latest to refuse starting on an old schema.
func Version(ctx context.Context, dsn string, set Set) (int64, error) {
	var version int64
	err := withProvider(ctx, dsn, set, func(p *goose.Provider) error {
		v, err := p.GetDBVersion(ctx)
		version = v
		return err
	})
	return version, err
}

// Latest returns the highest migration version embedded for set, without touching a database.
func Latest(set Set) (int64, error) {
	fsys, err := source(set)
	if err != nil {
		return 0, err
	}
	names, err := fs.Glob(fsys, "*.sql")
	if err != nil {
		return 0, fmt.Errorf("migrate %s: list: %w", set, err)
	}
	var latest int64
	for _, name := range names {
		v, err := goose.NumericComponent(name)
		if err != nil {
			return 0, fmt.Errorf("migrate %s: %s: %w", set, name, err)
		}
		latest = max(latest, v)
	}
	return latest, nil
}

// withProvider opens the database, builds a locked goose provider for set and runs fn. For an
// empty set it only pings the database and skips fn.
func withProvider(ctx context.Context, dsn string, set Set, fn func(*goose.Provider) error) error {
	fsys, err := source(set)
	if err != nil {
		return err
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("migrate %s: open: %w", set, err)
	}
	// Poll the advisory lock every second (goose's default is 5 s) for up to 5 minutes.
	locker, err := lock.NewPostgresSessionLocker(lock.WithLockTimeout(1, 300))
	if err != nil {
		_ = db.Close() // the locker error is the one worth returning
		return fmt.Errorf("migrate %s: lock: %w", set, err)
	}
	p, err := goose.NewProvider(goose.DialectPostgres, db, fsys, goose.WithSessionLocker(locker))
	if errors.Is(err, goose.ErrNoMigrations) {
		defer func() { _ = db.Close() }() // read-only ping; a close error changes nothing
		if err := db.PingContext(ctx); err != nil {
			return fmt.Errorf("migrate %s: connect: %w", set, err)
		}
		return nil
	}
	if err != nil {
		_ = db.Close() // the provider error is the one worth returning
		return fmt.Errorf("migrate %s: %w", set, err)
	}
	defer func() { _ = p.Close() }() // closes db; all work has already succeeded or failed
	if err := fn(p); err != nil {
		return fmt.Errorf("migrate %s: %w", set, err)
	}
	return nil
}
