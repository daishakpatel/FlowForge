package testutil

import (
	"context"
	"testing"
	"time"

	"github.com/daishakpatel/flowforge/internal/persistence/migrate"
)

// ApplyMigrations applies migration set "flowforge" or "marketdata" to the database at dsn,
// usually a PG.DSN from NewPostgres or NewTimescale. It returns the error instead of failing
// the test, so tests can assert on failures too.
func ApplyMigrations(t testing.TB, dsn, set string) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	_, err := migrate.Up(ctx, dsn, migrate.Set(set))
	return err
}

// NewFlowforgeDB returns a fresh database with every flowforge migration applied, dropped when
// the test ends. Use it for repository and schema tests (integration build tag).
func NewFlowforgeDB(t testing.TB) *PG {
	t.Helper()
	db := NewPostgres(t)
	if t.Skipped() {
		return db
	}
	if err := ApplyMigrations(t, db.DSN, string(migrate.Flowforge)); err != nil {
		t.Fatalf("testutil: migrate %s: %v", db.Database, err)
	}
	return db
}
