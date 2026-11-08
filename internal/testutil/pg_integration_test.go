//go:build integration

package testutil

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/daishakpatel/flowforge/internal/persistence/migrate"
)

func connect(t *testing.T, dsn string) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(t.Context(), dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) }) // close errors don't affect the test
	return conn
}

func TestNewPostgres_ParallelDatabasesAreIsolated(t *testing.T) {
	for _, name := range []string{"a", "b"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			db := NewPostgres(t)
			conn := connect(t, db.DSN)
			if _, err := conn.Exec(t.Context(), "CREATE TABLE marker (owner text)"); err != nil {
				t.Fatalf("create table: %v", err)
			}
			if _, err := conn.Exec(t.Context(), "INSERT INTO marker VALUES ($1)", name); err != nil {
				t.Fatalf("insert: %v", err)
			}
			var owners []string
			rows, err := conn.Query(t.Context(), "SELECT owner FROM marker")
			if err != nil {
				t.Fatalf("select: %v", err)
			}
			owners, err = pgx.CollectRows(rows, pgx.RowTo[string])
			if err != nil {
				t.Fatalf("collect: %v", err)
			}
			if len(owners) != 1 || owners[0] != name {
				t.Fatalf("marker rows = %v, want only %q: another test's data is visible", owners, name)
			}
		})
	}
}

func TestNewPostgres_DistinctDatabasesDontShareData(t *testing.T) {
	a, b := NewPostgres(t), NewPostgres(t)
	if a.Database == b.Database || a.DSN == b.DSN {
		t.Fatalf("got the same database twice: %q", a.Database)
	}
	if _, err := connect(t, a.DSN).Exec(t.Context(), "CREATE TABLE only_in_a (id int)"); err != nil {
		t.Fatalf("create table in a: %v", err)
	}
	var exists bool
	err := connect(t, b.DSN).QueryRow(t.Context(), "SELECT to_regclass('only_in_a') IS NOT NULL").Scan(&exists)
	if err != nil {
		t.Fatalf("query b: %v", err)
	}
	if exists {
		t.Fatal("table created in database a is visible in database b")
	}
}

func TestNewPostgres_CleanupDropsDatabase(t *testing.T) {
	var name, admin string
	t.Run("create", func(t *testing.T) {
		db := NewPostgres(t)
		name, admin = db.Database, db.AdminDSN
	}) // the subtest's cleanup has run once t.Run returns

	var exists bool
	err := connect(t, admin).QueryRow(t.Context(), "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)", name).Scan(&exists)
	if err != nil {
		t.Fatalf("query pg_database: %v", err)
	}
	if exists {
		t.Fatalf("database %s still exists after the test that created it ended", name)
	}
}

func TestNewPostgres_VectorExtensionAvailable(t *testing.T) {
	db := NewPostgres(t)
	if _, err := connect(t, db.DSN).Exec(t.Context(), "CREATE EXTENSION vector"); err != nil {
		t.Fatalf("create extension vector: %v", err)
	}
}

func TestNewTimescale_ExtensionInstalled(t *testing.T) {
	db := NewTimescale(t)
	var version string
	err := connect(t, db.DSN).QueryRow(t.Context(), "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'").Scan(&version)
	if err != nil {
		t.Fatalf("timescaledb extension not installed in a new database: %v", err)
	}
	if version == "" {
		t.Fatal("timescaledb extension has an empty version")
	}
}

// TestNewFlowforgeDB_SchemaApplied replaces the 01.02 stub test now that migrations exist.
func TestNewFlowforgeDB_SchemaApplied(t *testing.T) {
	db := NewFlowforgeDB(t)
	var n int
	if err := connect(t, db.DSN).QueryRow(t.Context(), "SELECT count(*) FROM cluster_state").Scan(&n); err != nil || n != 1 {
		t.Fatalf("cluster_state rows = %d (%v), want the seeded row", n, err)
	}
	if err := ApplyMigrations(t, db.DSN, "flowforge"); err != nil {
		t.Fatalf("second ApplyMigrations should be a no-op: %v", err)
	}
	if err := ApplyMigrations(t, db.DSN, "nope"); !errors.Is(err, migrate.ErrUnknownSet) {
		t.Fatalf("unknown set: %v", err)
	}
}
