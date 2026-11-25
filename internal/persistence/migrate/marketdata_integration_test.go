//go:build integration

package migrate_test

import (
	"database/sql"
	"fmt"
	"io/fs"
	"testing"
	"time"

	"github.com/pressly/goose/v3"

	"github.com/daishakpatel/flowforge/internal/persistence/migrate"
	"github.com/daishakpatel/flowforge/internal/testutil"
	"github.com/daishakpatel/flowforge/migrations"
)

// The TimescaleDB marketdata schema (task 05.02, 00E §12).

func marketdataDB(t *testing.T) string {
	t.Helper()
	db := testutil.NewTimescale(t)
	if err := testutil.ApplyMigrations(t, db.DSN, string(migrate.Marketdata)); err != nil {
		t.Fatalf("migrate marketdata: %v", err)
	}
	return db.DSN
}

// TestMarketdata_UpFromEachPreviousVersion: every migration also applies on top of its predecessor.
func TestMarketdata_UpFromEachPreviousVersion(t *testing.T) {
	fsys, err := fs.Sub(migrations.FS, "marketdata")
	if err != nil {
		t.Fatal(err)
	}
	for prev := int64(1); prev <= 3; prev++ {
		t.Run(fmt.Sprintf("from_%d", prev), func(t *testing.T) {
			db := testutil.NewTimescale(t)
			sqlDB, err := sql.Open("pgx", db.DSN)
			if err != nil {
				t.Fatal(err)
			}
			p, err := goose.NewProvider(goose.DialectPostgres, sqlDB, fsys)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := p.UpTo(t.Context(), prev); err != nil {
				t.Fatalf("UpTo(%d): %v", prev, err)
			}
			_ = p.Close() // also closes sqlDB
			if v, err := migrate.Up(t.Context(), db.DSN, migrate.Marketdata); err != nil || v != 4 {
				t.Fatalf("Up from %d = %d, %v; want 4", prev, v, err)
			}
		})
	}
}

func TestMarketdata_HypertablesAndPolicies(t *testing.T) {
	conn := connect(t, marketdataDB(t))
	ctx := t.Context()

	rows, err := conn.Query(ctx, `SELECT hypertable_name FROM timescaledb_information.hypertables ORDER BY 1`)
	if err != nil {
		t.Fatal(err)
	}
	var hypertables []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		hypertables = append(hypertables, n)
	}
	if rows.Err() != nil || fmt.Sprint(hypertables) != "[market_aggregates market_ticks]" {
		t.Errorf("hypertables = %v, %v; want market_aggregates and market_ticks", hypertables, rows.Err())
	}

	// The partitioning column of market_ticks is event_time (the dedupe key needs it).
	var dim string
	if err := conn.QueryRow(ctx, `SELECT column_name FROM timescaledb_information.dimensions WHERE hypertable_name = 'market_ticks'`).Scan(&dim); err != nil || dim != "event_time" {
		t.Errorf("market_ticks dimension = %q, %v", dim, err)
	}

	for _, c := range []struct{ proc, key, want string }{
		{"policy_retention", "drop_after", "14 days"},
		{"policy_compression", "compress_after", "2 days"},
	} {
		var got string
		err := conn.QueryRow(ctx, `SELECT config->>$2 FROM timescaledb_information.jobs
			WHERE hypertable_name = 'market_ticks' AND proc_name = $1`, c.proc, c.key).Scan(&got)
		if err != nil || got != c.want {
			t.Errorf("%s %s = %q, %v; want %q", c.proc, c.key, got, err, c.want)
		}
	}
	var segmentBy string
	err = conn.QueryRow(ctx, `SELECT attname FROM timescaledb_information.compression_settings
		WHERE hypertable_name = 'market_ticks' AND segmentby_column_index = 1`).Scan(&segmentBy)
	if err != nil || segmentBy != "symbol" {
		t.Errorf("compression segment-by = %q, %v; want symbol", segmentBy, err)
	}

	for _, table := range []string{"market_ingest_state", "market_replay_jobs"} {
		var n int
		if err := conn.QueryRow(ctx, `SELECT count(*) FROM information_schema.tables WHERE table_name = $1`, table).Scan(&n); err != nil || n != 1 {
			t.Errorf("table %s: %d, %v", table, n, err)
		}
	}
}

const insertTick = `INSERT INTO market_ticks (event_time, symbol, event_id, price, size, source, sequence, ingested_at)
	VALUES ($1, $2, $3, $4, $5, 'synthetic', 1, now())`

func TestMarketdata_TickDedupeAndConstraints(t *testing.T) {
	conn := connect(t, marketdataDB(t))
	ctx := t.Context()
	// Event times an hour before the database's clock: well inside the 14-day retention and 2-day
	// compression windows, so the policies' background jobs never touch these rows mid-test.
	var at time.Time
	if err := conn.QueryRow(ctx, `SELECT date_trunc('milliseconds', now()) - interval '1 hour'`).Scan(&at); err != nil {
		t.Fatal(err)
	}

	if _, err := conn.Exec(ctx, insertTick, at, "ACME", "e1", "101.25", "300"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, insertTick, at, "ACME", "e1", "101.25", "300"); sqlState(err) != uniqueViolation {
		t.Errorf("duplicate (event_id, event_time): %v, want unique violation", err)
	}
	tag, err := conn.Exec(ctx, insertTick+` ON CONFLICT (event_id, event_time) DO NOTHING`, at, "ACME", "e1", "101.25", "300")
	if err != nil || tag.RowsAffected() != 0 {
		t.Errorf("ON CONFLICT DO NOTHING: %v rows, %v; want 0 (the processor counts it as a duplicate)", tag.RowsAffected(), err)
	}
	// The same event_id at another event_time is a different row (documented producer bug, 05.09).
	if _, err := conn.Exec(ctx, insertTick, at.Add(time.Second), "ACME", "e1", "101.25", "300"); err != nil {
		t.Errorf("same event_id, other event_time: %v", err)
	}

	// Prices are exact decimals.
	const precise = "123456789.123456789012345"
	if _, err := conn.Exec(ctx, insertTick, at, "ACME", "e2", precise, "0"); err != nil {
		t.Fatal(err)
	}
	var back string
	if err := conn.QueryRow(ctx, `SELECT price::text FROM market_ticks WHERE event_id = 'e2'`).Scan(&back); err != nil || back != precise {
		t.Errorf("price read back %q, %v; want %q exactly", back, err, precise)
	}

	for name, args := range map[string][]any{
		"zero price":    {at, "ACME", "e3", "0", "1"},
		"negative size": {at, "ACME", "e4", "1", "-1"},
		"bad symbol":    {at, "acme", "e5", "1", "1"},
	} {
		if _, err := conn.Exec(ctx, insertTick, args...); sqlState(err) != checkViolation {
			t.Errorf("%s: %v, want check violation", name, err)
		}
	}
}

func TestMarketdata_AggregateAndJobConstraints(t *testing.T) {
	conn := connect(t, marketdataDB(t))
	ctx := t.Context()
	const insertAgg = `INSERT INTO market_aggregates (symbol, window_size, window_start, open, high, low, close,
		volume, vwap, event_count, input_hash, status) VALUES ('ACME', $1, '2026-11-02T14:30:00Z', 1, 2, 1, 2, 10, 1.5, $2, 'h', $3)`
	if _, err := conn.Exec(ctx, insertAgg, "1m", 4, "open"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, insertAgg, "1m", 4, "final"); sqlState(err) != uniqueViolation {
		t.Errorf("second row for one window: %v, want unique violation (upsert on the key)", err)
	}
	if _, err := conn.Exec(ctx, insertAgg, "5m", 4, "final"); err != nil {
		t.Errorf("same start, other size: %v", err)
	}
	for name, args := range map[string][]any{
		"window size 1h": {"1h", 4, "open"},
		"zero count":     {"1m", 0, "open"},
		"bad status":     {"1m", 4, "closed"},
	} {
		if _, err := conn.Exec(ctx, insertAgg, args...); sqlState(err) != checkViolation {
			t.Errorf("%s: %v, want check violation", name, err)
		}
	}
	var version int64
	var status string
	if err := conn.QueryRow(ctx, `SELECT version, status FROM market_aggregates WHERE window_size = '1m'`).Scan(&version, &status); err != nil || version != 1 || status != "open" {
		t.Errorf("defaults: version %d status %q, %v", version, status, err)
	}

	const insertJob = `INSERT INTO market_replay_jobs (id, mode, spec, status) VALUES ($1, $2, '{}', $3)`
	if _, err := conn.Exec(ctx, insertJob, "rpl_1", "time_range", "pending"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, insertJob, "rpl_1", "time_range", "pending"); sqlState(err) != uniqueViolation {
		t.Errorf("re-submitted replay id: %v, want unique violation (idempotency key)", err)
	}
	for name, args := range map[string][]any{
		"bad mode":   {"rpl_2", "rewind", "pending"},
		"bad status": {"rpl_3", "offset", "paused"},
	} {
		if _, err := conn.Exec(ctx, insertJob, args...); sqlState(err) != checkViolation {
			t.Errorf("%s: %v, want check violation", name, err)
		}
	}

	const upsertState = `INSERT INTO market_ingest_state (consumer_group, topic, partition, last_offset, max_event_time)
		VALUES ('md-live', 'market.ticks', 2, $1, now()) ON CONFLICT (consumer_group, topic, partition)
		DO UPDATE SET last_offset = EXCLUDED.last_offset, updated_at = now()`
	for _, off := range []int64{10, 20} {
		if _, err := conn.Exec(ctx, upsertState, off); err != nil {
			t.Fatal(err)
		}
	}
	var last int64
	if err := conn.QueryRow(ctx, `SELECT last_offset FROM market_ingest_state`).Scan(&last); err != nil || last != 20 {
		t.Errorf("ingest state last_offset = %d, %v; want 20 (one row per partition)", last, err)
	}
}
