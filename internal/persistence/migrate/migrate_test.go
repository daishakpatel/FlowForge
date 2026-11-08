package migrate_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/daishakpatel/flowforge/internal/persistence/migrate"
)

const root = "../../.."

// TestMigrations_VerbatimFrom00E checks that each migration's Up section is exactly the matching
// part of the 00E §2 DDL block, so the schema can't drift from the contract (task 02.01 §4).
func TestMigrations_VerbatimFrom00E(t *testing.T) {
	t.Parallel()
	b, err := os.ReadFile(filepath.Join(root, "implementation/00E_CONTRACTS_REFERENCE.md"))
	if err != nil {
		t.Fatal(err)
	}
	doc := string(b)
	doc = doc[strings.Index(doc, "## 2. PostgreSQL"):strings.Index(doc, "## 3. State machines")]
	block := doc[strings.Index(doc, "```sql\n")+len("```sql\n"):]
	block = block[:strings.Index(block, "```")]

	header := regexp.MustCompile(`(?m)^-- (000\d_[a-z_]+\.sql)\n`)
	locs := header.FindAllStringSubmatchIndex(block, -1)
	if len(locs) != 4 {
		t.Fatalf("found %d migration sections in 00E §2, want 4", len(locs))
	}
	for i, loc := range locs {
		name := block[loc[2]:loc[3]]
		end := len(block)
		if i+1 < len(locs) {
			end = locs[i+1][0]
		}
		want := strings.TrimSpace(block[loc[1]:end])

		file, err := os.ReadFile(filepath.Join(root, "migrations/flowforge", name)) //nolint:gosec // G304: name is a 000N_*.sql header from the repo's own 00E
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		up := string(file)
		up = up[strings.Index(up, "-- +goose Up\n")+len("-- +goose Up\n") : strings.Index(up, "-- +goose Down")]
		if got := strings.TrimSpace(up); got != want {
			t.Errorf("%s differs from 00E §2", name)
		}
	}
}

func TestLatest(t *testing.T) {
	t.Parallel()
	if v, err := migrate.Latest(migrate.Flowforge); err != nil || v != 4 {
		t.Errorf("Latest(flowforge) = %d, %v; want 4", v, err)
	}
	if v, err := migrate.Latest(migrate.Marketdata); err != nil || v != 0 {
		t.Errorf("Latest(marketdata) = %d, %v; want 0 until 05.03", v, err)
	}
}

func TestUnknownSet_RejectedBeforeConnecting(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const dsn = "postgres://nobody@invalid.invalid/db"
	if _, err := migrate.Latest("aiops"); !errors.Is(err, migrate.ErrUnknownSet) {
		t.Errorf("Latest: %v", err)
	}
	if _, err := migrate.Up(ctx, dsn, "aiops"); !errors.Is(err, migrate.ErrUnknownSet) {
		t.Errorf("Up: %v", err)
	}
	if _, err := migrate.Status(ctx, dsn, ""); !errors.Is(err, migrate.ErrUnknownSet) {
		t.Errorf("Status: %v", err)
	}
	if _, err := migrate.Version(ctx, dsn, "FLOWFORGE"); !errors.Is(err, migrate.ErrUnknownSet) {
		t.Errorf("Version: %v", err)
	}
}
