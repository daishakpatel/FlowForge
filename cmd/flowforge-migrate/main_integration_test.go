//go:build integration

package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/daishakpatel/flowforge/internal/testutil"
)

func TestMain(m *testing.M) { testutil.LeakCheck(m) }

// TestRun_UpAndStatus drives the CLI as `flowforge-migrate --db flowforge up` and `status`
// against an empty database (task 02.01 acceptance: the command creates the schema).
func TestRun_UpAndStatus(t *testing.T) {
	pg := testutil.NewPostgres(t)
	md := testutil.NewTimescale(t)
	opts := envOpts(map[string]string{"FF_PG_DSN": pg.DSN, "FF_MD_PG_DSN": md.DSN})

	for _, tc := range []struct {
		args []string
		want []string
	}{
		{[]string{"--db", "flowforge", "status"}, []string{"pending", "0001_core.sql"}},
		{[]string{"--db", "flowforge", "up"}, []string{"flowforge: schema at version 4"}},
		{[]string{"up"}, []string{"flowforge: schema at version 4"}}, // default --db, second run is a no-op
		{[]string{"--db", "flowforge", "status"}, []string{"VERSION", "applied", "0004_triggers_approvals.sql"}},
		{[]string{"--db", "marketdata", "up"}, []string{"marketdata: schema at version 0"}},
		{[]string{"--db", "marketdata", "status"}, []string{"marketdata: no migrations"}},
	} {
		var stdout, stderr bytes.Buffer
		if err := run(t.Context(), tc.args, opts, &stdout, &stderr); err != nil {
			t.Fatalf("run(%v): %v\nstderr: %s", tc.args, err, stderr.String())
		}
		for _, w := range tc.want {
			if !strings.Contains(stdout.String(), w) {
				t.Errorf("run(%v) output %q does not contain %q", tc.args, stdout.String(), w)
			}
		}
		if strings.Contains(stdout.String()+stderr.String(), "postgres://") {
			t.Errorf("run(%v) printed a DSN", tc.args)
		}
	}
}
