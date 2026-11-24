//go:build integration

package pg

import (
	"testing"

	"github.com/daishakpatel/flowforge/internal/config"
	"github.com/daishakpatel/flowforge/internal/testutil"
)

// TestNewPool_SessionParam: a SessionParam applies to the pool's sessions (04.07: the scheduler
// bounds how long a frozen process can sit in a transaction holding the epoch lock).
func TestNewPool_SessionParam(t *testing.T) {
	db := testutil.NewPostgres(t)
	pool, err := NewPool(t.Context(), config.Secret(db.DSN), SessionParam("idle_in_transaction_session_timeout", "30s"))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var got string
	if err := pool.QueryRow(t.Context(), "SHOW idle_in_transaction_session_timeout").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != "30s" {
		t.Fatalf("idle_in_transaction_session_timeout = %q, want 30s", got)
	}
}
