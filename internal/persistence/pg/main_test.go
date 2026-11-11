package pg

import (
	"testing"

	"github.com/daishakpatel/flowforge/internal/testutil"
)

// The integration tests run concurrent transactions over a pgx pool; every pool is closed in
// t.Cleanup, so nothing may outlive the tests (00C §2.10).
func TestMain(m *testing.M) { testutil.LeakCheck(m) }
