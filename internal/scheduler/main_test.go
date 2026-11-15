package scheduler

import (
	"testing"

	"github.com/daishakpatel/flowforge/internal/testutil"
)

// Loops, the Runner and the integration tests' simulated executor all spawn goroutines; none may
// outlive the tests (00C §2.10).
func TestMain(m *testing.M) { testutil.LeakCheck(m) }
