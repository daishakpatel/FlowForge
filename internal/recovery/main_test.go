package recovery

import (
	"testing"

	"github.com/daishakpatel/flowforge/internal/testutil"
)

// The tests run gRPC servers and clients, pools and loops in-process; none of their goroutines
// may outlive the tests (00C §2.10).
func TestMain(m *testing.M) { testutil.LeakCheck(m) }
