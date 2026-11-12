package workerapi

import (
	"testing"

	"github.com/daishakpatel/flowforge/internal/testutil"
)

// The integration tests run a gRPC server, the LISTEN waker and many concurrent pollers; all of
// them must stop with their test (00C §2.10).
func TestMain(m *testing.M) { testutil.LeakCheck(m) }
