package testutil

import (
	"testing"

	"go.uber.org/goleak"
)

// LeakCheck runs the package's tests and fails them if goroutines are still running afterwards
// (00C §2.10). Call it from TestMain; it exits the process:
//
//	func TestMain(m *testing.M) { testutil.LeakCheck(m) }
//
// Pass goleak options, such as goleak.IgnoreTopFunction, for goroutines a dependency owns.
//
// The testcontainers Ryuk reaper's connection goroutine is always ignored: it lives as long as the
// test binary by design, so the reaper can remove the shared database containers when it exits.
func LeakCheck(m *testing.M, opts ...goleak.Option) {
	opts = append(opts, goleak.IgnoreAnyFunction("github.com/testcontainers/testcontainers-go.(*Reaper).connect.func1"))
	goleak.VerifyTestMain(m, opts...)
}
