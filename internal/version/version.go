// Package version reports which build of FlowForge a binary is.
package version

// These are set at link time by the Makefile:
//
//	-ldflags "-X github.com/daishakpatel/flowforge/internal/version.version=<v> -X ….commit=<sha>"
//
// They are the only package-level variables allowed (00C §8): the linker can only set
// variables. They stay unexported so nothing can change them at run time.
var (
	version = "dev"
	commit  = "unknown"
)

// Version returns the release version, or "dev" for a build without ldflags.
func Version() string { return version }

// Commit returns the git commit the binary was built from, or "unknown".
func Commit() string { return commit }
