//go:build !failpoint

package failpoint

// Enabled reports whether this build carries failpoints (build tag `failpoint`).
const Enabled = false

// Inject does nothing in builds without the `failpoint` tag.
func Inject(string) error { return nil }
