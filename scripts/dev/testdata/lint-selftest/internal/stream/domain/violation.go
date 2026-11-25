// Package domain is a lint fixture that breaks the stream domain's purity rules on purpose (05.02).
// scripts/dev/lint-selftest.sh copies it into a scratch module; normal lint skips testdata/.
package domain

import (
	"math/rand" // depguard: the stream domain uses no randomness
	"net"       // depguard: the stream domain does no I/O
)

// Violation exists only so lint-selftest.sh can prove the rules fire.
func Violation() string { return net.JoinHostPort("h", string(rune('0'+rand.Intn(9)))) }
