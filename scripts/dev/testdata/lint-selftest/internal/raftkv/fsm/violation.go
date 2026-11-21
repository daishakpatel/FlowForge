// Package fsm is a lint fixture that breaks the Raft FSM determinism rules on purpose (04.03).
// scripts/dev/lint-selftest.sh copies it into a scratch module; normal lint skips testdata/.
package fsm

import (
	"math/rand" // depguard: the FSM uses no randomness
	"time"      // depguard: the FSM reads no clock
)

// Violation exists only so lint-selftest.sh can prove the rules fire.
func Violation() int64 { return time.Duration(rand.Int63()).Milliseconds() }
