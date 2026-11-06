// Package dag is a lint fixture that breaks the boundary rules on purpose.
// scripts/dev/lint-selftest.sh copies it into a scratch module; normal lint skips testdata/.
package dag

import (
	"database/sql" // depguard: domain packages must not touch the database
	"time"
)

// Violation exists only so lint-selftest.sh can prove the rules fire.
func Violation(db *sql.DB) time.Time {
	_ = db
	return time.Now() // forbidigo: read time from an injected clock.Clock
}
