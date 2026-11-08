// Package dag is a lint fixture that breaks the boundary rules on purpose.
// scripts/dev/lint-selftest.sh copies it into a scratch module; normal lint skips testdata/.
package dag

import (
	"database/sql" // depguard: domain packages must not touch the database
	"os"
	"time"
)

// Violation exists only so lint-selftest.sh can prove the rules fire.
func Violation(db *sql.DB) time.Time {
	_ = db
	_ = os.Getenv("FF_ENV")    // forbidigo: read configuration through internal/config
	_ = time.NewTimer(0)       // forbidigo: timers come from an injected clock.Clock
	return time.Now()          // forbidigo: read time from an injected clock.Clock
}
