package migrate

import (
	"errors"
	"fmt"
	"io/fs"

	"github.com/daishakpatel/flowforge/migrations"
)

// Set names a migration set: one database and its directory under migrations/.
type Set string

// The migration sets (ADR-013: one goose directory per database).
const (
	Flowforge  Set = "flowforge"
	Marketdata Set = "marketdata"
)

// ErrUnknownSet is returned for a Set other than Flowforge or Marketdata.
var ErrUnknownSet = errors.New("unknown migration set")

// source returns the embedded directory of set.
func source(set Set) (fs.FS, error) {
	switch set {
	case Flowforge, Marketdata:
		return fs.Sub(migrations.FS, string(set))
	default:
		return nil, fmt.Errorf("%w: %q (want %q or %q)", ErrUnknownSet, set, Flowforge, Marketdata)
	}
}
