package ids

import (
	"encoding/binary"
	"sync"

	"github.com/oklog/ulid/v2"
)

// Fake is a deterministic Generator for tests. It returns valid, increasing IDs with a zero
// timestamp and a counter as entropy: the first is <prefix>_00000000000000000000000001.
// It is safe for concurrent use.
type Fake struct {
	mu sync.Mutex
	n  uint64
}

var _ Generator = (*Fake)(nil)

// New returns the next deterministic ID.
func (f *Fake) New(prefix string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.n++
	var id ulid.ULID
	binary.BigEndian.PutUint64(id[8:], f.n)
	return prefix + "_" + id.String()
}
