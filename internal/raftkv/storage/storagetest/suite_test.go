package storagetest_test

import (
	"testing"

	"github.com/daishakpatel/flowforge/internal/raftkv/storage"
	"github.com/daishakpatel/flowforge/internal/raftkv/storage/storagetest"
)

// TestRun_AgainstTheReference runs the suite against Mem, which wraps the raft library's own
// MemoryStorage: the suite must accept the library's semantics.
func TestRun_AgainstTheReference(t *testing.T) {
	storagetest.Run(t, func(*testing.T) storage.Storage { return storage.NewMem() })
}
