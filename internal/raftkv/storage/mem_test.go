package storage_test

import (
	"testing"

	"github.com/daishakpatel/flowforge/internal/raftkv/storage"
	"github.com/daishakpatel/flowforge/internal/raftkv/storage/storagetest"
)

func TestMem_Conformance(t *testing.T) {
	storagetest.Run(t, func(*testing.T) storage.Storage { return storage.NewMem() })
}
