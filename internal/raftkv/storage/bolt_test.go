package storage_test

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"go.etcd.io/bbolt"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/daishakpatel/flowforge/internal/raftkv/storage"
	st "github.com/daishakpatel/flowforge/internal/raftkv/storage/storagetest"
)

var node1 = storage.Identity{ClusterID: 7, NodeID: 1}

func openBolt(t testing.TB, path string) *storage.Bolt {
	t.Helper()
	b, err := storage.OpenBolt(path, node1)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestBolt_Conformance(t *testing.T) {
	st.Run(t, func(t *testing.T) storage.Storage { return openBolt(t, filepath.Join(t.TempDir(), "raft.db")) })
}

// TestBolt_ReopenKeepsEverything: hard state, log, snapshot, compaction and ConfState survive a
// close and reopen.
func TestBolt_ReopenKeepsEverything(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.db")
	b := openBolt(t, path)
	ents := st.Entries(1, 1, 1, 2, 2, 3, 3)
	st.MustSave(t, b, st.HardState(3, 2, 5), ents, nil)
	snap := st.Snapshot(4, 2, []byte("state at 4"))
	if err := b.SaveSnapshot(snap); err != nil {
		t.Fatal(err)
	}
	if err := b.Compact(3); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	b = openBolt(t, path)
	defer func() { _ = b.Close() }()
	st.Bounds(t, b, 4, 6)
	st.TermIs(t, b, 3, 2, nil)
	st.EntriesEqual(t, b, 4, 7, 1<<20, ents[3:])
	hs, cs, err := b.InitialState()
	if err != nil || !proto.Equal(hs, st.HardState(3, 2, 5)) || !st.ConfStateEqual(cs, snap.GetMetadata().GetConfState()) {
		t.Fatalf("initial state %v, %v, %v", hs, cs, err)
	}
	if got, err := b.LoadSnapshot(); err != nil || !st.SnapshotEqual(got, snap) {
		t.Fatalf("snapshot %v, %v", got, err)
	}
}

func TestBolt_RefusesAnotherIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.db")
	if err := openBolt(t, path).Close(); err != nil {
		t.Fatal(err)
	}
	for _, id := range []storage.Identity{{ClusterID: 7, NodeID: 2}, {ClusterID: 8, NodeID: 1}} {
		if _, err := storage.OpenBolt(path, id); !errors.Is(err, storage.ErrIdentityMismatch) {
			t.Errorf("open as %+v: %v, want ErrIdentityMismatch", id, err)
		}
	}
	if _, err := storage.OpenBolt(path, storage.Identity{ClusterID: 7}); err == nil {
		t.Error("a zero node id was accepted")
	}
	b := openBolt(t, path) // the right identity still opens
	_ = b.Close()
}

func TestBolt_RejectsAFileThatIsNotBolt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.db")
	if err := os.WriteFile(path, make([]byte, 8192), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := storage.OpenBolt(path, node1); err == nil {
		t.Fatal("opened a file of zeros")
	}
}

// TestBolt_DetectsCorruption damages a valid file directly through bbolt and expects
// ErrCorruptStorage at the next open.
func TestBolt_DetectsCorruption(t *testing.T) {
	key := func(i uint64) []byte { return binary.BigEndian.AppendUint64(nil, i) }
	marshal := func(m proto.Message) []byte {
		raw, err := proto.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	for _, tc := range []struct {
		name, want string // want: a piece of the reported problem
		damage     func(tx *bbolt.Tx) error
	}{
		{"gap in the indexes", "where 3 was expected", func(tx *bbolt.Tx) error { return tx.Bucket([]byte("entries")).Delete(key(3)) }},
		{"term goes down", "has term 1 after term 2", func(tx *bbolt.Tx) error {
			return tx.Bucket([]byte("entries")).Put(key(4), marshal(st.Entry(4, 1)))
		}},
		{"entry under the wrong key", "says index 6", func(tx *bbolt.Tx) error {
			return tx.Bucket([]byte("entries")).Put(key(5), marshal(st.Entry(6, 3)))
		}},
		{"undecodable entry", "entry 5:", func(tx *bbolt.Tx) error { return tx.Bucket([]byte("entries")).Put(key(5), []byte{0xff, 0xff}) }},
		{"commit past the last index", "past the last index", func(tx *bbolt.Tx) error {
			return tx.Bucket([]byte("meta")).Put([]byte("hardstate"), marshal(st.HardState(3, 1, 9)))
		}},
		{"hard state older than the log", "older than the last entry", func(tx *bbolt.Tx) error {
			return tx.Bucket([]byte("meta")).Put([]byte("hardstate"), marshal(st.HardState(1, 1, 2)))
		}},
		{"commit before the compaction marker", "before the compaction marker", func(tx *bbolt.Tx) error {
			// A valid snapshot and marker at 3 with entries 1..3 gone; only the commit (2) is wrong.
			for i := uint64(1); i <= 3; i++ {
				if err := tx.Bucket([]byte("entries")).Delete(key(i)); err != nil {
					return err
				}
			}
			md := st.Snapshot(3, 2, nil).GetMetadata()
			if err := tx.Bucket([]byte("snap")).Put([]byte("meta"), marshal(md)); err != nil {
				return err
			}
			if err := tx.Bucket([]byte("meta")).Put([]byte("compacted"), append(key(3), key(2)...)); err != nil {
				return err
			}
			return tx.Bucket([]byte("meta")).Put([]byte("hardstate"), marshal(st.HardState(3, 1, 2)))
		}},
		{"compacted past the snapshot", "past the latest snapshot", func(tx *bbolt.Tx) error {
			return tx.Bucket([]byte("meta")).Put([]byte("compacted"), append(key(2), key(1)...))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "raft.db")
			b := openBolt(t, path)
			st.MustSave(t, b, st.HardState(3, 1, 5), st.Entries(1, 1, 1, 2, 2, 3), nil)
			if err := b.Close(); err != nil {
				t.Fatal(err)
			}
			db, err := bbolt.Open(path, 0o600, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := db.Update(tc.damage); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := storage.OpenBolt(path, node1); !errors.Is(err, storage.ErrCorruptStorage) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("open: %v, want ErrCorruptStorage about %q", err, tc.want)
			}
		})
	}
}

// BenchmarkBolt_Save100 measures one fsynced Save of 100 entries of 128 bytes plus a hard state:
// the cost of persisting one busy Ready.
func BenchmarkBolt_Save100(b *testing.B) {
	s := openBolt(b, filepath.Join(b.TempDir(), "raft.db"))
	defer func() { _ = s.Close() }()
	data := make([]byte, 128)
	next := uint64(1)
	for b.Loop() {
		ents := make([]*pb.Entry, 100)
		for i := range ents {
			ents[i] = &pb.Entry{Index: proto.Uint64(next), Term: proto.Uint64(1), Data: data}
			next++
		}
		if err := s.Save(st.HardState(1, 1, next-1), ents, nil); err != nil {
			b.Fatal(err)
		}
	}
}

// TestBolt_ReadersDuringWrites: peer senders read Entries while the Ready loop saves; every read
// sees a whole, contiguous log (bbolt read transactions see one committed state).
func TestBolt_ReadersDuringWrites(t *testing.T) {
	s := openBolt(t, filepath.Join(t.TempDir(), "raft.db"))
	defer func() { _ = s.Close() }()
	done := make(chan struct{})
	errs := make(chan error, 4)
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for {
				select {
				case <-done:
					return
				default:
				}
				last, err := s.LastIndex()
				if err != nil || last == 0 {
					continue
				}
				ents, err := s.Entries(1, last+1, 1<<62)
				if err != nil || uint64(len(ents)) != last || ents[len(ents)-1].GetIndex() != last {
					errs <- fmt.Errorf("read [1, %d]: %d entries, err %w", last, len(ents), err)
					return
				}
			}
		})
	}
	for i := uint64(0); i < 200; i++ {
		st.MustSave(t, s, st.HardState(1, 1, 2*i+2), st.Entries(2*i+1, 1, 1), nil)
	}
	close(done)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}
