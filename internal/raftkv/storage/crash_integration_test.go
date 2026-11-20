//go:build integration

package storage_test

import (
	"bytes"
	"flag"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/daishakpatel/flowforge/internal/raftkv/storage"
	st "github.com/daishakpatel/flowforge/internal/raftkv/storage/storagetest"
)

var crashChild = flag.String("raftkv.crash-child", "", "run as the crash test's child, writing to this storage file")

const (
	childSaves = 10_000
	crashKills = 50
	crashSeed  = 20261119
)

// TestCrashChild is the child process of TestBolt_CrashSafety; without -raftkv.crash-child it
// does nothing. It continues the log it finds with childSaves Saves of 1–5 entries, each with
// a hard state whose commit is the new last index, and each time the log passes a multiple of
// 200 takes a snapshot and compacts, until it is killed.
func TestCrashChild(t *testing.T) {
	if *crashChild == "" {
		t.Skip("only runs as TestBolt_CrashSafety's child")
	}
	s, err := storage.OpenBolt(*crashChild, node1)
	if err != nil {
		t.Fatal(err)
	}
	last, _ := s.LastIndex()
	hs, _, _ := s.InitialState()
	term := hs.GetTerm() + 1                      // a new term per incarnation, like a restarted node
	rng := rand.New(rand.NewPCG(crashSeed, last)) //nolint:gosec // test data, not security
	for range childSaves {
		start := last
		ents := make([]*pb.Entry, 1+rng.IntN(5))
		for j := range ents {
			last++
			ents[j] = &pb.Entry{Index: proto.Uint64(last), Term: proto.Uint64(term), Data: make([]byte, rng.IntN(512))}
		}
		if err := s.Save(st.HardState(term, 1, last), ents, nil); err != nil {
			t.Fatal(err)
		}
		if last/200 > start/200 {
			snap, _ := s.LoadSnapshot()
			if last-10 > snap.GetMetadata().GetIndex() {
				at, err := s.Term(last - 10) // may be an earlier incarnation's term
				if err != nil {
					t.Fatal(err)
				}
				if err := s.SaveSnapshot(st.Snapshot(last-10, at, []byte("fsm"))); err != nil {
					t.Fatal(err)
				}
				if err := s.Compact(last - 20); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
}

// TestBolt_CrashSafety SIGKILLs a child running Saves at crashKills random moments. After each
// kill the file must open (its invariants hold), the log must be contiguous, and the hard state
// must match the last entry: the child writes them in the same Save, so a commit index that
// disagrees with LastIndex would mean a torn Save.
func TestBolt_CrashSafety(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.db")
	rng := rand.New(rand.NewPCG(crashSeed, 0)) //nolint:gosec // kill timing, not security
	t.Logf("seed %d", crashSeed)
	var prev uint64
	grew := 0
	for kill := range crashKills {
		cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestCrashChild$", "-raftkv.crash-child="+path) //nolint:gosec // this test binary
		var out bytes.Buffer
		cmd.Stdout, cmd.Stderr = &out, &out
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Duration(20+rng.IntN(230)) * time.Millisecond) //nolint:forbidigo // real time: killing a real process mid-write
		_ = cmd.Process.Kill()                                         // SIGKILL; it may have finished already
		_ = cmd.Wait()                                                 // judged from ProcessState below
		if ps := cmd.ProcessState; ps.Exited() && ps.ExitCode() != 0 {
			t.Fatalf("kill %d: the child failed on its own (exit %d):\n%s", kill, ps.ExitCode(), out.String())
		}

		s, err := storage.OpenBolt(path, node1)
		if err != nil {
			t.Fatalf("kill %d: reopen: %v", kill, err)
		}
		first, _ := s.FirstIndex()
		last, _ := s.LastIndex()
		hs, _, _ := s.InitialState()
		if last > 0 && hs.GetCommit() != last {
			t.Fatalf("kill %d: commit %d but last index %d: a Save was torn", kill, hs.GetCommit(), last)
		}
		if last >= first {
			ents, err := s.Entries(first, last+1, 1<<62)
			if err != nil || uint64(len(ents)) != last-first+1 || ents[len(ents)-1].GetIndex() != last ||
				ents[len(ents)-1].GetTerm() > hs.GetTerm() {
				t.Fatalf("kill %d: log [%d, %d] read back %d entries, err %v", kill, first, last, len(ents), err)
			}
		}
		if last > prev {
			grew++
		}
		prev = last
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
	s := openBolt(t, path)
	first, _ := s.FirstIndex()
	_ = s.Close()
	t.Logf("%d kills, the log grew between %d of them, log [%d, %d]", crashKills, grew, first, prev)
	if grew < crashKills/2 || first == 1 {
		t.Fatalf("the child grew the log before only %d of %d kills and compacted to %d; the test did not exercise Save and Compact", grew, crashKills, first-1)
	}
}
