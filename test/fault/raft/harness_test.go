//go:build fault

package raft_test

// The fault suite's harness (task 04.08): every scenario declares its injected fault and expected
// behaviour, runs a client load whose history Porcupine checks, measures time-to-recover as the
// first successful linearizable write after the fault, and appends one JSON row to the results
// file FF_FAULT_RESULTS (scripts/chaos/render-fault-report.go renders it).

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pc "github.com/anishathalye/porcupine"

	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/raftkv/client"
	"github.com/daishakpatel/flowforge/internal/raftkv/kvtest"
	"github.com/daishakpatel/flowforge/internal/testutil"
	"github.com/daishakpatel/flowforge/internal/testutil/porcupine"
)

// result is one scenario run, one JSON object per line in FF_FAULT_RESULTS.
type result struct {
	ID         string   `json:"id"` // 1..12 (v1.0 §8.7), X1..X5 (extra), N (negative control), C* (container level)
	Scenario   string   `json:"scenario"`
	Level      string   `json:"level"` // in-process | container
	Fault      string   `json:"fault"`
	Expected   string   `json:"expected"`
	Observed   string   `json:"observed"`
	Pass       bool     `json:"pass"`
	TTRSeconds *float64 `json:"ttr_seconds,omitempty"` // first successful linearizable write after the fault
	Target     string   `json:"target,omitempty"`      // e.g. "raft failover ≤ 3 s"
	TargetMet  *bool    `json:"target_met,omitempty"`
	Ops        int      `json:"ops"` // history size Porcupine checked
	At         string   `json:"at"`  // RFC 3339 UTC
}

// scenario is one fault scenario run.
type scenario struct {
	t        *testing.T
	row      result
	clk      clock.Clock
	rec      *porcupine.Recorder
	unknown  atomic.Int64
	prefix   string
	faultAt  time.Time
	observed []string
	checked  bool
	limit    time.Duration // the recovery target set by target; 0: none
}

func newScenario(t *testing.T, id, name, level, fault, expected string) *scenario {
	t.Helper()
	// The prefix is unique per scenario and per run: the Compose cluster keeps its data between runs,
	// and Porcupine's model starts every key absent, so a reused key would read as a violation.
	clk := clock.Real{}
	s := &scenario{t: t, clk: clk, rec: porcupine.NewRecorder(), prefix: fmt.Sprintf("s%s-%d/", strings.ToLower(id), clk.Now().UnixNano()),
		row: result{ID: id, Scenario: name, Level: level, Fault: fault, Expected: expected}}
	t.Cleanup(s.finish)
	return s
}

// observe adds a sentence to what the scenario observed.
func (s *scenario) observe(format string, args ...any) {
	s.observed = append(s.observed, fmt.Sprintf(format, args...))
	s.t.Logf("observed: "+format, args...)
}

// inject marks the fault's moment and runs it.
func (s *scenario) inject(fault func()) {
	s.faultAt = s.clk.Now()
	fault()
}

// recover writes through cl until a write succeeds and records the time since the fault as the
// time-to-recover; it fails the scenario if none succeeds within limit. Probe writes join the
// history.
func (s *scenario) recover(cl *client.Client, limit time.Duration) time.Duration {
	s.t.Helper()
	key := s.prefix + "probe"
	for i := 0; ; i++ {
		ctx, cancel := context.WithTimeout(s.t.Context(), time.Second)
		value := fmt.Sprintf("probe-%d", i)
		call := s.rec.Now()
		kv, err := cl.Put(ctx, key, []byte(value))
		s.rec.Record(probeClient, porcupine.Input{Op: porcupine.OpPut, Key: key, Value: value}, call,
			writeOutput(err, &s.unknown, porcupine.Output{Version: kv.GetVersion()}))
		cancel()
		ttr := s.clk.Now().Sub(s.faultAt)
		if err == nil {
			secs := ttr.Seconds()
			s.row.TTRSeconds = &secs
			return ttr
		}
		if ttr > limit {
			s.t.Fatalf("no write succeeded within %s of the fault: %v", limit, err)
		}
		_ = s.clk.Sleep(s.t.Context(), 20*time.Millisecond)
	}
}

// target declares the scenario's recovery target (00_MASTER §6.1). Call it before the fault: finish
// judges it from the time-to-recover, and a scenario that never recovered missed it. A miss is
// recorded, not failed: the report states it with analysis.
func (s *scenario) target(name string, limit time.Duration) {
	s.row.Target = fmt.Sprintf("%s ≤ %s", name, limit)
	s.limit = limit
}

// check runs Porcupine on the history; a violation keeps the history and its visualization in a
// directory the failure names (FF_FAULT_ARTIFACTS, else the temp dir).
func (s *scenario) check() {
	s.t.Helper()
	s.checked = true
	ops := s.rec.Operations()
	s.row.Ops = len(ops)
	dir := os.Getenv("FF_FAULT_ARTIFACTS")
	if dir == "" {
		dir = os.TempDir()
	}
	// Porcupine keeps a violation in a new directory under dir, so dir must exist: without it the
	// evidence of a violation would be lost.
	if err := os.MkdirAll(dir, 0o750); err != nil { //nolint:gosec // G703: the operator's FF_FAULT_ARTIFACTS path
		s.t.Fatalf("artifacts dir: %v", err)
	}
	res, kept := porcupine.Check(ops, dir)
	if res != pc.Ok {
		s.t.Fatalf("history of %d operations is %s; kept in %s", len(ops), res, kept)
	}
	s.observe("history of %d operations (%d writes with unknown outcome) is linearizable", len(ops), s.unknown.Load())
}

// converged waits until every listed member of c has applied the same index with the same state
// hash (scenario 10's check, run by every scenario).
func (s *scenario) converged(c *kvtest.Cluster, ids []uint64) {
	s.t.Helper()
	var hash [32]byte
	var index uint64
	ok := testutil.Eventually(func() bool {
		for i, id := range ids {
			h, idx := c.Member(id).FSM.StateHashAt()
			if i > 0 && (h != hash || idx != index) {
				return false
			}
			hash, index = h, idx
		}
		return true
	}, 30*time.Second, 20*time.Millisecond)
	if !ok {
		s.t.Fatalf("members %v did not converge to one state hash", ids)
	}
	s.observe("members %v converged: state hash %x… at applied index %d", ids, hash[:4], index)
}

// finish appends the row to FF_FAULT_RESULTS (when set). A scenario that did not reach its checks
// failed.
func (s *scenario) finish() {
	s.row.Pass = !s.t.Failed() && s.checked
	if s.row.Target != "" {
		met := s.row.TTRSeconds != nil && *s.row.TTRSeconds <= s.limit.Seconds()
		s.row.TargetMet = &met // no time-to-recover: it never recovered, a miss
	}
	s.row.Observed = strings.Join(s.observed, "; ")
	if s.t.Failed() {
		s.row.Observed += " (FAILED; see the test log)"
	}
	s.row.At = s.clk.Now().UTC().Format(time.RFC3339)
	path := os.Getenv("FF_FAULT_RESULTS")
	if path == "" {
		return
	}
	line, err := json.Marshal(s.row)
	if err != nil {
		s.t.Errorf("encode result: %v", err)
		return
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil { //nolint:gosec // G703: the operator's FF_FAULT_RESULTS path
		s.t.Errorf("results dir: %v", err)
		return
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600) //nolint:gosec // G304: the operator's results file
	if err != nil {
		s.t.Errorf("open results: %v", err)
		return
	}
	defer func() { _ = f.Close() }()
	if _, err := f.Write(append(line, '\n')); err != nil {
		s.t.Errorf("write result: %v", err)
	}
}

// probeClient is the recorder's client id for recover's probe writes.
const probeClient = 99

// load is a client workload on the scenario's keys; stop ends it and waits for every client.
type load struct {
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// startLoad runs n clients doing reads (40%), puts (40%) and CAS (20%) on four keys under the
// scenario's prefix, recording every operation, until stop.
func (s *scenario) startLoad(endpoints []string, n int) *load {
	ctx, cancel := context.WithCancel(context.Background())
	l := &load{cancel: cancel}
	keys := []string{s.prefix + "k0", s.prefix + "k1", s.prefix + "k2", s.prefix + "k3"}
	for i := range n {
		cl := s.client(endpoints, uint64(i)+1) //nolint:gosec // G115: small non-negative id; built here: t.Fatal only on the test goroutine
		l.wg.Go(func() { s.work(ctx, cl, i, keys) })
	}
	s.t.Cleanup(l.stop) // a scenario that fails early still stops its clients (stop is idempotent)
	return l
}

func (l *load) stop() {
	l.cancel()
	l.wg.Wait()
}

func (s *scenario) work(ctx context.Context, cl *client.Client, id int, keys []string) {
	defer func() { _ = cl.Close() }()
	rng := rand.New(rand.NewPCG(seed, uint64(id)+500)) //nolint:gosec // G115, G404: seeded workload
	seen := map[string]uint64{}
	for n := 0; ctx.Err() == nil; n++ {
		key := keys[rng.IntN(len(keys))]
		value := fmt.Sprintf("c%d-%d", id, n)
		switch p := rng.IntN(10); {
		case p < 4:
			get(ctx, cl, id, key, s.rec, seen)
		case p < 8:
			call := s.rec.Now()
			kv, err := cl.Put(ctx, key, []byte(value))
			s.rec.Record(id, porcupine.Input{Op: porcupine.OpPut, Key: key, Value: value}, call,
				writeOutput(err, &s.unknown, porcupine.Output{Version: kv.GetVersion()}))
			if err == nil {
				seen[key] = kv.GetVersion()
			}
		default:
			call := s.rec.Now()
			res, err := cl.CAS(ctx, key, seen[key], []byte(value))
			s.rec.Record(id, porcupine.Input{Op: porcupine.OpCAS, Key: key, Value: value, Expected: seen[key]}, call,
				writeOutput(err, &s.unknown, porcupine.Output{Succeeded: res.GetSucceeded(), Version: res.GetKv().GetVersion()}))
			if err == nil {
				seen[key] = res.GetKv().GetVersion()
			}
		}
	}
}

// client is a KV client with short backoffs, so recovery is measured, not waited out.
func (s *scenario) client(endpoints []string, stream uint64) *client.Client {
	s.t.Helper()
	cl, err := client.New(client.Config{
		Endpoints: endpoints, BaseBackoff: 20 * time.Millisecond, MaxBackoff: 200 * time.Millisecond,
		Rand: rand.New(rand.NewPCG(seed, stream)), //nolint:gosec // G404: seeded jitter
	})
	if err != nil {
		s.t.Fatal(err)
	}
	return cl
}

// run lets the load run for d.
func (s *scenario) run(d time.Duration) { _ = s.clk.Sleep(s.t.Context(), d) }
