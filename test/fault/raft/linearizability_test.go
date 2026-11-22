// Package raft_test checks the Raft KV for linearizability under leader kills (task 04.04,
// FR-RAFT-06, 00C §11 "Porcupine: Raft KV paths").
package raft_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
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

func TestMain(m *testing.M) { testutil.LeakCheck(m) }

const (
	clients = 8
	seed    = 4404
)

var keys = []string{"k0", "k1", "k2", "k3"}

// TestLinearizability_UnderLeaderKills runs 8 clients doing Put, Get and CAS on 4 keys for 60 s
// (5 s with -short) while the leader is killed every 2–4 s and restarted a second later, then
// checks the history with Porcupine. A write whose outcome a client never learned is recorded
// as unknown (it may apply once, or not at all). On a violation the history and Porcupine's
// visualization are kept in a directory the failure names.
func TestLinearizability_UnderLeaderKills(t *testing.T) {
	duration, minOps := 60*time.Second, 20_000
	if testing.Short() {
		duration, minOps = 5*time.Second, 0
	}
	t.Logf("seed %d, %s", seed, duration)
	c := kvtest.New(t, kvtest.Options{ProposalTimeout: 500 * time.Millisecond})
	c.WaitLeader(5 * time.Second)
	rec := porcupine.NewRecorder()
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	var wg sync.WaitGroup
	var unknown atomic.Int64
	for i := range clients {
		wg.Go(func() { work(ctx, t, c.Endpoints(), i, rec, &unknown) })
	}
	kills := killLeaders(ctx, c, rand.New(rand.NewPCG(seed, 0))) //nolint:gosec // G404: seeded chaos schedule
	wg.Wait()

	ops := rec.Operations()
	t.Logf("%d operations (%d writes with unknown outcome), %d leader kills", len(ops), unknown.Load(), kills)
	if len(ops) < minOps {
		t.Fatalf("only %d operations, want ≥ %d", len(ops), minOps)
	}
	if kills == 0 {
		t.Fatal("no leader was killed")
	}
	res, dir := porcupine.Check(ops, os.TempDir())
	if res != pc.Ok {
		t.Fatalf("history is %s; history and visualization kept in %s", res, dir)
	}
}

// killLeaders kills the leader every 2–4 s and restarts it a second later, until ctx ends; then
// it restarts every member. It returns how many leaders it killed.
func killLeaders(ctx context.Context, c *kvtest.Cluster, rng *rand.Rand) int {
	clk := clock.Real{}
	kills := 0
	for clk.Sleep(ctx, 2*time.Second+time.Duration(rng.Int64N(int64(2*time.Second)))) == nil {
		lead := c.Leader()
		if lead == 0 {
			continue
		}
		c.Kill(lead)
		kills++
		_ = clk.Sleep(ctx, time.Second) // cut short at the end: the restart below still runs
		c.Start(lead)                   //nolint:contextcheck // kvtest re-binds the port with its own bounded retry
	}
	for _, id := range c.IDs() {
		c.Start(id) //nolint:contextcheck // as above
	}
	return kills
}

// work is one client: a random mix of reads (40%), puts (40%) and CAS on the last version it saw
// (20%), until ctx ends.
func work(ctx context.Context, t *testing.T, endpoints []string, id int, rec *porcupine.Recorder, unknown *atomic.Int64) {
	cl, err := client.New(client.Config{
		Endpoints: endpoints, BaseBackoff: 60 * time.Millisecond, MaxBackoff: 600 * time.Millisecond,
		Rand: rand.New(rand.NewPCG(seed, uint64(id)+1)), //nolint:gosec // G115: small non-negative id
	})
	if err != nil {
		t.Error(err)
		return
	}
	defer func() { _ = cl.Close() }()
	rng := rand.New(rand.NewPCG(seed, uint64(id)+100)) //nolint:gosec // G115: as above
	seen := map[string]uint64{}
	for n := 0; ctx.Err() == nil; n++ {
		key := keys[rng.IntN(len(keys))]
		value := fmt.Sprintf("c%d-%d", id, n)
		switch p := rng.IntN(10); {
		case p < 4:
			get(ctx, cl, id, key, rec, seen)
		case p < 8:
			in := porcupine.Input{Op: porcupine.OpPut, Key: key, Value: value}
			call := rec.Now()
			kv, err := cl.Put(ctx, key, []byte(value))
			rec.Record(id, in, call, writeOutput(err, unknown, porcupine.Output{Version: kv.GetVersion()}))
			if err == nil {
				seen[key] = kv.GetVersion()
			}
		default:
			in := porcupine.Input{Op: porcupine.OpCAS, Key: key, Value: value, Expected: seen[key]}
			call := rec.Now()
			res, err := cl.CAS(ctx, key, seen[key], []byte(value))
			rec.Record(id, in, call, writeOutput(err, unknown, porcupine.Output{Succeeded: res.GetSucceeded(), Version: res.GetKv().GetVersion()}))
			if err == nil {
				seen[key] = res.GetKv().GetVersion()
			}
		}
	}
}

// get records a linearizable read; a failed read changes nothing and is left out.
func get(ctx context.Context, cl *client.Client, id int, key string, rec *porcupine.Recorder, seen map[string]uint64) {
	call := rec.Now()
	res, err := cl.Get(ctx, key)
	switch {
	case errors.Is(err, client.ErrNotFound):
		rec.Record(id, porcupine.Input{Op: porcupine.OpGet, Key: key}, call, porcupine.Output{})
		seen[key] = 0
	case err == nil:
		kv := res.GetKv()
		rec.Record(id, porcupine.Input{Op: porcupine.OpGet, Key: key}, call, porcupine.Output{Found: true, Value: string(kv.GetValue()), Version: kv.GetVersion()})
		seen[key] = kv.GetVersion()
	}
}

// writeOutput is ok for a write that returned, or Unknown for one that failed: the client gives
// up only after retries, so the write may have applied (once, thanks to its request id).
func writeOutput(err error, unknown *atomic.Int64, ok porcupine.Output) porcupine.Output {
	if err != nil {
		unknown.Add(1)
		return porcupine.Output{Unknown: true}
	}
	return ok
}
