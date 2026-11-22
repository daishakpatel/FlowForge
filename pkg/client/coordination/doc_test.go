package coordination_test

import (
	"context"
	"testing"
	"time"

	"github.com/daishakpatel/flowforge/internal/raftkv/kvtest"
	"github.com/daishakpatel/flowforge/internal/testutil"
	"github.com/daishakpatel/flowforge/pkg/client/coordination"
)

func TestMain(m *testing.M) { testutil.LeakCheck(m) }

// TestPublicAPI_TxnWithoutInternalImports uses only this package's names (no internal/gen import,
// which another module could not make) to run a Txn, as code outside the module would.
func TestPublicAPI_TxnWithoutInternalImports(t *testing.T) {
	c := kvtest.New(t, kvtest.Options{})
	c.WaitLeader(5 * time.Second)
	cl, err := coordination.New(coordination.Config{Endpoints: c.Endpoints(), BaseBackoff: 60 * time.Millisecond, MaxBackoff: 600 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cl.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := cl.Txn(ctx,
		[]*coordination.Compare{{Key: "k", Target: coordination.TargetVersion, Result: coordination.Equal, Operand: &coordination.CompareVersion{Version: 0}}},
		[]*coordination.Op{{Op: &coordination.OpPut{Put: &coordination.PutOp{Key: "k", Value: []byte("v")}}}}, nil)
	if err != nil || !res.GetSucceeded() {
		t.Fatalf("Txn = %v, %v", res, err)
	}
	version := func(kv *coordination.KeyValue) uint64 { return kv.GetVersion() } // the alias names the result type
	if v := version(res.GetResults()[0].GetPut()); v != 1 {
		t.Fatalf("written version = %d", v)
	}
}
