package porcupine_test

import (
	"math"
	"testing"

	pc "github.com/anishathalye/porcupine"

	"github.com/daishakpatel/flowforge/internal/testutil/porcupine"
)

func op(client int, in porcupine.Input, call int64, out porcupine.Output, ret int64) pc.Operation {
	return pc.Operation{ClientId: client, Input: in, Call: call, Output: out, Return: ret}
}

func put(k, v string) porcupine.Input { return porcupine.Input{Op: porcupine.OpPut, Key: k, Value: v} }
func get(k string) porcupine.Input    { return porcupine.Input{Op: porcupine.OpGet, Key: k} }
func cas(k string, expected uint64, v string) porcupine.Input {
	return porcupine.Input{Op: porcupine.OpCAS, Key: k, Expected: expected, Value: v}
}

func found(v string, ver uint64) porcupine.Output {
	return porcupine.Output{Found: true, Value: v, Version: ver}
}

func TestModel(t *testing.T) {
	unknown := porcupine.Output{Unknown: true}
	never := int64(math.MaxInt64)
	cases := []struct {
		name string
		ops  []pc.Operation
		ok   bool
	}{
		{"sequential put then get", []pc.Operation{
			op(0, put("a", "1"), 0, porcupine.Output{Version: 1}, 1),
			op(0, get("a"), 2, found("1", 1), 3),
		}, true},
		{"read before any write sees nothing", []pc.Operation{
			op(0, get("a"), 0, porcupine.Output{}, 1),
		}, true},
		{"stale read after an acknowledged write", []pc.Operation{
			op(0, put("a", "1"), 0, porcupine.Output{Version: 1}, 1),
			op(1, get("a"), 2, porcupine.Output{}, 3),
		}, false},
		{"concurrent read may see either", []pc.Operation{
			op(0, put("a", "1"), 0, porcupine.Output{Version: 1}, 10),
			op(1, get("a"), 1, porcupine.Output{}, 2),
			op(2, get("a"), 3, found("1", 1), 4),
		}, true},
		{"reads can't go back in time", []pc.Operation{
			op(0, put("a", "1"), 0, porcupine.Output{Version: 1}, 10),
			op(1, get("a"), 1, found("1", 1), 2),
			op(2, get("a"), 3, porcupine.Output{}, 4),
		}, false},
		{"cas on the current version succeeds", []pc.Operation{
			op(0, put("a", "1"), 0, porcupine.Output{Version: 1}, 1),
			op(0, cas("a", 1, "2"), 2, porcupine.Output{Succeeded: true, Version: 2}, 3),
			op(0, get("a"), 4, found("2", 2), 5),
		}, true},
		{"cas on a stale version can't succeed", []pc.Operation{
			op(0, put("a", "1"), 0, porcupine.Output{Version: 1}, 1),
			op(0, put("a", "2"), 2, porcupine.Output{Version: 2}, 3),
			op(0, cas("a", 1, "3"), 4, porcupine.Output{Succeeded: true, Version: 3}, 5),
		}, false},
		{"failed cas reports the current version", []pc.Operation{
			op(0, put("a", "1"), 0, porcupine.Output{Version: 1}, 1),
			op(0, cas("a", 0, "x"), 2, porcupine.Output{Version: 1}, 3),
		}, true},
		{"cas with 0 creates an absent key", []pc.Operation{
			op(0, cas("a", 0, "x"), 0, porcupine.Output{Succeeded: true, Version: 1}, 1),
		}, true},
		{"unknown write may have applied", []pc.Operation{
			op(0, put("a", "1"), 0, unknown, never),
			op(1, get("a"), 5, found("1", 1), 6),
		}, true},
		{"unknown write may not have applied", []pc.Operation{
			op(0, put("a", "1"), 0, unknown, never),
			op(1, get("a"), 5, porcupine.Output{}, 6),
			op(1, put("a", "2"), 7, porcupine.Output{Version: 1}, 8),
		}, true},
		{"unknown write applies at most once", []pc.Operation{
			op(0, put("a", "1"), 0, unknown, never),
			op(1, get("a"), 5, found("1", 1), 6),
			op(1, get("a"), 7, porcupine.Output{}, 8),
		}, false},
		{"keys are independent", []pc.Operation{
			op(0, put("a", "1"), 0, porcupine.Output{Version: 1}, 1),
			op(1, get("b"), 2, porcupine.Output{}, 3),
		}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := pc.CheckOperations(porcupine.Model(), c.ops); got != c.ok {
				t.Fatalf("linearizable = %v, want %v", got, c.ok)
			}
		})
	}
}

func TestRecorder_UnknownWritesNeverReturn(t *testing.T) {
	r := porcupine.NewRecorder()
	call := r.Now()
	r.Record(3, put("a", "1"), call, porcupine.Output{Unknown: true})
	r.Record(4, get("a"), r.Now(), porcupine.Output{})
	ops := r.Operations()
	if len(ops) != 2 || ops[0].Return != math.MaxInt64 || ops[0].ClientId != 3 || ops[1].Return < ops[1].Call {
		t.Fatalf("operations = %+v", ops)
	}
}

func TestCheck_DumpsTheHistoryOfAViolation(t *testing.T) {
	ops := []pc.Operation{
		op(0, put("a", "1"), 0, porcupine.Output{Version: 1}, 1),
		op(1, get("a"), 2, porcupine.Output{}, 3),
	}
	res, dir := porcupine.Check(ops, t.TempDir())
	if res != pc.Illegal || dir == "" {
		t.Fatalf("Check = %v, %q", res, dir)
	}
}
