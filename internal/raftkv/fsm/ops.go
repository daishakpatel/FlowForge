package fsm

import (
	"bytes"
	"slices"

	pb "github.com/daishakpatel/flowforge/internal/gen/proto/coordination/v1"
)

// checkVersion compares an optional expected version with the key's. nil means the write may
// proceed. expected 0 means "must not exist"; an absent key with expected > 0 is KEY_NOT_FOUND.
func checkVersion(key string, cur *entry, expected *uint64) *pb.CommandResult {
	if expected == nil || *expected == cur.ver() {
		return nil
	}
	if cur == nil {
		return result(codeNoKey)
	}
	return &pb.CommandResult{Code: codeMismatch, Kv: kv(key, cur)}
}

func (s *state) put(index uint64, ts int64, key string, value []byte, expected *uint64, leaseID uint64) *pb.CommandResult {
	if c := checkKV(key, value); c != codeOK {
		return result(c)
	}
	cur := s.keys[key]
	if r := checkVersion(key, cur, expected); r != nil {
		return r
	}
	if leaseID != 0 {
		if c := s.leaseLive(leaseID, ts); c != codeOK {
			return result(c)
		}
	}
	delta := len(key) + len(value)
	if cur != nil {
		delta -= len(key) + len(cur.value)
	}
	if (cur == nil && len(s.keys) >= MaxKeys) || !s.fits(delta) {
		return result(codeQuota)
	}
	prev := kv(key, cur)
	s.write(index, key, value, leaseID)
	return &pb.CommandResult{Code: codeOK, Kv: kv(key, s.keys[key]), Prev: prev}
}

func (s *state) del(key string, expected *uint64) *pb.CommandResult {
	if c := checkKey(key); c != codeOK {
		return result(c)
	}
	cur := s.keys[key]
	if r := checkVersion(key, cur, expected); r != nil {
		return r
	}
	if cur == nil {
		return result(codeOK)
	}
	prev := kv(key, cur)
	s.remove(key)
	return &pb.CommandResult{Code: codeOK, Deleted: true, Prev: prev}
}

func (s *state) cas(index uint64, ts int64, r *pb.CASRequest) *pb.CommandResult {
	expected := r.GetExpectedVersion()
	switch a := r.GetAction().(type) {
	case *pb.CASRequest_Value:
		return s.put(index, ts, r.GetKey(), a.Value, &expected, r.GetLeaseId())
	case *pb.CASRequest_Delete:
		if expected == 0 || r.GetLeaseId() != 0 { // "delete if absent" and a lease mean nothing here
			return result(codeInvalid)
		}
		return s.del(r.GetKey(), &expected)
	default:
		return result(codeInvalid)
	}
}

// txn evaluates every compare, validates the chosen branch as a whole, then runs it: a Txn
// either runs every op of its branch or changes nothing.
func (s *state) txn(index uint64, ts int64, r *pb.TxnRequest) *pb.CommandResult {
	if len(r.GetCompare()) > MaxTxnCompares || len(r.GetSuccess())+len(r.GetFailure()) > MaxTxnOps {
		return result(codeTooLarge)
	}
	// Both branches must be well-formed whichever runs, so a malformed request fails the same way
	// in every state; lease liveness and quotas are checked for the branch that runs.
	for _, op := range append(slices.Clone(r.GetSuccess()), r.GetFailure()...) {
		if code := checkOp(op); code != codeOK {
			return result(code)
		}
	}
	held := true
	for _, c := range r.GetCompare() {
		ok, code := s.compare(c)
		if code != codeOK {
			return result(code)
		}
		held = held && ok
	}
	branch := r.GetFailure()
	if held {
		branch = r.GetSuccess()
	}
	if code := s.validateOps(branch, ts); code != codeOK {
		return result(code)
	}
	res := &pb.CommandResult{Code: codeOK, Succeeded: held}
	for _, op := range branch {
		res.Results = append(res.Results, s.runOp(index, ts, op))
	}
	return res
}

func (s *state) compare(c *pb.Compare) (bool, pb.ResultCode) {
	if checkKey(c.GetKey()) != codeOK {
		return false, codeInvalid
	}
	e := s.keys[c.GetKey()]
	var diff int
	switch op := c.GetOperand().(type) {
	case *pb.Compare_Version:
		if c.GetTarget() != pb.Compare_TARGET_VERSION {
			return false, codeInvalid
		}
		diff = cmpU(e.ver(), op.Version)
	case *pb.Compare_Value:
		if c.GetTarget() != pb.Compare_TARGET_VALUE {
			return false, codeInvalid
		}
		if e == nil {
			// An absent key has no value: no value compare holds (as in etcd), so "== empty"
			// can't be mistaken for "absent". Compare versions with 0 to test absence.
			return false, checkResult(c.GetResult())
		}
		diff = bytes.Compare(e.value, op.Value)
	case *pb.Compare_LeaseId:
		if c.GetTarget() != pb.Compare_TARGET_LEASE {
			return false, codeInvalid
		}
		var id uint64
		if e != nil {
			id = e.leaseID
		}
		diff = cmpU(id, op.LeaseId)
	default:
		return false, codeInvalid
	}
	if code := checkResult(c.GetResult()); code != codeOK {
		return false, code
	}
	switch c.GetResult() {
	case pb.Compare_RESULT_EQUAL:
		return diff == 0, codeOK
	case pb.Compare_RESULT_NOT_EQUAL:
		return diff != 0, codeOK
	case pb.Compare_RESULT_GREATER:
		return diff > 0, codeOK
	default: // RESULT_LESS
		return diff < 0, codeOK
	}
}

func checkResult(r pb.Compare_Result) pb.ResultCode {
	if r < pb.Compare_RESULT_EQUAL || r > pb.Compare_RESULT_LESS {
		return codeInvalid
	}
	return codeOK
}

// checkOp validates an op's shape and sizes, independent of the state.
func checkOp(op *pb.Op) pb.ResultCode {
	switch o := op.GetOp().(type) {
	case *pb.Op_Put:
		return checkKV(o.Put.GetKey(), o.Put.GetValue())
	case *pb.Op_Delete:
		return checkKey(o.Delete.GetKey())
	case *pb.Op_Get:
		return checkKey(o.Get.GetKey())
	default:
		return codeInvalid
	}
}

// validateOps checks every op of the branch that will run before any runs, simulating key
// creation and deletion so the key and byte quotas are checked as the ops would leave them.
func (s *state) validateOps(ops []*pb.Op, ts int64) pb.ResultCode {
	size := map[string]int{} // simulated len(key)+len(value); -1: deleted
	current := func(k string) int {
		if n, ok := size[k]; ok {
			return n
		}
		if e := s.keys[k]; e != nil {
			return len(k) + len(e.value)
		}
		return -1
	}
	exists := func(k string) bool { return current(k) >= 0 }
	count, bytes := len(s.keys), s.dataBytes
	for _, op := range ops {
		switch o := op.GetOp().(type) {
		case *pb.Op_Put:
			if c := checkKV(o.Put.GetKey(), o.Put.GetValue()); c != codeOK {
				return c
			}
			if id := o.Put.GetLeaseId(); id != 0 {
				if c := s.leaseLive(id, ts); c != codeOK {
					return c
				}
			}
			k := o.Put.GetKey()
			if !exists(k) {
				if count++; count > MaxKeys {
					return codeQuota
				}
			} else {
				bytes -= current(k)
			}
			size[k] = len(k) + len(o.Put.GetValue())
			if bytes += size[k]; bytes > MaxStateBytes {
				return codeQuota
			}
		case *pb.Op_Delete:
			if c := checkKey(o.Delete.GetKey()); c != codeOK {
				return c
			}
			if k := o.Delete.GetKey(); exists(k) {
				count--
				bytes -= current(k)
				size[k] = -1
			}
		case *pb.Op_Get:
			if c := checkKey(o.Get.GetKey()); c != codeOK {
				return c
			}
		default:
			return codeInvalid
		}
	}
	return codeOK
}

// runOp runs one op that validateOps accepted.
func (s *state) runOp(index uint64, ts int64, op *pb.Op) *pb.OpResult {
	switch o := op.GetOp().(type) {
	case *pb.Op_Put:
		r := s.put(index, ts, o.Put.GetKey(), o.Put.GetValue(), nil, o.Put.GetLeaseId())
		return &pb.OpResult{Result: &pb.OpResult_Put{Put: r.GetKv()}}
	case *pb.Op_Delete:
		return &pb.OpResult{Result: &pb.OpResult_Deleted{Deleted: s.del(o.Delete.GetKey(), nil).GetDeleted()}}
	default: // *pb.Op_Get
		k := op.GetGet().GetKey()
		if e := s.keys[k]; e != nil {
			return &pb.OpResult{Result: &pb.OpResult_Get{Get: kv(k, e)}}
		}
		return &pb.OpResult{}
	}
}

func cmpU(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}
