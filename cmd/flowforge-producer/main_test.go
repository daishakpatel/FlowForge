package main

import (
	"bytes"
	"testing"
	"time"

	"github.com/daishakpatel/flowforge/internal/clock"
)

func env(kv map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) { v, ok := kv[k]; return v, ok }
}

// TestRun_RejectsBadInputBeforeProducing: every case fails before a broker is contacted.
func TestRun_RejectsBadInputBeforeProducing(t *testing.T) {
	t.Parallel()
	base := map[string]string{"FF_PRODUCER_HTTP_ADDR": "127.0.0.1:0", "FF_KAFKA_BROKERS": "127.0.0.1:1"}
	cases := []struct {
		args []string
		env  map[string]string
		code int
	}{
		{[]string{"--nope"}, base, 2},
		{[]string{"extra"}, base, 2},
		{[]string{"--clock", "simulated"}, base, 2}, // unbounded simulated run
		{[]string{"--inject", "dup=1"}, base, 2},
		{[]string{"--start", "yesterday", "--count", "1"}, base, 2},
		{[]string{"--symbols", "acme", "--count", "1"}, base, 2},
		{[]string{"--count", "-1"}, base, 2},
		{[]string{"--clock", "warp", "--count", "1"}, base, 2},
		{[]string{"--file", "testdata/missing.csv"}, base, 2},
		{nil, map[string]string{"FF_PRODUCER_RATE": "0", "FF_PRODUCER_HTTP_ADDR": "127.0.0.1:0"}, 1},
		{nil, map[string]string{"FF_KAFKA_SECURITY_PROTOCOL": "SSL", "FF_PRODUCER_HTTP_ADDR": "127.0.0.1:0"}, 1},
	}
	clk := clock.NewFake(time.Date(2026, 11, 2, 14, 30, 0, 0, time.UTC))
	for _, tc := range cases {
		var out, errb bytes.Buffer
		if code := run(t.Context(), tc.args, env(tc.env), &out, &errb, clk); code != tc.code || errb.Len() == 0 {
			t.Errorf("%v %v: exit %d, stderr %q; want %d and a message", tc.args, tc.env, code, errb.String(), tc.code)
		}
	}
}

func TestRun_HelpPrintsFlagsAndExits0(t *testing.T) {
	t.Parallel()
	var out, errb bytes.Buffer
	clk := clock.NewFake(time.Date(2026, 11, 2, 14, 30, 0, 0, time.UTC))
	if code := run(t.Context(), []string{"-h"}, env(nil), &out, &errb, clk); code != 0 || !bytes.Contains(errb.Bytes(), []byte("-allow-invalid")) {
		t.Errorf("-h: exit %d, stderr %q; want 0 and the flag list", code, errb.String())
	}
}

func TestRun_RejectsMoreFlagMistakes(t *testing.T) {
	t.Parallel()
	base := map[string]string{"FF_PRODUCER_HTTP_ADDR": "127.0.0.1:0", "FF_KAFKA_BROKERS": "127.0.0.1:1"}
	clk := clock.NewFake(time.Date(2026, 11, 2, 14, 30, 0, 0, time.UTC))
	for _, args := range [][]string{
		{"--file", "../../testdata/marketdata/sample_ticks.csv", "--duration", "1h"}, // --duration needs event times it controls
		{"--clock", "simulated", "--duration", "360h"},                               // 15 days: the start would be past the tick retention
	} {
		var out, errb bytes.Buffer
		if code := run(t.Context(), args, env(base), &out, &errb, clk); code != 2 {
			t.Errorf("%v: exit %d (%s), want 2", args, code, errb.String())
		}
	}
}

// TestNewSource_SimulatedRunsEndAtNow: gaps push a simulated stream later, so it is cut at now
// rather than running into the future (where every tick would be rejected as future_timestamp).
func TestNewSource_SimulatedRunsEndAtNow(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 2, 14, 30, 0, 0, time.UTC)
	o, err := parseFlags([]string{"--clock", "simulated", "--count", "100000", "--rate", "100", "--inject", "gap=50%"}, 100)
	if err != nil {
		t.Fatal(err)
	}
	src, _, err := newSource(o, clock.NewFake(now))
	if err != nil {
		t.Fatal(err)
	}
	for {
		tk, err := src.Next(t.Context())
		if err != nil {
			break // io.EOF at now, possibly before 100 000 ticks
		}
		if tk.EventTime.After(now) {
			t.Fatalf("tick at %v, after now %v", tk.EventTime, now)
		}
	}
}
