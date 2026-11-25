package domain_test

import (
	"testing"
	"time"

	"github.com/daishakpatel/flowforge/internal/stream/domain"
)

func utc(s string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		panic(err)
	}
	return t
}

func TestWindowFor(t *testing.T) {
	t.Parallel()
	cases := []struct {
		at    string
		size  domain.WindowSize
		start string
	}{
		{"2026-11-02T14:30:00Z", domain.Window1m, "2026-11-02T14:30:00Z"},           // on a boundary: its own window
		{"2026-11-02T14:30:59.999999999Z", domain.Window1m, "2026-11-02T14:30:00Z"}, // half-open: end excluded
		{"2026-11-02T14:34:59.999Z", domain.Window5m, "2026-11-02T14:30:00Z"},       //
		{"2026-11-02T14:35:00Z", domain.Window5m, "2026-11-02T14:35:00Z"},           //
		{"2026-11-02T09:32:10-05:00", domain.Window5m, "2026-11-02T14:30:00Z"},      // other zones map to UTC boundaries
		{"2026-11-02T20:02:10+05:30", domain.Window5m, "2026-11-02T14:30:00Z"},      // a half-hour offset too
		{"1970-01-01T00:00:00Z", domain.Window1m, "1970-01-01T00:00:00Z"},           // the epoch
		{"1969-12-31T23:59:59.999Z", domain.Window1m, "1969-12-31T23:59:00Z"},       // before the epoch rounds down, not toward zero
		{"1969-12-31T23:57:30Z", domain.Window5m, "1969-12-31T23:55:00Z"},           //
		{"2016-12-31T23:59:59Z", domain.Window1m, "2016-12-31T23:59:00Z"},           // a leap-second day: Go time has no leap seconds
		{"2017-01-01T00:00:00Z", domain.Window1m, "2017-01-01T00:00:00Z"},           //
		{"2026-03-08T07:02:00Z", domain.Window5m, "2026-03-08T07:00:00Z"},           // a US DST switch instant: UTC is unaffected
	}
	for _, tc := range cases {
		w, err := domain.WindowFor(utc(tc.at), tc.size)
		if err != nil {
			t.Fatalf("%s %s: %v", tc.at, tc.size, err)
		}
		if !w.Start.Equal(utc(tc.start)) || w.Start.Location() != time.UTC || w.Size != tc.size {
			t.Errorf("WindowFor(%s, %s) = %v %s, want start %s UTC", tc.at, tc.size, w.Start, w.Size, tc.start)
		}
		if !w.Contains(utc(tc.at)) || w.Contains(w.End()) || !w.Contains(w.Start) {
			t.Errorf("window %v-%v: half-open containment wrong for %s", w.Start, w.End(), tc.at)
		}
		if w.End().Sub(w.Start) != tc.size.Duration() {
			t.Errorf("window length %v, want %v", w.End().Sub(w.Start), tc.size.Duration())
		}
	}
}

func TestWindowSize(t *testing.T) {
	t.Parallel()
	if domain.Window1m.Duration() != time.Minute || domain.Window5m.Duration() != 5*time.Minute {
		t.Error("window durations")
	}
	for _, s := range []string{"1m", "5m"} {
		if got, err := domain.ParseWindowSize(s); err != nil || string(got) != s {
			t.Errorf("ParseWindowSize(%q) = %q, %v", s, got, err)
		}
	}
	for _, s := range []string{"", "1h", "60s", "1M"} {
		if _, err := domain.ParseWindowSize(s); err == nil {
			t.Errorf("ParseWindowSize(%q) accepted", s)
		}
		if _, err := domain.WindowFor(now, domain.WindowSize(s)); err == nil {
			t.Errorf("WindowFor with size %q accepted", s)
		}
	}
	if got := domain.WindowSizes(); len(got) != 2 || got[0] != domain.Window1m || got[1] != domain.Window5m {
		t.Errorf("WindowSizes = %v", got)
	}
}
