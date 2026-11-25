package domain_test

import (
	"testing"
	"time"

	"github.com/daishakpatel/flowforge/internal/stream/domain"
)

func TestWatermark(t *testing.T) {
	t.Parallel()
	maxEvent := utc("2026-11-02T14:31:20Z")
	if got := domain.Watermark(maxEvent, domain.DefaultAllowedLateness); !got.Equal(utc("2026-11-02T14:30:50Z")) {
		t.Errorf("Watermark = %v, want max event time - 30 s", got)
	}
	if domain.DefaultAllowedLateness != 30*time.Second || domain.DefaultMaxLateness != 10*time.Minute {
		t.Errorf("defaults %v %v, want 30s 10m", domain.DefaultAllowedLateness, domain.DefaultMaxLateness)
	}
}

func TestClassify(t *testing.T) {
	t.Parallel()
	wm := utc("2026-11-02T14:45:00Z")
	cases := []struct {
		end  string
		want domain.Lateness
	}{
		{"2026-11-02T14:46:00Z", domain.OnTime},        // the window is still open
		{"2026-11-02T14:45:00.001Z", domain.OnTime},    //
		{"2026-11-02T14:45:00Z", domain.LateRecompute}, // closed exactly at the watermark: a late tick recomputes it
		{"2026-11-02T14:40:00Z", domain.LateRecompute}, //
		{"2026-11-02T14:35:00Z", domain.LateRecompute}, // exactly max lateness behind
		{"2026-11-02T14:34:59.999Z", domain.TooLate},   // beyond it: DLQ, reason too_late
	}
	for _, tc := range cases {
		if got := domain.Classify(utc(tc.end), wm, domain.DefaultMaxLateness); got != tc.want {
			t.Errorf("Classify(end %s, watermark %s) = %v, want %v", tc.end, wm.Format(time.RFC3339), got, tc.want)
		}
	}
	for l, s := range map[domain.Lateness]string{domain.OnTime: "on_time", domain.LateRecompute: "late_recompute", domain.TooLate: "too_late"} {
		if l.String() != s {
			t.Errorf("%d.String() = %q, want %q", l, l.String(), s)
		}
	}
	if domain.ReasonTooLate != "too_late" {
		t.Errorf("ReasonTooLate = %q", domain.ReasonTooLate)
	}
}
