package cron

import (
	"strings"
	"testing"
	"time"
)

func utc(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}

func TestNextFire(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, spec, tz, after, want string
	}{
		{"every 5 minutes", "*/5 * * * *", "UTC", "2026-11-17T10:02:30Z", "2026-11-17T10:05:00Z"},
		{"on the boundary is after", "*/5 * * * *", "UTC", "2026-11-17T10:05:00Z", "2026-11-17T10:10:00Z"},
		{"empty timezone is UTC", "0 * * * *", "", "2026-11-17T10:00:00Z", "2026-11-17T11:00:00Z"},
		// 09:00 New York on weekdays: Friday's fire, then Monday's (EST, UTC-5).
		{"weekdays 9am New York", "0 9 * * 1-5", "America/New_York", "2026-11-13T14:00:00Z", "2026-11-16T14:00:00Z"},
		{"weekdays 9am New York in summer (EDT)", "0 9 * * 1-5", "America/New_York", "2026-07-01T12:00:00Z", "2026-07-01T13:00:00Z"},
		// Spring forward, 2027-03-14 New York: 02:00 EST jumps to 03:00 EDT, so 02:30 does not
		// exist and fires at the next valid time, 03:00 EDT (07:00Z); the day after is normal.
		{"spring-forward gap fires at 03:00", "30 2 * * *", "America/New_York", "2027-03-13T07:30:00Z", "2027-03-14T07:00:00Z"},
		{"after the gap, the next day", "30 2 * * *", "America/New_York", "2027-03-14T07:00:00Z", "2027-03-15T06:30:00Z"},
		{"every 5 minutes across the gap", "*/5 * * * *", "America/New_York", "2027-03-14T06:55:00Z", "2027-03-14T07:00:00Z"},
		// Fall back, 2026-11-01 New York: 01:00–01:59 occurs twice (EDT, then EST). 01:30 fires
		// on the first pass only (05:30Z), never again at 01:30 EST (06:30Z).
		{"fall-back first pass", "30 1 * * *", "America/New_York", "2026-10-31T05:30:00Z", "2026-11-01T05:30:00Z"},
		{"fall-back does not repeat", "30 1 * * *", "America/New_York", "2026-11-01T05:30:00Z", "2026-11-02T06:30:00Z"},
		{"inside the repeated hour", "30 1 * * *", "America/New_York", "2026-11-01T06:10:00Z", "2026-11-02T06:30:00Z"},
		{"every 30 minutes across fall-back", "*/30 * * * *", "America/New_York", "2026-11-01T05:30:00Z", "2026-11-01T07:00:00Z"},
		// From inside the repeated hour (01:00 EST, the second pass), every candidate in it maps to
		// its first occurrence, already past: the next fire is 02:00 EST (code review of 03.08).
		{"every 10 minutes from inside the repeated hour", "*/10 * * * *", "America/New_York", "2026-11-01T06:00:00Z", "2026-11-01T07:00:00Z"},
		{"every minute from inside the repeated hour", "* * * * *", "America/New_York", "2026-11-01T06:00:00Z", "2026-11-01T07:00:00Z"},
	}
	for _, tt := range tests {
		got, err := NextFire(tt.spec, tt.tz, utc(tt.after))
		if err != nil {
			t.Errorf("%s: %v", tt.name, err)
			continue
		}
		if want := utc(tt.want); !got.Equal(want) {
			t.Errorf("%s: NextFire(%q, %q, %s) = %s; want %s", tt.name, tt.spec, tt.tz, tt.after, got.UTC().Format(time.RFC3339), tt.want)
		}
	}
}

func TestParse_Rejects(t *testing.T) {
	t.Parallel()
	for _, c := range []struct{ spec, tz, want string }{
		{"61 * * * *", "UTC", "above maximum"},
		{"* * * *", "UTC", "5 fields"},
		{"*/10 * * * * *", "UTC", "5 fields"}, // seconds: below the 1-minute minimum
		{"@every 30s", "UTC", "5 fields"},
		{"@hourly", "UTC", "5 fields"},
		{"TZ=Europe/Paris * * * * *", "UTC", "timezone"},
		{"CRON_TZ=UTC * * * * *", "UTC", "timezone"},
		{"0 9 * * 1-5", "Mars/Olympus_Mons", "timezone"},
		{"0 9 * * 1-5", "Local", "timezone"},
		{"", "UTC", "5 fields"},
		{"0 0 30 2 *", "UTC", "never"}, // February 30th
	} {
		_, err := Parse(c.spec, c.tz)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("Parse(%q, %q) = %v; want an error mentioning %q", c.spec, c.tz, err, c.want)
		}
	}
}

func TestSchedule_StrictlyIncreasing(t *testing.T) {
	t.Parallel()
	s, err := Parse("*/7 * * * *", "Europe/London")
	if err != nil {
		t.Fatal(err)
	}
	at := utc("2026-10-24T00:00:00Z")
	for range 5000 { // over the 2026-10-25 fall-back
		next := s.Next(at)
		if !next.After(at) {
			t.Fatalf("Next(%s) = %s, not after", at, next)
		}
		at = next
	}
}
