// Package cron starts workflow runs on schedules (task 03.08, FR-WF-14): standard 5-field cron
// expressions in an IANA timezone, fired by a leader-gated Loop through the idempotent run-start
// path, so a fire time produces at most one run across restarts and leader changes.
package cron

import (
	"errors"
	"fmt"
	"strings"
	"time"
	_ "time/tzdata" // IANA zones even where the host or image has no zoneinfo

	rcron "github.com/robfig/cron/v3"
)

// Schedule is a parsed 5-field cron expression in a timezone.
//
// It fires at wall-clock times of its timezone, with these DST rules:
//   - a wall time that does not exist (the spring-forward gap) fires once, at the next valid time,
//     the end of the gap (02:30 on a 02:00→03:00 night fires at 03:00);
//   - a wall time that occurs twice (the fall-back overlap) fires once, at its first occurrence;
//     the repeated hour does not fire again.
//
// Each wall time fires at most once, and fires are strictly increasing.
type Schedule struct {
	spec *rcron.SpecSchedule // evaluated on naive wall-clock time (UTC, no DST)
	loc  *time.Location
}

// Parse parses a 5-field cron expression (minute hour day-of-month month day-of-week) in the IANA
// timezone tz ("" means UTC). Seconds, descriptors (@hourly, @every) and TZ= prefixes are
// rejected, so fires are at least a minute apart. robfig/cron is used as the parser only.
func Parse(spec, tz string) (Schedule, error) {
	spec = strings.TrimSpace(spec)
	if strings.Contains(spec, "TZ=") {
		return Schedule{}, errors.New("set the timezone in the timezone field, not in the schedule")
	}
	if strings.HasPrefix(spec, "@") || len(strings.Fields(spec)) != 5 {
		return Schedule{}, fmt.Errorf("schedule %q: want 5 fields: minute hour day-of-month month day-of-week", spec)
	}
	if tz == "" {
		tz = "UTC"
	}
	if tz == "Local" {
		return Schedule{}, errors.New(`timezone "Local" is not allowed; use an IANA name such as America/New_York`)
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return Schedule{}, fmt.Errorf("unknown timezone %q", tz)
	}
	parsed, err := rcron.NewParser(rcron.Minute | rcron.Hour | rcron.Dom | rcron.Month | rcron.Dow).Parse(spec)
	if err != nil {
		return Schedule{}, fmt.Errorf("schedule %q: %w", spec, err)
	}
	ss, ok := parsed.(*rcron.SpecSchedule)
	if !ok {
		return Schedule{}, fmt.Errorf("schedule %q: not a field schedule", spec)
	}
	ss.Location = time.UTC
	s := Schedule{spec: ss, loc: loc}
	if s.spec.Next(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)).IsZero() {
		return Schedule{}, fmt.Errorf("schedule %q never fires", spec)
	}
	return s, nil
}

// Next returns the first fire time strictly after after, or the zero time if there is none.
func (s Schedule) Next(after time.Time) time.Time {
	local := after.In(s.loc)
	wall := time.Date(local.Year(), local.Month(), local.Day(), local.Hour(), local.Minute(), local.Second(), local.Nanosecond(), time.UTC)
	w := s.spec.Next(wall)
	// A candidate is at or before after only inside a fall-back hour, where every wall time maps
	// to its already-past first occurrence; a day of minutes bounds the walk past it.
	for range 24*60 + 1 {
		if w.IsZero() {
			return time.Time{}
		}
		if at := s.instant(w); at.After(after) {
			return at
		}
		w = s.spec.Next(w)
	}
	return time.Time{}
}

// instant maps the wall time w to a moment in the schedule's timezone: the first occurrence when
// w happens twice, the end of the gap when it does not exist.
func (s Schedule) instant(w time.Time) time.Time {
	at := time.Date(w.Year(), w.Month(), w.Day(), w.Hour(), w.Minute(), 0, 0, s.loc)
	if !sameWall(at, w) { // in a spring-forward gap
		start, end := at.ZoneBounds()
		if wallAfter(at, w) { // landed past the gap: it began at this zone's start
			return start
		}
		return end
	}
	for _, back := range []time.Duration{30 * time.Minute, time.Hour} { // an earlier occurrence?
		if earlier := at.Add(-back); sameWall(earlier, w) {
			return earlier
		}
	}
	return at
}

func sameWall(at, w time.Time) bool {
	return at.Year() == w.Year() && at.YearDay() == w.YearDay() && at.Hour() == w.Hour() && at.Minute() == w.Minute()
}

func wallAfter(at, w time.Time) bool {
	return time.Date(at.Year(), at.Month(), at.Day(), at.Hour(), at.Minute(), 0, 0, time.UTC).After(w)
}

// NextFire returns spec's first fire in timezone tz strictly after after.
func NextFire(spec, tz string, after time.Time) (time.Time, error) {
	s, err := Parse(spec, tz)
	if err != nil {
		return time.Time{}, err
	}
	next := s.Next(after)
	if next.IsZero() {
		return time.Time{}, fmt.Errorf("schedule %q never fires after %s", spec, after.UTC().Format(time.RFC3339))
	}
	return next, nil
}
