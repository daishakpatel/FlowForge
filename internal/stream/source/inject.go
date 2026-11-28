package source

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// ParseInject parses the --inject flag: comma-separated key=value pairs, where dup, late, ooo,
// invalid and gap take a percentage ("dup=1%") and late_max a duration ("late_max=2m"). An empty
// string injects nothing. Rates are checked against each other by NewSynthetic.
func ParseInject(s string) (Inject, error) {
	var in Inject
	seen := map[string]bool{}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		key, val, ok := strings.Cut(part, "=")
		if !ok {
			return Inject{}, fmt.Errorf("inject %q: want key=value", part)
		}
		if seen[key] {
			return Inject{}, fmt.Errorf("inject: %s given twice", key)
		}
		seen[key] = true
		if key == "late_max" {
			d, err := time.ParseDuration(val)
			if err != nil {
				return Inject{}, fmt.Errorf("inject late_max: %w", err)
			}
			in.LateMax = d
			continue
		}
		target := map[string]*float64{"dup": &in.Dup, "late": &in.Late, "ooo": &in.OOO, "invalid": &in.Invalid, "gap": &in.Gap}[key]
		if target == nil {
			return Inject{}, fmt.Errorf("inject: unknown key %q (want dup, late, ooo, invalid, gap, late_max)", key)
		}
		pct, ok := strings.CutSuffix(val, "%")
		f, err := strconv.ParseFloat(pct, 64)
		if !ok || err != nil || math.IsNaN(f) || f < 0 || f > 100 {
			return Inject{}, fmt.Errorf("inject %s=%q: want a percentage from 0%% to 100%%", key, val)
		}
		*target = f / 100
	}
	return in, nil
}
