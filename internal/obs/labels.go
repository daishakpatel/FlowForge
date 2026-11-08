package obs

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
)

// ErrInvalidMetric is wrapped by every naming or label violation.
var ErrInvalidMetric = errors.New("invalid metric")

// AllowedLabels returns the low-cardinality label names a metric may use: the labels of every
// metric in 00E §11 and §11.1. IDs, users and free text are never labels (00C §10.2). "symbol"
// is allowed only while the configured universe has at most 50 symbols (00E §11.1).
func AllowedLabels() []string {
	return []string{
		"queue", "task_type", "status", "outcome", "node", "topic", "partition", "group",
		"route", "method", "code", "peer", "reason", "stage", "kind", "tool", "disposition",
		"result", "symbol",
	}
}

var metricName = regexp.MustCompile(`^flowforge_[a-z][a-z0-9_]*[a-z0-9]$`)

// checkName enforces flowforge_<area>_<name>[_unit], with _total reserved for counters.
func checkName(name string, counter bool) error {
	switch {
	case !metricName.MatchString(name):
		return fmt.Errorf("%w: %q must match %s", ErrInvalidMetric, name, metricName)
	case counter && !strings.HasSuffix(name, "_total"):
		return fmt.Errorf("%w: counter %q must end in _total", ErrInvalidMetric, name)
	case !counter && strings.HasSuffix(name, "_total"):
		return fmt.Errorf("%w: only counters end in _total, got %q", ErrInvalidMetric, name)
	}
	return nil
}

// checkLabels rejects labels outside AllowedLabels and duplicates.
func checkLabels(name string, labels []string) error {
	allowed := AllowedLabels()
	for i, l := range labels {
		if !slices.Contains(allowed, l) {
			return fmt.Errorf("%w: %s: label %q is not in the allow-list (obs.AllowedLabels)", ErrInvalidMetric, name, l)
		}
		if slices.Contains(labels[:i], l) {
			return fmt.Errorf("%w: %s: duplicate label %q", ErrInvalidMetric, name, l)
		}
	}
	return nil
}
