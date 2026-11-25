// Command render-fault-report turns the Raft fault suite's results (task 04.08: one JSON object
// per scenario run, appended by test/fault/raft to FF_FAULT_RESULTS) into the committed results
// file and report: every run of a scenario is folded into one row with its pass count and the
// spread of its time-to-recover, and each recovery target says whether every run met it.
//
//	go run ./scripts/chaos -in results.jsonl -json docs/testing/raft-fault-results.json -md docs/testing/raft-fault-report.md
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
)

// run is one scenario run, as test/fault/raft writes it.
type run struct {
	ID         string   `json:"id"`
	Scenario   string   `json:"scenario"`
	Level      string   `json:"level"`
	Fault      string   `json:"fault"`
	Expected   string   `json:"expected"`
	Observed   string   `json:"observed"`
	Pass       bool     `json:"pass"`
	TTRSeconds *float64 `json:"ttr_seconds"`
	Target     string   `json:"target"`
	TargetMet  *bool    `json:"target_met"`
	Ops        int      `json:"ops"`
	At         string   `json:"at"`
}

// Spread is the min, median and max of a scenario's time-to-recover over its runs.
type Spread struct {
	Min    float64 `json:"min"`
	Median float64 `json:"median"`
	Max    float64 `json:"max"`
}

// Scenario is one scenario's rows folded together.
type Scenario struct {
	ID           string  `json:"id"`
	Scenario     string  `json:"scenario"`
	Level        string  `json:"level"`
	Fault        string  `json:"fault"`
	Expected     string  `json:"expected"`
	Observed     string  `json:"observed"` // the latest run's
	Runs         int     `json:"runs"`
	ExpectedRuns int     `json:"expected_runs,omitempty"` // runs the suite was asked for (0: not checked)
	Passed       int     `json:"passed"`
	Pass         bool    `json:"pass"` // every expected run reported and passed
	TTR          *Spread `json:"ttr_seconds,omitempty"`
	Target       string  `json:"target,omitempty"`
	TargetMet    *bool   `json:"target_met,omitempty"` // every measured run met it
	Ops          int     `json:"ops"`                  // the largest history checked
	LastRunAt    string  `json:"last_run_at"`
}

// Results is the committed results file.
type Results struct {
	Rows      int        `json:"rows"`
	LastRunAt string     `json:"last_run_at"`
	Passed    int        `json:"scenarios_passed"`
	Scenarios []Scenario `json:"scenarios"`
}

// parse reads one run per non-empty line.
func parse(r io.Reader) ([]run, error) {
	var out []run
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 1<<16), 1<<20)
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var x run
		if err := json.Unmarshal([]byte(line), &x); err != nil {
			return nil, fmt.Errorf("line %d: %w", n, err)
		}
		if x.ID == "" {
			return nil, fmt.Errorf("line %d: a row without an id", n)
		}
		out = append(out, x)
	}
	return out, sc.Err()
}

// order sorts §8.7's numbered scenarios first, then the extras (X), the negative control (N) and
// the container-level ones (C).
func order(id string) (int, int) {
	if n, err := strconv.Atoi(id); err == nil {
		return 0, n
	}
	n, _ := strconv.Atoi(id[1:]) // "N" has no number: 0
	return strings.Index("XNC", id[:1]) + 1, n
}

// fold groups the runs by scenario.
func fold(rows []run) Results { return foldRuns(rows, 0) }

// foldRuns is fold with the number of runs the suite was asked for: a scenario that reported
// fewer (a run killed by go test -timeout, or ended by a panic, writes no row) fails.
func foldRuns(rows []run, expected int) Results {
	by := map[string][]run{}
	for _, r := range rows {
		by[r.ID] = append(by[r.ID], r)
	}
	res := Results{Rows: len(rows)}
	for id, rs := range by {
		sort.SliceStable(rs, func(i, j int) bool { return rs[i].At < rs[j].At })
		last := rs[len(rs)-1]
		s := Scenario{ID: id, Scenario: last.Scenario, Level: last.Level, Fault: last.Fault, Expected: last.Expected,
			Observed: last.Observed, Runs: len(rs), Target: last.Target, LastRunAt: last.At}
		var ttr []float64
		met, measured := true, false
		for _, r := range rs {
			if r.Pass {
				s.Passed++
			}
			s.Ops = max(s.Ops, r.Ops)
			if r.TTRSeconds != nil {
				ttr = append(ttr, *r.TTRSeconds)
			}
			if r.TargetMet != nil {
				measured, met = true, met && *r.TargetMet
			}
		}
		s.ExpectedRuns = expected
		s.Pass = s.Passed == s.Runs && s.Runs >= expected
		if len(ttr) > 0 {
			slices.Sort(ttr)
			s.TTR = &Spread{Min: ttr[0], Median: ttr[len(ttr)/2], Max: ttr[len(ttr)-1]}
		}
		if measured {
			s.TargetMet = &met
		}
		if s.Pass {
			res.Passed++
		}
		res.LastRunAt = max(res.LastRunAt, s.LastRunAt)
		res.Scenarios = append(res.Scenarios, s)
	}
	sort.Slice(res.Scenarios, func(i, j int) bool {
		gi, ni := order(res.Scenarios[i].ID)
		gj, nj := order(res.Scenarios[j].ID)
		return gi < gj || gi == gj && ni < nj
	})
	return res
}

// cell keeps a table cell on one line.
func cell(s string) string { return strings.ReplaceAll(strings.ReplaceAll(s, "|", "\\|"), "\n", " ") }

// markdown renders the report.
func markdown(res Results) string {
	var b strings.Builder
	b.WriteString("# Raft fault report\n\n")
	b.WriteString("Generated by `make test-fault-raft` (task 04.08) from `docs/testing/raft-fault-results.json`; do not edit by hand. " +
		"How the suite works and how to add a scenario: [`fault-injection.md`](fault-injection.md).\n\n")
	fmt.Fprintf(&b, "**%d of %d scenarios passed every run** (%d runs in all; latest %s). "+
		"Time-to-recover is the first successful linearizable write after the fault, as min / median / max over the runs.\n\n",
		res.Passed, len(res.Scenarios), res.Rows, res.LastRunAt)
	b.WriteString("| # | Scenario | Injected fault | Expected | Observed (latest run) | Pass/fail | Time-to-recover (s) | Notes |\n")
	b.WriteString("|---|---|---|---|---|---|---|---|\n")
	for _, s := range res.Scenarios {
		verdict := "pass"
		if !s.Pass {
			verdict = "**FAIL**"
		}
		ttr := "—"
		if s.TTR != nil {
			ttr = fmt.Sprintf("%.2f / %.2f / %.2f", s.TTR.Min, s.TTR.Median, s.TTR.Max)
		}
		notes := fmt.Sprintf("%s; %d/%d runs passed; largest history %d ops", s.Level, s.Passed, s.Runs, s.Ops)
		if s.Runs < s.ExpectedRuns {
			notes += fmt.Sprintf("; **only %d of %d runs reported**", s.Runs, s.ExpectedRuns)
		}
		if s.TargetMet != nil {
			met := "met in every run"
			if !*s.TargetMet {
				met = "**missed** in at least one run"
			}
			notes += fmt.Sprintf("; target %s: %s", s.Target, met)
		}
		fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %s | %s | %s |\n", s.ID, cell(s.Scenario), cell(s.Fault),
			cell(s.Expected), cell(s.Observed), verdict, ttr, cell(notes))
	}
	b.WriteString("\n## Recovery targets (00_MASTER §6.1)\n\n| Target | Scenarios | Worst measured (s) | Met |\n|---|---|---|---|\n")
	targets := map[string][]Scenario{}
	var names []string
	for _, s := range res.Scenarios {
		if s.TargetMet == nil {
			continue
		}
		if _, ok := targets[s.Target]; !ok {
			names = append(names, s.Target)
		}
		targets[s.Target] = append(targets[s.Target], s)
	}
	for _, name := range names {
		var ids []string
		worst, met := 0.0, true
		for _, s := range targets[name] {
			ids = append(ids, s.ID)
			if s.TTR != nil {
				worst = max(worst, s.TTR.Max)
			}
			met = met && *s.TargetMet
		}
		verdict := "yes"
		if !met {
			verdict = "**no**"
		}
		fmt.Fprintf(&b, "| %s | %s | %.2f | %s |\n", cell(name), strings.Join(ids, ", "), worst, verdict)
	}
	return b.String()
}

func main() {
	in := flag.String("in", "", "results file (one JSON object per line)")
	jsonOut := flag.String("json", "docs/testing/raft-fault-results.json", "results file to write")
	mdOut := flag.String("md", "docs/testing/raft-fault-report.md", "report to write")
	runs := flag.Int("runs", 0, "runs every scenario should have reported (0: don't check)")
	notes := flag.String("notes", "", "hand-written analysis appended to the report as ## Analysis (optional)")
	flag.Parse()
	if err := render(*in, *jsonOut, *mdOut, *runs, *notes); err != nil {
		fmt.Fprintln(os.Stderr, "render-fault-report:", err)
		os.Exit(1)
	}
}

func render(in, jsonOut, mdOut string, runs int, notes string) error {
	if in == "" {
		return errors.New("-in is required")
	}
	f, err := os.Open(in) //nolint:gosec // G304: the operator's results file
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }() // read-only
	rows, err := parse(f)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return fmt.Errorf("%s has no results", in)
	}
	res := foldRuns(rows, runs)
	js, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(jsonOut, append(js, '\n'), 0o644); err != nil { //nolint:gosec // G306: a committed report
		return err
	}
	report := markdown(res)
	if notes != "" {
		// The analysis of targets missed and anything else the numbers can't say is written by a
		// person (task 04.08: shortfalls are reported with analysis) and kept beside the report.
		text, err := os.ReadFile(notes) //nolint:gosec // G304: the operator's notes file
		if err != nil {
			return err
		}
		report += "\n## Analysis\n\n" + string(text)
	}
	return os.WriteFile(mdOut, []byte(report), 0o644) //nolint:gosec // G306: a committed report
}
