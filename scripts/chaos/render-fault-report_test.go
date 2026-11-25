package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func sample(t *testing.T) []run {
	t.Helper()
	f, err := os.Open("testdata/sample.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	rows, err := parse(f)
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func TestFold_GroupsRunsOrdersScenariosAndJudgesTargets(t *testing.T) {
	rows := sample(t)
	if len(rows) != 6 {
		t.Fatalf("parsed %d rows, want 6 (the empty line skipped)", len(rows))
	}
	res := fold(rows)
	var ids []string
	for _, s := range res.Scenarios {
		ids = append(ids, s.ID)
	}
	if got := strings.Join(ids, ","); got != "1,10,X2,N,C1" {
		t.Fatalf("order = %s, want 1,10,X2,N,C1", got)
	}
	one := res.Scenarios[0]
	if one.Runs != 2 || one.Passed != 2 || !one.Pass || one.Observed != "second run" || one.Ops != 1000 {
		t.Errorf("scenario 1 = %+v", one)
	}
	if one.TTR == nil || one.TTR.Min != 1.2 || one.TTR.Max != 3.4 {
		t.Errorf("scenario 1 ttr = %+v", one.TTR)
	}
	if one.TargetMet == nil || *one.TargetMet {
		t.Errorf("scenario 1 missed its target in one run; target_met = %v", one.TargetMet)
	}
	if x := res.Scenarios[2]; x.Pass || x.Passed != 0 || x.TargetMet != nil {
		t.Errorf("X2 = %+v, want failed with no target", x)
	}
	if res.Passed != 4 || res.Rows != 6 || res.LastRunAt != "2026-11-23T01:00:00Z" {
		t.Errorf("totals = %d passed, %d rows, last %s", res.Passed, res.Rows, res.LastRunAt)
	}
}

func TestMarkdown_TableAndTargets(t *testing.T) {
	md := markdown(fold(sample(t)))
	for _, want := range []string{
		"**4 of 5 scenarios passed every run** (6 runs in all; latest 2026-11-23T01:00:00Z)",
		"| 1 | Kill leader | Kill \\| leader | new leader | second run | pass | 1.20 / 3.40 / 3.40 |",
		"| X2 | Leader transfer | transfer | ok | ok | **FAIL** | 0.10 / 0.10 / 0.10 |",
		"| N | Negative control | stale read | Illegal | Illegal | pass | — |",
		"target Raft failover ≤ 3s: **missed** in at least one run",
		"| Raft failover ≤ 3s | 1, C1 | 3.40 | **no** |",
	} {
		if !strings.Contains(md, want) {
			t.Errorf("report lacks %q\n%s", want, md)
		}
	}
}

func TestParse_RejectsBadRows(t *testing.T) {
	for name, in := range map[string]string{
		"not json": "{\"id\":\"1\"}\nnot json\n",
		"no id":    "{\"scenario\":\"x\"}\n",
	} {
		if _, err := parse(strings.NewReader(in)); err == nil {
			t.Errorf("%s: parsed without error", name)
		}
	}
}

func TestRender_WritesBothFiles(t *testing.T) {
	dir := t.TempDir()
	js, md := filepath.Join(dir, "r.json"), filepath.Join(dir, "r.md")
	notes := filepath.Join(dir, "notes.md")
	if err := os.WriteFile(notes, []byte("Run 2 of X1 missed the target: one election timeout too many.\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := render("testdata/sample.jsonl", js, md, 0, notes); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(js) //nolint:gosec // G304: the test's temp file
	if err != nil {
		t.Fatal(err)
	}
	var res Results
	if err := json.Unmarshal(raw, &res); err != nil || len(res.Scenarios) != 5 {
		t.Fatalf("results file: %d scenarios, %v", len(res.Scenarios), err)
	}
	b, err := os.ReadFile(md) //nolint:gosec // G304: as above
	if err != nil || !strings.HasPrefix(string(b), "# Raft fault report") {
		t.Fatalf("report: %.40q, %v", b, err)
	}
	if !strings.HasSuffix(string(b), "## Analysis\n\nRun 2 of X1 missed the target: one election timeout too many.\n") {
		t.Errorf("report does not end with the analysis notes:\n%s", b)
	}
	if err := render("", js, md, 0, ""); err == nil {
		t.Error("render without -in succeeded")
	}
}

// A run that wrote no row (a hang killed by go test -timeout, or a panic that ended the binary)
// must not vanish: with the expected run count, a scenario with fewer rows fails (code review).
func TestFold_MissingRunsFailTheScenario(t *testing.T) {
	res := foldRuns(sample(t), 2)
	for _, s := range res.Scenarios {
		if want := s.ID == "1"; s.Pass != want {
			t.Errorf("scenario %s with %d of 2 runs reported: pass = %v", s.ID, s.Runs, s.Pass)
		}
	}
	if md := markdown(res); !strings.Contains(md, "only 1 of 2 runs reported") {
		t.Errorf("report does not flag missing runs:\n%s", md)
	}
}
