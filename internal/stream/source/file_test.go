package source_test

import (
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/shopspring/decimal"

	"github.com/daishakpatel/flowforge/internal/stream/domain"
	"github.com/daishakpatel/flowforge/internal/stream/source"
)

const fixtures = "../../../testdata/marketdata/"

// readAll drains s, returning the ticks and the line numbers of the rows it rejected.
func readAll(t *testing.T, s source.Source) ([]domain.Tick, []int) {
	t.Helper()
	var ticks []domain.Tick
	var bad []int
	for {
		tk, err := s.Next(t.Context())
		var pe *source.ParseError
		switch {
		case errors.Is(err, io.EOF):
			return ticks, bad
		case errors.As(err, &pe):
			bad = append(bad, pe.Line)
		case err != nil:
			t.Fatalf("Next: %v", err)
		default:
			ticks = append(ticks, tk)
		}
	}
}

func openFixture(t *testing.T, name string) []domain.Tick {
	t.Helper()
	fs, err := source.Open(fixtures + name)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fs.Close() }) // read-only
	ticks, bad := readAll(t, fs)
	if len(bad) != 0 {
		t.Fatalf("%s: bad rows at lines %v", name, bad)
	}
	return ticks
}

func TestFileSource_FixturesParseTheSame(t *testing.T) {
	t.Parallel()
	csv, jsonl := openFixture(t, "sample_ticks.csv"), openFixture(t, "sample_ticks.jsonl")
	if len(csv) != 8 || len(jsonl) != 8 {
		t.Fatalf("csv %d ticks, jsonl %d; want 8 each", len(csv), len(jsonl))
	}
	for i := range csv {
		a, b := csv[i], jsonl[i]
		if a.EventID != b.EventID || a.Symbol != b.Symbol || !a.Price.Equal(b.Price) || !a.Size.Equal(b.Size) ||
			!a.EventTime.Equal(b.EventTime) || a.Source != b.Source || a.Seq != b.Seq {
			t.Errorf("row %d differs: csv %+v jsonl %+v", i, a, b)
		}
		if r := domain.Validate(a, a.EventTime); r != domain.ReasonNone {
			t.Errorf("row %d invalid: %s", i, r)
		}
	}
	first := csv[0]
	if first.EventID != source.EventID("csv", "ACME", 1, first.EventTime) || !first.Price.Equal(dec("101.25")) {
		t.Errorf("first row %+v: the event id is computed when the file has none", first)
	}
	if csv[7].EventID != "hist-acme-4" {
		t.Errorf("a given event id was replaced: %q", csv[7].EventID)
	}
}

func TestFileSource_BadRowsReportTheirLine(t *testing.T) {
	t.Parallel()
	csvIn := "event_time,symbol,price,size,source,sequence\n" + // line 1
		"2026-11-02T14:30:00Z,ACME,10,1,csv,1\n" + // 2
		"2026-11-02T14:30:01Z,ACME,ten,1,csv,2\n" + // 3: bad price
		"2026-11-02T14:30:02Z,ACME,10,1,csv,3\n" + // 4
		"yesterday,ACME,10,1,csv,4\n" + // 5: bad time
		"2026-11-02T14:30:03Z,ACME,10,1\n" + // 6: missing fields
		"2026-11-02T14:30:04Z,ACME,10,1,csv,x\n" // 7: bad sequence
	ticks, bad := readAll(t, source.NewFileSource(strings.NewReader(csvIn), source.FormatCSV))
	if len(ticks) != 2 || !equalInts(bad, []int{3, 5, 6, 7}) {
		t.Errorf("csv: %d good, bad lines %v; want 2 good and lines [3 5 6 7]", len(ticks), bad)
	}

	jsonIn := `{"event_time":"2026-11-02T14:30:00Z","symbol":"ACME","price":10,"size":1,"source":"jsonl","sequence":1}` + "\n" + // 1
		"\n" + // 2: blank lines are skipped
		`{"event_time":"2026-11-02T14:30:01Z","symbol":"ACME","price":"10.5","size":1,"source":"jsonl"}` + "\n" + // 3: string numbers are fine
		`{"event_time":` + "\n" + // 4: not JSON
		`{"event_time":"2026-11-02T14:30:02Z","symbol":"ACME","price":true,"size":1,"source":"jsonl"}` + "\n" // 5: bad price
	ticks, bad = readAll(t, source.NewFileSource(strings.NewReader(jsonIn), source.FormatJSONL))
	if len(ticks) != 2 || !equalInts(bad, []int{4, 5}) {
		t.Errorf("jsonl: %d good, bad lines %v; want 2 good and lines [4 5]", len(ticks), bad)
	}
	if len(ticks) == 2 && !ticks[1].Price.Equal(dec("10.5")) {
		t.Errorf("string price parsed as %s", ticks[1].Price)
	}

	// A broken quote in the first field is a bad row, not a crash.
	quoted := "event_time,symbol,price,size,source\n" + "2026\"x,ACME,1,1,csv\n" + "2026-11-02T14:30:00Z,ACME,10,1,csv\n"
	ticks, bad = readAll(t, source.NewFileSource(strings.NewReader(quoted), source.FormatCSV))
	if len(ticks) != 1 || !equalInts(bad, []int{2}) {
		t.Errorf("bare quote: %d good, bad lines %v; want 1 good and line [2]", len(ticks), bad)
	}

	// An Excel byte-order mark does not hide the first column.
	bom := "\ufeffevent_time,symbol,price,size,source\n2026-11-02T14:30:00Z,ACME,10,1,csv\n"
	if ticks, bad = readAll(t, source.NewFileSource(strings.NewReader(bom), source.FormatCSV)); len(ticks) != 1 || len(bad) != 0 {
		t.Errorf("BOM header: %d good, bad %v", len(ticks), bad)
	}
}

// TestFileSource_BadHeaderIsFatal: without a usable header nothing can be read, so the error is not
// a skippable ParseError and a run stops on it instead of reporting success.
func TestFileSource_BadHeaderIsFatal(t *testing.T) {
	t.Parallel()
	for name, in := range map[string]string{"missing column": "symbol,price\nACME,1\n", "empty file": ""} {
		_, err := source.NewFileSource(strings.NewReader(in), source.FormatCSV).Next(t.Context())
		var pe *source.ParseError
		if err == nil || errors.Is(err, io.EOF) || errors.As(err, &pe) {
			t.Errorf("%s: %v, want a fatal (non-ParseError) error", name, err)
		}
	}
	_, err := source.NewFileSource(strings.NewReader("symbol,price\nACME,1\n"), source.FormatCSV).Next(t.Context())
	if err == nil || !strings.Contains(err.Error(), "event_time") {
		t.Errorf("missing column error %v does not name it", err)
	}
}

func TestOpen_RejectsUnknownFormats(t *testing.T) {
	t.Parallel()
	if _, err := source.Open(fixtures + "missing.parquet"); err == nil {
		t.Error("unknown extension accepted")
	}
	if _, err := source.Open(fixtures + "does_not_exist.csv"); err == nil {
		t.Error("missing file accepted")
	}
}

func dec(s string) decimal.Decimal { return decimal.RequireFromString(s) }

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
