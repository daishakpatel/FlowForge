package source

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/shopspring/decimal"

	"github.com/daishakpatel/flowforge/internal/stream/domain"
)

// Format is a FileSource input format.
type Format string

// The file formats (docs/architecture/market-data-generator.md).
const (
	// FormatCSV has a header row naming the columns event_time, symbol, price, size, source and
	// optionally sequence and event_id, in any order.
	FormatCSV Format = "csv"
	// FormatJSONL has one JSON object per line with the same keys; price and size may be JSON
	// numbers or strings. Blank lines are skipped.
	FormatJSONL Format = "jsonl"
)

const maxLine = 1 << 20 // a JSONL line longer than 1 MiB stops the read

var requiredColumns = []string{"event_time", "symbol", "price", "size", "source"}

// FileSource reads historical ticks from CSV or JSON lines. Rows it cannot parse come back from
// Next as *ParseError with their line, and reading goes on. It checks only structure (times,
// numbers); domain rules are the producer's, so a negative price is returned as read. A row with no
// event_id gets EventID of its fields.
type FileSource struct {
	format Format
	closer io.Closer
	csv    *csv.Reader
	cols   map[string]int
	lines  *bufio.Scanner
	line   int
	done   bool
}

// Open opens path as a FileSource, choosing the format from the extension: .csv, or .jsonl, .ndjson
// and .json for JSON lines. Close it when done.
func Open(path string) (*FileSource, error) {
	var f Format
	switch strings.ToLower(filepath.Ext(path)) {
	case ".csv":
		f = FormatCSV
	case ".jsonl", ".ndjson", ".json":
		f = FormatJSONL
	default:
		return nil, fmt.Errorf("open %s: unknown format (want .csv or .jsonl)", path)
	}
	file, err := os.Open(path) //nolint:gosec // G304: the operator names the input file
	if err != nil {
		return nil, fmt.Errorf("open ticks file: %w", err)
	}
	fs := NewFileSource(file, f)
	fs.closer = file
	return fs, nil
}

// NewFileSource reads r in format f.
func NewFileSource(r io.Reader, f Format) *FileSource {
	fs := &FileSource{format: f}
	if f == FormatCSV {
		fs.csv = csv.NewReader(r)
		fs.csv.FieldsPerRecord = -1 // rows are checked one by one, so a short row is a ParseError
	} else {
		fs.lines = bufio.NewScanner(r)
		fs.lines.Buffer(make([]byte, 0, 64<<10), maxLine)
	}
	return fs
}

// Close closes the underlying file, if Open opened one.
func (f *FileSource) Close() error {
	if f.closer == nil {
		return nil
	}
	return f.closer.Close()
}

// Next returns the next tick (see Source).
func (f *FileSource) Next(ctx context.Context) (domain.Tick, error) {
	if err := ctx.Err(); err != nil {
		return domain.Tick{}, err
	}
	if f.done {
		return domain.Tick{}, io.EOF
	}
	if f.format == FormatCSV {
		return f.nextCSV()
	}
	return f.nextJSONL()
}

func (f *FileSource) nextCSV() (domain.Tick, error) {
	if f.cols == nil {
		if err := f.readHeader(); err != nil {
			f.done = true // without a usable header no row can be read
			return domain.Tick{}, err
		}
	}
	rec, err := f.csv.Read()
	var pe *csv.ParseError
	switch {
	case errors.Is(err, io.EOF):
		f.done = true
		return domain.Tick{}, io.EOF
	case errors.As(err, &pe):
		return domain.Tick{}, &ParseError{Line: pe.Line, Err: pe.Err}
	case err != nil:
		return domain.Tick{}, fmt.Errorf("read ticks csv: %w", err)
	}
	line, _ := f.csv.FieldPos(0) // only after a successful Read: a failed one records no positions
	get := func(col string) string {
		if i, ok := f.cols[col]; ok && i < len(rec) {
			return strings.TrimSpace(rec[i])
		}
		return ""
	}
	if len(rec) < len(requiredColumns) {
		return domain.Tick{}, &ParseError{Line: line, Err: fmt.Errorf("%d fields, want at least %d", len(rec), len(requiredColumns))}
	}
	t, err := parseRow(get("event_time"), get("symbol"), get("price"), get("size"), get("source"), get("sequence"), get("event_id"))
	if err != nil {
		return domain.Tick{}, &ParseError{Line: line, Err: err}
	}
	return t, nil
}

// readHeader reads the header row. Its errors are fatal, not *ParseError: without a header no row
// can be read, so a run must stop rather than report an empty success.
func (f *FileSource) readHeader() error {
	hdr, err := f.csv.Read()
	switch {
	case errors.Is(err, io.EOF):
		return errors.New("ticks csv: empty input, no header row") // not io.EOF: an empty load is not a success
	case err != nil:
		return fmt.Errorf("ticks csv line 1: header: %w", err)
	}
	if len(hdr) > 0 {
		hdr[0] = strings.TrimPrefix(hdr[0], "\ufeff") // a byte-order mark, as Excel writes
	}
	f.cols = make(map[string]int, len(hdr))
	for i, h := range hdr {
		f.cols[strings.TrimSpace(h)] = i
	}
	for _, c := range requiredColumns {
		if _, ok := f.cols[c]; !ok {
			return fmt.Errorf("ticks csv line 1: header lacks column %s (want %s, optionally sequence and event_id)", c, strings.Join(requiredColumns, ", "))
		}
	}
	return nil
}

// jsonRow is one JSONL line. price, size and sequence stay raw so numbers and strings both work.
type jsonRow struct {
	EventTime string          `json:"event_time"`
	Symbol    string          `json:"symbol"`
	Price     json.RawMessage `json:"price"`
	Size      json.RawMessage `json:"size"`
	Source    string          `json:"source"`
	Sequence  json.RawMessage `json:"sequence"`
	EventID   string          `json:"event_id"`
}

func (f *FileSource) nextJSONL() (domain.Tick, error) {
	for f.lines.Scan() {
		f.line++
		text := strings.TrimSpace(f.lines.Text())
		if text == "" {
			continue
		}
		var r jsonRow
		if err := json.Unmarshal([]byte(text), &r); err != nil {
			return domain.Tick{}, &ParseError{Line: f.line, Err: err}
		}
		t, err := parseRow(r.EventTime, r.Symbol, unquote(r.Price), unquote(r.Size), r.Source, unquote(r.Sequence), r.EventID)
		if err != nil {
			return domain.Tick{}, &ParseError{Line: f.line, Err: err}
		}
		return t, nil
	}
	f.done = true
	if err := f.lines.Err(); err != nil {
		return domain.Tick{}, fmt.Errorf("read ticks jsonl after line %d: %w", f.line, err)
	}
	return domain.Tick{}, io.EOF
}

// unquote turns a raw JSON number or string into its text; anything else stays raw and fails to
// parse as a number.
func unquote(raw json.RawMessage) string {
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	return string(raw)
}

func parseRow(eventTime, symbol, price, size, src, seq, eventID string) (domain.Tick, error) {
	at, err := time.Parse(time.RFC3339Nano, eventTime)
	if err != nil {
		return domain.Tick{}, fmt.Errorf("event_time %q: want RFC 3339", eventTime)
	}
	p, err := decimal.NewFromString(price)
	if err != nil {
		return domain.Tick{}, fmt.Errorf("price %q: not a number", price)
	}
	sz, err := decimal.NewFromString(size)
	if err != nil {
		return domain.Tick{}, fmt.Errorf("size %q: not a number", size)
	}
	var n int64
	if seq != "" {
		if n, err = strconv.ParseInt(seq, 10, 64); err != nil {
			return domain.Tick{}, fmt.Errorf("sequence %q: not an integer", seq)
		}
	}
	t := domain.Tick{EventID: eventID, Symbol: symbol, Price: p, Size: sz, EventTime: at.UTC(), Source: src, Seq: n}
	if t.EventID == "" {
		t.EventID = EventID(t.Source, t.Symbol, t.Seq, t.EventTime)
	}
	return t, nil
}
