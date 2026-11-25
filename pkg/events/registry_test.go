package events_test

import (
	"encoding/json"
	"errors"
	"io/fs"
	"strings"
	"testing"
	"time"

	"github.com/daishakpatel/flowforge/pkg/events"
	"github.com/daishakpatel/flowforge/schemas"
)

func registry(t *testing.T) *events.Registry {
	t.Helper()
	r, err := events.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// schemaDoc is the parts of a schema file the tests read.
type schemaDoc struct {
	Required   []string                   `json:"required"`
	Properties map[string]json.RawMessage `json:"properties"`
	Examples   []map[string]any           `json:"examples"`
}

func readSchema(t *testing.T, name string) schemaDoc {
	t.Helper()
	b, err := fs.ReadFile(schemas.Events, "events/"+name+".schema.json")
	if err != nil {
		t.Fatal(err)
	}
	var d schemaDoc
	if err := json.Unmarshal(b, &d); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	if len(d.Examples) == 0 {
		t.Fatalf("%s has no examples", name)
	}
	return d
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// clone deep-copies a decoded JSON object.
func clone(t *testing.T, m map[string]any) map[string]any {
	t.Helper()
	var c map[string]any
	if err := json.Unmarshal(mustJSON(t, m), &c); err != nil {
		t.Fatal(err)
	}
	return c
}

func validationError(t *testing.T, err error) *events.ValidationError {
	t.Helper()
	var ve *events.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("error %v (%T) is not a *events.ValidationError", err, err)
	}
	return ve
}

func TestRegistry_HasEverySchemaFile(t *testing.T) {
	t.Parallel()
	files, err := fs.Glob(schemas.Events, "events/*.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	names := registry(t).Names()
	if len(files) == 0 || len(names) != len(files) {
		t.Fatalf("registry has %d schemas %v, the tree %d files", len(names), names, len(files))
	}
	for _, want := range []string{"envelope.v1", "flowforge_event.v1", "market_tick.v1", "market_aggregate.v1", "market_invalid.v1"} {
		if !contains(names, want) {
			t.Errorf("registry lacks %s: %v", want, names)
		}
	}
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

func TestRegistry_ExamplesValidate(t *testing.T) {
	t.Parallel()
	r := registry(t)
	for _, name := range r.Names() {
		for i, ex := range readSchema(t, name).Examples {
			if err := r.Validate(name, mustJSON(t, ex)); err != nil {
				t.Errorf("%s example %d: %v", name, i, err)
			}
		}
	}
}

// wrongType returns a JSON value of a different type than the schema property declares.
func wrongType(prop json.RawMessage) (any, bool) {
	var p struct {
		Type any `json:"type"`
	}
	if json.Unmarshal(prop, &p) != nil || p.Type == nil {
		return nil, false
	}
	if s, ok := p.Type.(string); ok && s == "string" {
		return 12345, true
	}
	return "not-the-declared-type", true
}

func TestRegistry_InvalidExamplesFailWithPath(t *testing.T) {
	t.Parallel()
	r := registry(t)
	for _, name := range r.Names() {
		d := readSchema(t, name)
		ex := d.Examples[0]
		for _, field := range d.Required {
			if _, ok := ex[field]; !ok {
				continue // flowforge_event.v1 inherits its required fields from envelope.v1
			}
			bad := clone(t, ex)
			delete(bad, field)
			err := r.Validate(name, mustJSON(t, bad))
			if err == nil {
				t.Errorf("%s without %q validated", name, field)
				continue
			}
			if !strings.Contains(validationError(t, err).Error(), field) {
				t.Errorf("%s without %q: error %q does not name the field", name, field, err)
			}
		}
		for field, prop := range d.Properties {
			v, ok := wrongType(prop)
			if _, present := ex[field]; !ok || !present {
				continue
			}
			bad := clone(t, ex)
			bad[field] = v
			err := r.Validate(name, mustJSON(t, bad))
			if err == nil {
				t.Errorf("%s with %s=%v validated", name, field, v)
				continue
			}
			if ve := validationError(t, err); ve.Path != "/"+field {
				t.Errorf("%s with %s=%v: path %q, want /%s (%v)", name, field, v, ve.Path, field, err)
			}
		}
	}
}

func tickEnvelope(t *testing.T) map[string]any {
	t.Helper()
	return clone(t, readSchema(t, "envelope.v1").Examples[0])
}

func TestRegistry_KnownEventTypeChecksPayload(t *testing.T) {
	t.Parallel()
	r := registry(t)
	cases := map[string]func(p map[string]any){
		"/payload/price":  func(p map[string]any) { p["price"] = -1 },
		"/payload/symbol": func(p map[string]any) { p["symbol"] = "acme" },
		"/payload/size":   func(p map[string]any) { p["size"] = "300" },
	}
	for path, mutate := range cases {
		env := tickEnvelope(t)
		mutate(env["payload"].(map[string]any))
		err := r.Validate("envelope.v1", mustJSON(t, env))
		if err == nil {
			t.Errorf("%s: invalid tick payload validated", path)
			continue
		}
		if ve := validationError(t, err); ve.Path != path || ve.Schema != "envelope.v1" {
			t.Errorf("%s: got schema %q path %q (%v)", path, ve.Schema, ve.Path, err)
		}
	}
}

func TestRegistry_UnknownFieldsAndTypesAreForwardCompatible(t *testing.T) {
	t.Parallel()
	r := registry(t)
	env := tickEnvelope(t)
	env["added_in_a_later_minor"] = map[string]any{"x": 1}
	env["payload"].(map[string]any)["venue"] = "XNYS"
	if err := r.Validate("envelope.v1", mustJSON(t, env)); err != nil {
		t.Errorf("unknown fields rejected: %v", err)
	}

	quote := tickEnvelope(t)
	quote["event_type"] = "market.quote" // a type this version does not know keeps any object payload
	quote["payload"] = map[string]any{"bid": 1, "ask": 2}
	if err := r.Validate("envelope.v1", mustJSON(t, quote)); err != nil {
		t.Errorf("unknown event type rejected by envelope.v1: %v", err)
	}
	if err := r.Validate("flowforge_event.v1", mustJSON(t, quote)); err == nil {
		t.Error("flowforge_event.v1 accepted a non-workflow event type")
	}

	v2 := tickEnvelope(t)
	v2["event_version"] = 2 // a later version of a known type is not checked against the v1 payload
	v2["payload"] = map[string]any{"renamed": true}
	if err := r.Validate("envelope.v1", mustJSON(t, v2)); err != nil {
		t.Errorf("market.tick version 2 checked against version 1: %v", err)
	}
}

func TestRegistry_Rejects(t *testing.T) {
	t.Parallel()
	r := registry(t)
	if err := r.Validate("nope.v1", []byte(`{}`)); !errors.Is(err, events.ErrUnknownSchema) {
		t.Errorf("unknown schema: %v, want ErrUnknownSchema", err)
	}
	for _, in := range []string{``, `{`, `{"a":1} trailing`, `[]`} {
		if err := r.Validate("market_tick.v1", []byte(in)); err == nil {
			t.Errorf("%q validated", in)
		} else {
			_ = validationError(t, err) // fails t unless err is a *ValidationError
		}
	}
	env := tickEnvelope(t)
	env["occurred_at"] = "yesterday"
	if err := r.Validate("envelope.v1", mustJSON(t, env)); err == nil {
		t.Error("occurred_at that is not RFC 3339 validated (format must be asserted)")
	}
}

type tick struct {
	EventID   string    `json:"event_id"`
	Symbol    string    `json:"symbol"`
	EventTime time.Time `json:"event_time"`
	Price     float64   `json:"price"`
	Size      float64   `json:"size"`
	Source    string    `json:"source"`
}

func TestEnvelope_MarshalValidatesAndRoundTrips(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 11, 2, 9, 30, 0, 250_000_000, time.FixedZone("EST", -5*3600))
	e := events.Envelope[tick]{
		EventID: "3f786850e387550fdab836ed7e6dc881de23001b", EventType: "market.tick", EventVersion: 1,
		OccurredAt: at, Producer: "marketdata-producer", CorrelationID: "ACME", AggregateID: "ACME",
		Payload: tick{EventID: "3f786850e387550fdab836ed7e6dc881de23001b", Symbol: "ACME", EventTime: at, Price: 101.25, Size: 300, Source: "synthetic"},
	}
	b, err := events.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"occurred_at":"2026-11-02T14:30:00.25Z"`) {
		t.Errorf("occurred_at not written in UTC: %s", b)
	}
	if strings.Contains(string(b), `"trace"`) {
		t.Errorf("empty trace written: %s", b)
	}
	if err := registry(t).Validate("envelope.v1", b); err != nil {
		t.Fatalf("marshalled envelope invalid: %v", err)
	}
	got, err := events.Unmarshal[tick](b)
	if err != nil {
		t.Fatal(err)
	}
	if !got.OccurredAt.Equal(at) || got.Payload.Price != 101.25 || got.EventType != "market.tick" || got.AggregateID != "ACME" {
		t.Errorf("round trip = %+v", got)
	}
}

func TestUnmarshal_IgnoresUnknownFields(t *testing.T) {
	t.Parallel()
	in := `{"event_id":"e1","event_type":"market.tick","event_version":1,"occurred_at":"2026-11-02T14:30:00Z",` +
		`"producer":"p","correlation_id":"c","aggregate_id":"a","from_the_future":[1,2],` +
		`"payload":{"symbol":"ACME","price":1,"size":1,"new_field":{"x":true}}}`
	got, err := events.Unmarshal[tick]([]byte(in))
	if err != nil {
		t.Fatalf("unknown fields broke decoding: %v", err)
	}
	if got.Payload.Symbol != "ACME" || got.EventID != "e1" {
		t.Errorf("decoded %+v", got)
	}
	if _, err := events.Unmarshal[tick]([]byte(`{"event_version":"one"}`)); err == nil {
		t.Error("a wrongly typed known field decoded")
	}
}
