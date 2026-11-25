package events

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"slices"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"golang.org/x/text/language"
	"golang.org/x/text/message"

	"github.com/daishakpatel/flowforge/schemas"
)

// ErrUnknownSchema is returned by Validate for a schema name the registry does not have.
var ErrUnknownSchema = errors.New("unknown event schema")

// Registry holds the embedded event schemas (schemas/events), compiled once. It is immutable
// after NewRegistry and safe for concurrent use; build one per process and share it.
type Registry struct {
	schemas map[string]*jsonschema.Schema // by name, e.g. "envelope.v1"
	printer *message.Printer              // renders violations; safe for concurrent use
}

// NewRegistry compiles every schemas/events/<name>.schema.json. format (date-time) is asserted,
// and a $ref can only reach another embedded schema: nothing is ever fetched.
func NewRegistry() (*Registry, error) {
	files, err := fs.Glob(schemas.Events, "events/*.schema.json")
	if err != nil {
		return nil, fmt.Errorf("list event schemas: %w", err)
	}
	c := jsonschema.NewCompiler()
	c.DefaultDraft(jsonschema.Draft2020)
	c.AssertFormat()
	c.UseLoader(noFetch{})
	ids := make(map[string]string, len(files))
	for _, f := range files {
		b, err := fs.ReadFile(schemas.Events, f)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", f, err)
		}
		doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(b))
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", f, err)
		}
		m, _ := doc.(map[string]any)
		id, _ := m["$id"].(string)
		if id == "" {
			return nil, fmt.Errorf("%s has no $id", f)
		}
		if err := c.AddResource(id, doc); err != nil {
			return nil, fmt.Errorf("add %s: %w", f, err)
		}
		ids[strings.TrimSuffix(path.Base(f), ".schema.json")] = id
	}
	r := &Registry{schemas: make(map[string]*jsonschema.Schema, len(ids)), printer: message.NewPrinter(language.English)}
	for name, id := range ids {
		s, err := c.Compile(id)
		if err != nil {
			return nil, fmt.Errorf("compile schema %s: %w", name, err)
		}
		r.schemas[name] = s
	}
	return r, nil
}

// Names returns the schema names, sorted.
func (r *Registry) Names() []string {
	names := make([]string, 0, len(r.schemas))
	for n := range r.schemas {
		names = append(names, n)
	}
	slices.Sort(names)
	return names
}

// Validate checks that data is one JSON document valid against schema name ("envelope.v1",
// "market_tick.v1", …). A document that is not JSON or not valid gives a *ValidationError; an
// unknown name gives ErrUnknownSchema. envelope.v1 also checks the payload of every event type
// and version it knows (docs/architecture/event-schemas.md).
func (r *Registry) Validate(name string, data []byte) error {
	s, ok := r.schemas[name]
	if !ok {
		return fmt.Errorf("%w %q", ErrUnknownSchema, name)
	}
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
	if err != nil {
		return &ValidationError{Schema: name, Reason: "not one JSON document: " + err.Error()}
	}
	err = s.Validate(doc)
	var ve *jsonschema.ValidationError
	switch {
	case err == nil:
		return nil
	case errors.As(err, &ve):
		return newValidationError(name, ve, r.printer)
	}
	return fmt.Errorf("validate against %s: %w", name, err)
}

// ValidationError says why a document does not match a schema. Path is the JSON pointer of the
// most specific offending value ("" for the document itself, "/payload/price"); a missing
// property is reported at its parent, with its name in Reason.
type ValidationError struct {
	Schema string
	Path   string
	Reason string // every violation, "; "-separated, each "<path>: <message>"
}

func (e *ValidationError) Error() string {
	return e.Schema + ": " + e.Reason
}

// newValidationError flattens the library's error tree into its leaves, the actual violations
// (the inner nodes are "allOf failed" and the like). The leaves' own messages are used because
// the library's flat output reduces violations reached through a $ref to "validation failed".
func newValidationError(name string, ve *jsonschema.ValidationError, pr *message.Printer) *ValidationError {
	out := &ValidationError{Schema: name}
	var msgs []string
	depth := -1
	var walk func(e *jsonschema.ValidationError)
	walk = func(e *jsonschema.ValidationError) {
		if len(e.Causes) > 0 {
			for _, c := range e.Causes {
				walk(c)
			}
			return
		}
		p := pointer(e.InstanceLocation)
		msgs = append(msgs, cmp.Or(p, "(document)")+": "+e.ErrorKind.LocalizedString(pr))
		if len(e.InstanceLocation) > depth {
			depth, out.Path = len(e.InstanceLocation), p
		}
	}
	walk(ve)
	out.Reason = strings.Join(msgs, "; ")
	return out
}

// pointer is the RFC 6901 JSON pointer of a decoded path ("" for the document itself).
func pointer(tokens []string) string {
	var b strings.Builder
	for _, t := range tokens {
		b.WriteByte('/')
		b.WriteString(strings.NewReplacer("~", "~0", "/", "~1").Replace(t))
	}
	return b.String()
}

// noFetch refuses every $ref outside the embedded set, so validation never touches the network.
type noFetch struct{}

func (noFetch) Load(url string) (any, error) {
	return nil, fmt.Errorf("schema %s is not embedded (schemas/events)", url)
}
