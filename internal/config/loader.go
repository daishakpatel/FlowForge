package config

import (
	"cmp"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

// Options controls Load.
type Options struct {
	// Prefix names the config-file variable, <Prefix>_CONFIG_FILE. Empty means "FF". Field
	// variable names come from the env tags, which carry their full name (OTEL_* has no FF_ prefix).
	Prefix string
	// LookupEnv reads one variable. Nil means os.LookupEnv; tests pass a map lookup so they
	// can run in parallel.
	LookupEnv func(key string) (string, bool)
}

// field is one leaf of a config struct: a settable value plus its tags.
type field struct {
	env    string
	def    string
	hasDef bool
	rules  string
	value  reflect.Value
}

// Load builds a T from defaults, the optional FF_CONFIG_FILE and the environment, then
// validates it. Invalid variables are all reported together as a *ValidationError; an unreadable
// or malformed config file is returned as a plain wrapped error.
func Load[T any](opts Options) (T, error) {
	var cfg T
	rv := reflect.ValueOf(&cfg).Elem()
	if rv.Kind() != reflect.Struct {
		return cfg, fmt.Errorf("config: Load needs a struct type, got %s", rv.Type())
	}
	lookup := opts.LookupEnv
	if lookup == nil {
		lookup = os.LookupEnv
	}
	fields := collect(rv)

	var file map[string]string
	var errs []FieldError
	fileVar := cmp.Or(opts.Prefix, "FF") + "_CONFIG_FILE"
	if path, ok := lookup(fileVar); ok && path != "" {
		var err error
		if file, err = readFile(fileVar, path); err != nil {
			return cfg, err
		}
		errs = unknownKeys(file, fields)
	}

	invalid := map[string]bool{}
	for _, f := range fields {
		s, set := f.def, f.hasDef
		if v, ok := file[f.env]; ok {
			s, set = v, true
		}
		if v, ok := lookup(f.env); ok && v != "" {
			s, set = v, true
		}
		if !set {
			continue
		}
		if err := setValue(f.value, s); err != nil {
			errs = append(errs, FieldError{Env: f.env, Msg: err.Error()})
			invalid[f.env] = true
		}
	}

	env := envValue(fields)
	for _, f := range fields {
		if !invalid[f.env] {
			errs = append(errs, checkRules(f, env)...)
		}
	}
	if c, ok := any(cfg).(crossChecker); ok {
		errs = append(errs, c.crossCheck()...)
	}
	if len(errs) > 0 {
		return cfg, &ValidationError{Fields: errs}
	}
	return cfg, nil
}

// collect lists the leaf fields of the struct v. Fields without an env tag are walked as
// nested structs; unexported fields are skipped.
func collect(v reflect.Value) []field {
	var out []field
	t := v.Type()
	for i := range t.NumField() {
		sf := t.Field(i)
		if !sf.IsExported() {
			continue
		}
		env, ok := sf.Tag.Lookup("env")
		if !ok {
			if sf.Type.Kind() == reflect.Struct {
				out = append(out, collect(v.Field(i))...)
			}
			continue
		}
		def, hasDef := sf.Tag.Lookup("default")
		out = append(out, field{env: env, def: def, hasDef: hasDef, rules: sf.Tag.Get("validate"), value: v.Field(i)})
	}
	return out
}

// setValue parses s into v. Messages never include s, because v may be a Secret.
func setValue(v reflect.Value, s string) error {
	switch {
	case v.Type() == reflect.TypeFor[time.Duration]():
		d, err := time.ParseDuration(s)
		if err != nil {
			return errors.New("not a duration (examples: 500ms, 10s)")
		}
		v.SetInt(int64(d))
	case v.Kind() == reflect.String:
		v.SetString(s)
	case v.Kind() == reflect.Int:
		n, err := strconv.Atoi(s)
		if err != nil {
			return errors.New("not an integer")
		}
		v.SetInt(int64(n))
	case v.Kind() == reflect.Uint64:
		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return errors.New("not a non-negative integer")
		}
		v.SetUint(n)
	case v.Type() == reflect.TypeFor[[]string]():
		v.Set(reflect.ValueOf(splitList(s)))
	default:
		return fmt.Errorf("unsupported field type %s", v.Type())
	}
	return nil
}

// splitList splits a comma-separated list, trimming spaces and dropping empty items.
func splitList(s string) []string {
	var out []string
	for _, item := range strings.Split(s, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}

// readFile reads the YAML config file: a flat map of variable names to scalar values.
func readFile(fileVar, path string) (map[string]string, error) {
	b, err := os.ReadFile(path) //nolint:gosec // G304: the operator chooses the file through FF_CONFIG_FILE
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", fileVar, err)
	}
	var m map[string]string
	if err := yaml.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("config: parse %s %s (want a flat map of variable names to scalars): %w", fileVar, path, err)
	}
	return m, nil
}

// unknownKeys rejects file keys that no FlowForge service reads, which catches typos. Keys of
// other services are allowed so one file can serve every service.
func unknownKeys(file map[string]string, fields []field) []FieldError {
	known := map[string]bool{}
	for _, f := range fields {
		known[f.env] = true
	}
	for _, f := range serviceFields() {
		known[f.env] = true
	}
	var errs []FieldError
	for k := range file {
		if !known[k] {
			errs = append(errs, FieldError{Env: k, Msg: "unknown variable in config file"})
		}
	}
	sort.Slice(errs, func(i, j int) bool { return errs[i].Env < errs[j].Env })
	return errs
}

// serviceFields lists the fields of every service config struct.
func serviceFields() []field {
	var out []field
	for _, cfg := range []any{API{}, Scheduler{}, Worker{}, Raft{}, Processor{}} {
		out = append(out, collect(reflect.ValueOf(cfg))...)
	}
	return out
}

// envValue returns the parsed FF_ENV, or "" when the struct has no such field.
func envValue(fields []field) string {
	for _, f := range fields {
		if f.env == "FF_ENV" {
			return f.value.String()
		}
	}
	return ""
}
