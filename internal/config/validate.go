package config

import (
	"net"
	"net/url"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"
)

// FieldError is one invalid variable. Msg never contains the value, so it is safe to log even
// when the variable is a secret.
type FieldError struct {
	Env string // variable name, e.g. FF_PG_DSN
	Msg string
}

func (e FieldError) Error() string { return e.Env + ": " + e.Msg }

// ValidationError lists every invalid variable found by one Load call.
type ValidationError struct {
	Fields []FieldError
}

func (e *ValidationError) Error() string {
	parts := make([]string, len(e.Fields))
	for i, f := range e.Fields {
		parts[i] = f.Error()
	}
	return "config: invalid configuration: " + strings.Join(parts, "; ")
}

// crossChecker is implemented by service structs whose rules span several fields.
type crossChecker interface {
	crossCheck() []FieldError
}

// checkRules applies f's validate tag. env is the parsed FF_ENV.
func checkRules(f field, env string) []FieldError {
	var errs []FieldError
	for _, rule := range strings.Split(f.rules, ",") {
		name, arg, _ := strings.Cut(strings.TrimSpace(rule), "=")
		if msg := checkRule(name, arg, f.value, env); msg != "" {
			errs = append(errs, FieldError{Env: f.env, Msg: msg})
		}
	}
	return errs
}

// checkRule returns a message when v breaks the rule, or "". Format rules skip empty values;
// required covers those.
func checkRule(name, arg string, v reflect.Value, env string) string {
	switch name {
	case "":
		return ""
	case "required":
		if v.IsZero() || (v.Kind() == reflect.Slice && v.Len() == 0) {
			return "required"
		}
	case "required_in_prod":
		if env == EnvProd && v.IsZero() {
			return "required when FF_ENV=" + EnvProd
		}
	case "positive":
		if (v.CanInt() && v.Int() <= 0) || (v.CanUint() && v.Uint() == 0) {
			return "must be greater than 0"
		}
	case "oneof":
		if !slices.Contains(strings.Fields(arg), v.String()) {
			return "must be one of: " + arg
		}
	case "dsn":
		return eachString(v, validDSN, "not a postgres:// URL with host and database name")
	case "addr":
		return eachString(v, validAddr, "not a host:port address")
	case "url":
		return eachString(v, validURL, "not an absolute URL")
	default:
		return "unknown validate rule " + strconv.Quote(name) // programmer error in a struct tag
	}
	return ""
}

// eachString applies ok to v, or to each item when v is a list, skipping empty values.
func eachString(v reflect.Value, ok func(string) bool, msg string) string {
	items := []string{v.String()}
	if v.Kind() == reflect.Slice {
		items, _ = v.Interface().([]string)
	}
	for _, s := range items {
		if s != "" && !ok(s) {
			return msg
		}
	}
	return ""
}

// validDSN accepts URL-form Postgres DSNs. The parse error is dropped because it would quote
// the DSN, password included.
func validDSN(s string) bool {
	u, err := url.Parse(s)
	return err == nil && (u.Scheme == "postgres" || u.Scheme == "postgresql") &&
		u.Host != "" && strings.TrimPrefix(u.Path, "/") != ""
}

// validAddr accepts host:port with an optional host (":8080") and a port of 0–65535.
func validAddr(s string) bool {
	_, port, err := net.SplitHostPort(s)
	if err != nil {
		return false
	}
	_, err = strconv.ParseUint(port, 10, 16)
	return err == nil
}

func validURL(s string) bool {
	u, err := url.Parse(s)
	return err == nil && u.Scheme != "" && u.Host != ""
}

// renewBelowHalf reports when renew is not strictly below ttl/2, so a lease survives one missed
// renewal. Non-positive values are skipped: the positive rule already reports them.
func renewBelowHalf(renewEnv string, renew time.Duration, ttlEnv string, ttl time.Duration) []FieldError {
	if renew <= 0 || ttl <= 0 || renew < ttl/2 {
		return nil
	}
	return []FieldError{{Env: renewEnv, Msg: "must be less than half of " + ttlEnv}}
}
