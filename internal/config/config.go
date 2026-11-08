// Package config loads typed, validated service configuration (task 01.03, 00E §6).
//
// Each service calls Load once in main and passes sub-structs to constructors; there is no
// global config. Values come from three layers, later ones winning:
//
//  1. the struct tag `default:"…"`
//  2. a YAML file named by FF_CONFIG_FILE (flat map of variable names to values)
//  3. environment variables (an empty variable counts as unset)
//
// Tags:
//
//	env:"FF_NAME"         variable name (required on every leaf field)
//	default:"value"       default, parsed like any other value
//	validate:"r1,r2=arg"  rules: required, required_in_prod, positive, dsn, addr, url, oneof=a b c
//
// Supported field types: string, Secret, int, uint64, time.Duration and []string
// (comma-separated). Struct fields without an env tag, such as the embedded Common, are walked
// recursively. Load reports every invalid variable at once in a *ValidationError.
//
// Only this package and cmd/** read the environment; the forbidigo lint rule enforces it.
package config

import (
	"fmt"
	"reflect"
	"strings"
)

// EnvProd is the FF_ENV value that turns on production-only rules such as required_in_prod.
const EnvProd = "prod"

// Common holds the variables every service reads.
type Common struct {
	Env          string `env:"FF_ENV" default:"dev" validate:"oneof=dev test prod"`
	LogLevel     string `env:"FF_LOG_LEVEL" default:"info" validate:"oneof=debug info warn error"`
	LogFormat    string `env:"FF_LOG_FORMAT" default:"json" validate:"oneof=json text"`
	OTLPEndpoint string `env:"OTEL_EXPORTER_OTLP_ENDPOINT" default:"http://otel-collector:4317" validate:"url"`
}

// redacted maps each variable name of cfg to its value for startup logging. Set secrets show
// as "***" and unset ones as "", so the log tells whether a secret is configured without leaking it.
func redacted(cfg any) map[string]string {
	out := map[string]string{}
	for _, f := range collect(reflect.ValueOf(cfg)) {
		switch v := f.value.Interface().(type) {
		case Secret:
			if v != "" {
				out[f.env] = redactedText
			} else {
				out[f.env] = ""
			}
		case []string:
			out[f.env] = strings.Join(v, ",")
		default:
			out[f.env] = fmt.Sprint(v)
		}
	}
	return out
}
