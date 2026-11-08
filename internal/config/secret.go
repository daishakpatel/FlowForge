package config

import (
	"fmt"
	"io"
	"log/slog"
)

const redactedText = "***"

// Secret holds a credential (password, DSN with a password, API key, token).
//
// Every formatting path prints "***": fmt with any verb (via Format), String, GoString,
// encoding/json, encoding.TextMarshaler and slog (LogValuer). Only Reveal returns the value,
// so leaking it takes an explicit call. A Secret must sit in an exported struct field:
// fmt prints unexported fields without calling their methods.
type Secret string

// Reveal returns the secret's value. Call it only where the value is used, never to log it.
func (s Secret) Reveal() string { return string(s) }

// String returns "***".
func (Secret) String() string { return redactedText }

// GoString returns "***", so %#v does not leak.
func (Secret) GoString() string { return redactedText }

// Format prints "***" for every fmt verb, including %x and %q, which would otherwise encode the value.
func (Secret) Format(f fmt.State, _ rune) { _, _ = io.WriteString(f, redactedText) }

// MarshalJSON encodes as "***".
func (Secret) MarshalJSON() ([]byte, error) { return []byte(`"` + redactedText + `"`), nil }

// MarshalText encodes as "***" (YAML, JSON map keys, slog text handler).
func (Secret) MarshalText() ([]byte, error) { return []byte(redactedText), nil }

// LogValue logs as "***" with any slog handler.
func (Secret) LogValue() slog.Value { return slog.StringValue(redactedText) }
