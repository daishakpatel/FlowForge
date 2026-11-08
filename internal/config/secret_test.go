package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

const leak = "hunter2-very-secret"

// TestSecret_NeverLeaks covers every formatting path task 01.03 §8 lists, for a bare Secret
// and for a config struct holding Secrets.
func TestSecret_NeverLeaks(t *testing.T) {
	t.Parallel()
	s := Secret(leak)
	cfg := API{PGDSN: Secret("postgres://u:" + leak + "@h/db"), AdminBootstrapKey: s}

	var outputs []string
	for _, v := range []any{s, cfg, &cfg} {
		for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%X", "%d"} {
			outputs = append(outputs, fmt.Sprintf(verb, v))
		}
		outputs = append(outputs, fmt.Sprint(v), fmt.Sprintln(v))
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("json.Marshal: %v", err)
		}
		outputs = append(outputs, string(b))
	}
	text, _ := s.MarshalText()
	outputs = append(outputs, s.String(), s.GoString(), string(text))

	for _, newHandler := range []func(*bytes.Buffer) slog.Handler{
		func(b *bytes.Buffer) slog.Handler { return slog.NewJSONHandler(b, nil) },
		func(b *bytes.Buffer) slog.Handler { return slog.NewTextHandler(b, nil) },
	} {
		var buf bytes.Buffer
		slog.New(newHandler(&buf)).Info("startup", "secret", s, "cfg", cfg, "ptr", &cfg, "redacted", cfg.Redacted())
		outputs = append(outputs, buf.String())
	}

	for _, out := range outputs {
		if strings.Contains(out, leak) || strings.Contains(out, fmt.Sprintf("%x", leak)) {
			t.Errorf("secret leaked: %s", out)
		}
	}
	if s.Reveal() != leak {
		t.Errorf("Reveal() = %q, want the value", s.Reveal())
	}
}

func TestSecret_MasksAsStars(t *testing.T) {
	t.Parallel()
	b, _ := json.Marshal(Secret(leak))
	if got := fmt.Sprintf("%v|%s", Secret(leak), b); got != `***|"***"` {
		t.Errorf("got %s, want ***|\"***\"", got)
	}
}

func TestRedacted_MasksSetSecretsAndShowsUnset(t *testing.T) {
	t.Parallel()
	cfg, err := Load[API](envOpts(map[string]string{"FF_ADMIN_BOOTSTRAP_KEY": leak}))
	if err != nil {
		t.Fatal(err)
	}
	r := cfg.Redacted()
	want := map[string]string{
		"FF_ADMIN_BOOTSTRAP_KEY": "***",
		"FF_PG_DSN":              "***",
		"FF_API_HTTP_ADDR":       ":8080",
		"FF_WORKER_LEASE_TTL":    "15s",
		"FF_RAFT_ENDPOINTS":      "raft-1:7000,raft-2:7000,raft-3:7000",
		"FF_ENV":                 "dev",
	}
	for k, v := range want {
		if r[k] != v {
			t.Errorf("Redacted()[%s] = %q, want %q", k, r[k], v)
		}
	}
	if got := (Worker{}).Redacted()["FF_WORKER_TOKEN"]; got != "" {
		t.Errorf("unset secret shows %q, want empty", got)
	}
}
