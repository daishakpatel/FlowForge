package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

// Secret values used throughout; none may appear in any output.
const (
	pw     = "s3cr3t-pa55"
	tok    = "tok-9f8e7d6c5b4a"
	apiKey = "ff_k1_ABCDEFGHIJKLMNOPQRST"
	jwt    = "eyJhbGciOiJIUzI1NiJ9.payload.sig"
)

func newJSON(buf *bytes.Buffer) *slog.Logger {
	return New(Config{Level: "debug", Writer: buf, Service: "flowforge-api", Version: "v1.2.3"})
}

func assertNoSecrets(t *testing.T, out string) {
	t.Helper()
	for _, s := range []string{pw, tok, apiKey, "ABCDEFGHIJKLMNOPQRST", jwt} {
		if strings.Contains(out, s) {
			t.Errorf("secret %q leaked:\n%s", s, out)
		}
	}
}

func decode(t *testing.T, line string) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, line)
	}
	return m
}

func TestRedaction_SensitiveKeys(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	newJSON(&buf).Info("login",
		"password", pw, "Token", tok, "api_key", apiKey, "apiKey", apiKey, "api-key", apiKey,
		"authorization", "Bearer "+jwt, "pg_dsn", "postgres://u:"+pw+"@h/db", "worker_token", tok,
		"client_secret", pw, "user", "alice")
	assertNoSecrets(t, buf.String())
	m := decode(t, buf.String())
	for _, k := range []string{"password", "Token", "api_key", "apiKey", "api-key", "authorization", "pg_dsn", "worker_token", "client_secret"} {
		if m[k] != "***" {
			t.Errorf("%s = %v, want ***", k, m[k])
		}
	}
	if m["user"] != "alice" {
		t.Errorf("non-sensitive value changed: %v", m["user"])
	}
}

func TestRedaction_SecretShapedValuesAndMessage(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	l := newJSON(&buf)
	l.Info("using key " + apiKey + " for request")
	l.Info("call", "header", "Bearer "+jwt)
	l.Error("connect", Err(errors.New("dial postgres://flowforge:"+pw+"@db:5432/flowforge: refused")))
	l.Info("query", "url", "/cb?token="+tok+"&x=1")
	l.Info("body", "raw", `{"password":"`+pw+`","n":1}`)
	l.Info("struct", "creds", struct{ User, Password string }{"u", pw})
	l.Info("plain", "vals", map[string]int{"a": 1})
	assertNoSecrets(t, buf.String())

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if got := decode(t, lines[0])["msg"]; got != "using key ff_*** for request" {
		t.Errorf("msg = %v", got)
	}
	if got := decode(t, lines[2])["err"]; got != "dial postgres://flowforge:***@db:5432/flowforge: refused" {
		t.Errorf("err = %v", got)
	}
	if got := decode(t, lines[3])["url"]; got != "/cb?token=***&x=1" {
		t.Errorf("url = %v", got)
	}
	if _, ok := decode(t, lines[6])["vals"].(map[string]any); !ok {
		t.Error("values without secrets must keep their JSON shape")
	}
}

type creds struct{ user, password string }

// LogValue exposes the password under a sensitive key; the handler must still mask it.
func (c creds) LogValue() slog.Value {
	return slog.GroupValue(slog.String("user", c.user), slog.String("password", c.password))
}

func TestRedaction_NestedGroupsAndLogValuer(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	l := newJSON(&buf)
	l.Info("groups",
		slog.Group("db", "password", pw, "host", "db"),
		slog.Group("secrets", "a", pw, "b", tok),
		slog.Group("outer", slog.Group("inner", "token", tok)),
		"who", creds{"bob", pw})
	l.WithGroup("req").With("authorization", "Bearer "+jwt).Info("bound", "api_key", apiKey)
	assertNoSecrets(t, buf.String())

	m := decode(t, strings.Split(buf.String(), "\n")[0])
	db := m["db"].(map[string]any)
	if db["password"] != "***" || db["host"] != "db" {
		t.Errorf("db group = %v", db)
	}
	if s := m["secrets"].(map[string]any); s["a"] != "***" || s["b"] != "***" {
		t.Errorf("sensitive group members not masked: %v", s)
	}
	if who := m["who"].(map[string]any); who["password"] != "***" || who["user"] != "bob" {
		t.Errorf("LogValuer group = %v", who)
	}
}

func TestRedact(t *testing.T) {
	t.Parallel()
	for in, want := range map[string]string{
		"key ff_abc_0123456789abcdefXYZ ok": "key ff_*** ok",
		"Authorization: Bearer abc.def-ghi": "Authorization: ***",
		"bearer abc.def":                    "bearer ***",
		"postgresql://u:p%40ss@h:5432/db":   "postgresql://u:***@h:5432/db",
		"password=hunter2 user=x":           "password=*** user=x",
		`{"api_key": "k-123", "n": 2}`:      `{"api_key": "***", "n": 2}`,
		"dsn: postgres://u:pw@h/db":         "dsn: ***",
		"token bucket refilled":             "token bucket refilled",
		"ff_scheduler_wake":                 "ff_scheduler_wake",
		"postgres://h/db (no password)":     "postgres://h/db (no password)",
	} {
		if got := Redact(in); got != want {
			t.Errorf("Redact(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestContextFields_IncludedAutomatically(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	l := newJSON(&buf)
	ctx := WithFields(context.Background(), KeyRequestID, "req_1", KeyRunID, "run_1")
	ctx = WithFields(ctx, KeyRunID, "run_2", KeyTaskID, "task_1") // run_id replaced

	l.InfoContext(ctx, "via InfoContext")
	FromContext(NewContext(ctx, l)).Info("via FromContext")
	FromContext(NewContext(ctx, l)).InfoContext(ctx, "both, no duplicates")
	l.InfoContext(ctx, "explicit wins", KeyRunID, "run_explicit")
	l.Info("no ctx")

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	for _, line := range lines[:3] {
		m := decode(t, line)
		if m[KeyRequestID] != "req_1" || m[KeyRunID] != "run_2" || m[KeyTaskID] != "task_1" {
			t.Errorf("context fields missing: %s", line)
		}
		if n := strings.Count(line, `"run_id"`); n != 1 {
			t.Errorf("run_id appears %d times: %s", n, line)
		}
	}
	if line := lines[3]; strings.Count(line, `"run_id"`) != 1 || decode(t, line)[KeyRunID] != "run_explicit" {
		t.Errorf("explicit attribute should win without duplication: %s", line)
	}
	if _, ok := decode(t, lines[4])[KeyRequestID]; ok {
		t.Error("Info without a context must not carry context fields")
	}
	if FromContext(context.Background()) != slog.Default() {
		t.Error("FromContext without logger or fields should return slog.Default()")
	}
}

func TestField_ReturnsLatestValue(t *testing.T) {
	t.Parallel()
	ctx := WithFields(context.Background(), KeyRequestID, "req_1")
	ctx = WithFields(ctx, KeyRequestID, "req_2")
	if v, ok := Field(ctx, KeyRequestID); !ok || v.String() != "req_2" {
		t.Errorf("Field(request_id) = %v, %v; want req_2, true", v, ok)
	}
	if _, ok := Field(ctx, KeyRunID); ok {
		t.Error("Field(run_id) reported a value that was never set")
	}
}

func TestContextFields_AreRedactedAndGroupSafe(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	ctx := WithFields(context.Background(), "token", tok, KeyRunID, "run_9")
	newJSON(&buf).WithGroup("g").InfoContext(ctx, "grouped", "k", "v")
	assertNoSecrets(t, buf.String())
	if g := decode(t, buf.String())["g"].(map[string]any); g[KeyRunID] != "run_9" {
		t.Errorf("context fields inside an open group: %v", g)
	}
}

func TestNew_FormatLevelAndFixedFields(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	l := New(Config{Level: "warn", Writer: &buf, Service: "svc", Version: "v9"})
	l.Info("dropped")
	l.Warn("kept")
	m := decode(t, strings.TrimSpace(buf.String()))
	if m["msg"] != "kept" || m["service"] != "svc" || m["version"] != "v9" || m["ts"] == nil || m["time"] != nil {
		t.Errorf("record = %v", m)
	}

	buf.Reset()
	New(Config{Level: "bogus", Format: "text", Writer: &buf}).Info("hello", "password", pw)
	out := buf.String()
	if !strings.Contains(out, "msg=hello") || !strings.Contains(out, "password=***") || !strings.HasPrefix(out, "ts=") {
		t.Errorf("text output = %q", out)
	}
	assertNoSecrets(t, out)

	if New(Config{}).Handler().Enabled(context.Background(), slog.LevelDebug) {
		t.Error("default level should be info")
	}
}
