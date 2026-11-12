//go:build integration

package rest

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/daishakpatel/flowforge/internal/api/auth"
	"github.com/daishakpatel/flowforge/internal/logging"
)

// TestRBAC_Matrix: for every role and every /api/v1 operation, the request is denied (403)
// exactly when the ACL table denies it (FR-SEC-02). Responses are contract-checked.
func TestRBAC_Matrix(t *testing.T) {
	e := newAPIEnv(t)
	c := newContract(t)
	acl := routeACL()
	checked := 0
	for _, s := range c.operations() {
		if !strings.HasPrefix(s.path, "/api/v1/") {
			continue
		}
		need := requiredRole(acl, s.method, s.path)
		for _, role := range auth.Roles() {
			code := c.do(t, e.raw, s.method, s.samplePath(), nil, e.bearer(role)...).Code
			if denied := code == http.StatusForbidden; denied == auth.Allowed(role, need) {
				t.Errorf("%s %s as %s → %d; ACL requires %s", s.method, s.path, role, code, need)
			}
			checked++
		}
	}
	if checked != 14*len(auth.Roles()) {
		t.Errorf("checked %d (role, operation) pairs, want %d", checked, 14*len(auth.Roles()))
	}
}

func TestAuth_InvalidKeysAre401(t *testing.T) {
	e := newAPIEnv(t)
	c := newContract(t)
	past := e.clk.Now().Add(-time.Hour)
	expired := e.newKey(t, auth.RoleViewer, &past)
	unknown, _ := auth.GenerateKey(strings.NewReader(strings.Repeat("u", 64)))
	viewer := e.tokens[auth.RoleViewer]
	cases := map[string][]string{
		"missing header": nil,
		"not bearer":     {"Authorization", "Basic " + viewer},
		"malformed":      {"Authorization", "Bearer ff_garbage"},
		"unknown key":    {"Authorization", "Bearer " + unknown.Token},
		"wrong secret":   {"Authorization", "Bearer " + viewer[:16] + unknown.Token[16:]},
		"expired":        {"Authorization", "Bearer " + expired},
	}
	for name, h := range cases {
		rec := c.do(t, e.raw, "GET", "/api/v1/runs", nil, h...)
		var env errorEnvelope
		_ = json.Unmarshal(rec.Body.Bytes(), &env)
		if rec.Code != http.StatusUnauthorized || env.Error.Code != "UNAUTHENTICATED" || env.Error.RequestID == "" {
			t.Errorf("%s: %d %s, want the 401 envelope", name, rec.Code, rec.Body.String())
		}
	}
}

// TestAuth_RevocationTakesEffect: through the API a revoked key stops at once; revoked by another
// instance (here: directly in the database) it stops once the 30 s cache entry expires.
func TestAuth_RevocationTakesEffect(t *testing.T) {
	e := newAPIEnv(t)
	c := newContract(t)
	viaAPI := e.newKey(t, auth.RoleViewer, nil)
	elsewhere := e.newKey(t, auth.RoleViewer, nil)
	for _, tok := range []string{viaAPI, elsewhere} {
		want(t, c.do(t, e.raw, "GET", "/api/v1/runs", nil, "Authorization", "Bearer "+tok).Code, 200, "fresh key")
	}

	id, _, _ := auth.ParseToken(viaAPI)
	want(t, c.do(t, e.raw, "DELETE", "/api/v1/api-keys/"+id, nil, e.bearer(auth.RoleAdmin)...).Code, 204, "revoke via API")
	want(t, c.do(t, e.raw, "GET", "/api/v1/runs", nil, "Authorization", "Bearer "+viaAPI).Code, 401, "right after revoking via the API")

	id, _, _ = auth.ParseToken(elsewhere)
	if _, err := e.pool.Exec(t.Context(), `UPDATE api_keys SET revoked_at = now() WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	want(t, c.do(t, e.raw, "GET", "/api/v1/runs", nil, "Authorization", "Bearer "+elsewhere).Code, 200, "within the cache TTL")
	e.clk.Advance(auth.CacheTTL)
	want(t, c.do(t, e.raw, "GET", "/api/v1/runs", nil, "Authorization", "Bearer "+elsewhere).Code, 401, "after the cache TTL")
}

func TestAPIKeys_SecretShownOnceAndWorksAtOnce(t *testing.T) {
	e := newAPIEnv(t)
	c := newContract(t)
	rec := c.do(t, e.raw, "POST", "/api/v1/api-keys", []byte(`{"name":"dashboard","role":"viewer"}`), e.bearer(auth.RoleAdmin)...)
	want(t, rec.Code, 201, "create")
	created := decodeBody[createdAPIKeyResponse](t, rec.Body.Bytes())
	want(t, c.do(t, e.raw, "GET", "/api/v1/runs", nil, "Authorization", "Bearer "+created.Key).Code, 200, "new key works")
	want(t, c.do(t, e.raw, "POST", "/api/v1/workflows", marketDataHourly(t), "Authorization", "Bearer "+created.Key).Code, 403, "viewer cannot create")

	list := c.do(t, e.raw, "GET", "/api/v1/api-keys?limit=200", nil, e.bearer(auth.RoleAdmin)...)
	if strings.Contains(list.Body.String(), created.Key[16:]) || strings.Contains(list.Body.String(), `"key"`) {
		t.Errorf("listing returns a secret: %s", list.Body.String())
	}
	var stored []byte
	if err := e.pool.QueryRow(t.Context(), `SELECT secret_hash FROM api_keys WHERE id = $1`, created.ID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(stored), created.Key[16:]) {
		t.Error("api_keys stores the secret in clear text")
	}
	rec = c.do(t, e.raw, "POST", "/api/v1/api-keys", []byte(`{"name":"old","role":"viewer","expires_at":"2020-01-01T00:00:00Z"}`), e.bearer(auth.RoleAdmin)...)
	want(t, rec.Code, 400, "expiry in the past")
}

// TestAudit_RowsForMutationsAndDenials checks actor, action, resource and outcome, and that no
// secret or body content reaches audit_log.
func TestAudit_RowsForMutationsAndDenials(t *testing.T) {
	e := newAPIEnv(t)
	c := newContract(t)
	op := e.bearer(auth.RoleOperator)
	rec := c.do(t, e.raw, "POST", "/api/v1/workflows", marketDataHourly(t), op...)
	want(t, rec.Code, 201, "operator creates a workflow")
	wfID := decodeBody[workflowResponse](t, rec.Body.Bytes()).ID
	want(t, c.do(t, e.raw, "POST", "/api/v1/workflows", marketDataHourly(t), e.bearer(auth.RoleViewer)...).Code, 403, "viewer create")
	want(t, c.do(t, e.raw, "GET", "/api/v1/runs", nil, e.bearer(auth.RoleWorker)...).Code, 403, "worker read")
	want(t, c.do(t, e.raw, "GET", "/api/v1/runs", nil, e.bearer(auth.RoleViewer)...).Code, 200, "viewer read")
	rec = c.do(t, e.raw, "POST", "/api/v1/api-keys", []byte(`{"name":"svc","role":"worker"}`), e.bearer(auth.RoleAdmin)...)
	newKey := decodeBody[createdAPIKeyResponse](t, rec.Body.Bytes())

	type row struct{ actor, role, action, resType, resID, outcome string }
	rows, err := e.pool.Query(t.Context(), `SELECT coalesce(actor_key_id,''), coalesce(actor_role,''), action,
		coalesce(resource_type,''), coalesce(resource_id,''), outcome FROM audit_log ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.actor, &r.role, &r.action, &r.resType, &r.resID, &r.outcome); err != nil {
			t.Fatal(err)
		}
		got = append(got, r)
	}
	wantRows := []row{
		{e.keyID(auth.RoleOperator), "operator", "POST /api/v1/workflows", "workflows", wfID, "ok"},
		{e.keyID(auth.RoleViewer), "viewer", "POST /api/v1/workflows", "workflows", "", "denied"},
		{e.keyID(auth.RoleWorker), "worker", "GET /api/v1/runs", "runs", "", "denied"},
		{e.keyID(auth.RoleAdmin), "admin", "POST /api/v1/api-keys", "api-keys", newKey.ID, "ok"},
	}
	if len(got) != len(wantRows) {
		t.Fatalf("audit rows = %+v, want %+v (the allowed GET is not audited)", got, wantRows)
	}
	for i := range wantRows {
		if got[i] != wantRows[i] {
			t.Errorf("audit row %d = %+v, want %+v", i, got[i], wantRows[i])
		}
	}
	var all string
	if err := e.pool.QueryRow(t.Context(), `SELECT string_agg(audit_log::text, ' ') FROM audit_log`).Scan(&all); err != nil {
		t.Fatal(err)
	}
	for _, leak := range []string{"ff_", newKey.Key[16:], "market-data-ingest", "symbols"} {
		if strings.Contains(all, leak) {
			t.Errorf("audit_log contains %q", leak)
		}
	}
}

func TestRateLimit_SeparateBucketsPerKey(t *testing.T) {
	e := newAPIEnv(t)
	c := newContract(t)
	r := e.routerWith(t, func(d *Deps) { d.RateLimitRPS = 1 }) // burst 2; the fake clock never refills
	viewer, other := e.bearer(auth.RoleViewer), e.bearer(auth.RoleOperator)
	for i := range 2 {
		want(t, c.do(t, r, "GET", "/api/v1/runs", nil, viewer...).Code, 200, "within burst "+string(rune('1'+i)))
	}
	rec := c.do(t, r, "GET", "/api/v1/runs", nil, viewer...)
	if rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") != "1" {
		t.Errorf("third request = %d (Retry-After %q), want 429 with Retry-After 1", rec.Code, rec.Header().Get("Retry-After"))
	}
	want(t, c.do(t, r, "GET", "/api/v1/runs", nil, other...).Code, 200, "another key's bucket is full")
	want(t, c.do(t, r, "GET", "/healthz", nil).Code, 200, "health is exempt")
}

// TestLogs_NeverContainKeys: request logs at debug level (which include auth failures) carry the
// key id at most, never a key's secret.
func TestLogs_NeverContainKeys(t *testing.T) {
	e := newAPIEnv(t)
	c := newContract(t)
	var buf bytes.Buffer
	r := e.routerWith(t, func(d *Deps) {
		d.Logger = logging.New(logging.Config{Level: "debug", Format: "json", Writer: &buf})
	})
	admin := e.tokens[auth.RoleAdmin]
	c.do(t, r, "GET", "/api/v1/runs", nil, "Authorization", "Bearer "+admin)
	c.do(t, r, "GET", "/api/v1/runs", nil, "Authorization", "Bearer "+admin[:20]+"tampered"+admin[28:])
	rec := c.do(t, r, "POST", "/api/v1/api-keys", []byte(`{"name":"x","role":"viewer"}`), "Authorization", "Bearer "+admin)
	created := decodeBody[createdAPIKeyResponse](t, rec.Body.Bytes())
	logs := buf.String()
	if !strings.Contains(logs, "invalid API key") {
		t.Fatalf("expected the auth failure in the debug log:\n%s", logs)
	}
	for _, secret := range []string{admin[16:], created.Key[16:]} {
		if strings.Contains(logs, secret) {
			t.Errorf("a key secret appears in the logs:\n%s", logs)
		}
	}
}
