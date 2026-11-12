package auth

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/daishakpatel/flowforge/internal/apperrors"
)

func TestStub_DisabledIsDevPrincipalOtherwiseUnauthenticated(t *testing.T) {
	t.Parallel()
	req := httptest.NewRequestWithContext(context.Background(), "GET", "/api/v1/runs", nil)
	if p, err := NewStub(true).Authenticate(req); err != nil || p.KeyID != DevKeyID || p.Role != "admin" {
		t.Errorf("disabled = %+v, %v; want the dev admin", p, err)
	}
	if _, err := NewStub(false).Authenticate(req); apperrors.CodeOf(err) != apperrors.CodeUnauthenticated {
		t.Errorf("enabled = %v, want UNAUTHENTICATED", err)
	}
}

func TestPrincipalFrom_RoundTrip(t *testing.T) {
	t.Parallel()
	if _, ok := PrincipalFrom(t.Context()); ok {
		t.Error("PrincipalFrom on a bare context reported a principal")
	}
	p := Principal{KeyID: "k1", Name: "ci", Role: "operator"}
	if got, ok := PrincipalFrom(WithPrincipal(t.Context(), p)); !ok || got != p {
		t.Errorf("PrincipalFrom = %+v, %v; want %+v", got, ok, p)
	}
}
