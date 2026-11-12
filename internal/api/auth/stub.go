package auth

import (
	"context"
	"net/http"

	"github.com/daishakpatel/flowforge/internal/apperrors"
)

// DevKeyID is the key id of the dev admin every request runs as when authentication is
// disabled; it is what created_by records then.
const DevKeyID = "dev"

// NewStub returns the development authenticator (task 02.04). With disabled
// (FF_AUTH_DISABLED=true, dev/test only) every request is the dev admin; otherwise every request
// is rejected. Production uses NewKeyAuthenticator (task 02.05).
func NewStub(disabled bool) Authenticator {
	if disabled {
		return disabledAuth{}
	}
	return unconfiguredAuth{}
}

type disabledAuth struct{}

func (disabledAuth) Authenticate(*http.Request) (Principal, error) {
	return Principal{KeyID: DevKeyID, Name: "dev", Role: RoleAdmin}, nil
}

func (d disabledAuth) VerifyToken(context.Context, string) (Principal, error) {
	return d.Authenticate(nil)
}

type unconfiguredAuth struct{}

func (unconfiguredAuth) Authenticate(*http.Request) (Principal, error) {
	return Principal{}, apperrors.Unauthenticated("authentication required")
}

func (u unconfiguredAuth) VerifyToken(context.Context, string) (Principal, error) {
	return u.Authenticate(nil)
}
