package auth

import (
	"context"
	"fmt"
	"io"

	"github.com/daishakpatel/flowforge/internal/config"
	"github.com/daishakpatel/flowforge/internal/persistence"
)

// Names given to bootstrap keys in api_keys.
const (
	bootstrapKeyName    = "bootstrap-admin"
	devBootstrapKeyName = "dev-bootstrap-admin"
	bootstrapCreatedBy  = "bootstrap"
)

// Bootstrap makes sure an admin can reach the API on first start (task 02.05 §4). It is safe to
// run on every start and from several instances at once.
//
//   - A configured key (FF_ADMIN_BOOTSTRAP_KEY) must be format-valid, or Bootstrap fails. When
//     no active admin key exists it is stored as an admin key; storing it again is a no-op.
//   - In dev, with no configured key and no active admin key, a new admin key is generated and
//     returned so the caller can show it once. Concurrent dev instances may each create one.
//   - Otherwise nothing happens and generated is "".
func Bootstrap(ctx context.Context, db persistence.Querier, keys persistence.APIKeyRepo, rnd io.Reader,
	env string, configured config.Secret) (generated string, err error) {
	var cfgKey NewKey
	if configured != "" {
		if cfgKey, err = HashToken(rnd, configured.Reveal()); err != nil {
			return "", fmt.Errorf("FF_ADMIN_BOOTSTRAP_KEY: %w", err)
		}
	}
	has, err := keys.HasActiveAdmin(ctx, db)
	if err != nil || has {
		return "", err
	}
	if configured != "" {
		_, err = keys.CreateIfAbsent(ctx, db, newAdmin(cfgKey, bootstrapKeyName))
		return "", err
	}
	if env != "dev" {
		return "", nil
	}
	k, err := GenerateKey(rnd)
	if err != nil {
		return "", err
	}
	if _, err := keys.Create(ctx, db, newAdmin(k, devBootstrapKeyName)); err != nil {
		return "", err
	}
	return k.Token, nil
}

func newAdmin(k NewKey, name string) persistence.NewAPIKey {
	return persistence.NewAPIKey{ID: k.ID, Name: name, Role: RoleAdmin, SecretHash: k.Hash, Salt: k.Salt, CreatedBy: bootstrapCreatedBy}
}
