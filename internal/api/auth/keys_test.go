package auth

import (
	"context"
	"crypto/rand"
	"errors"
	"net/http/httptest"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/daishakpatel/flowforge/internal/apperrors"
	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/config"
	"github.com/daishakpatel/flowforge/internal/persistence"
)

var tokenShape = regexp.MustCompile(`^ff_[a-z2-7]{12}_[A-Za-z0-9_-]{43}$`)

func TestGenerateKey_FormatAndRoundTrip(t *testing.T) {
	t.Parallel()
	seen := map[string]bool{}
	for range 50 {
		k, err := GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		if !tokenShape.MatchString(k.Token) || len(k.Salt) != saltBytes || len(k.Hash) != 32 {
			t.Fatalf("key = %q salt %d hash %d", k.Token, len(k.Salt), len(k.Hash))
		}
		id, secret, err := ParseToken(k.Token)
		if err != nil || id != k.ID || !verifySecret(k.Salt, k.Hash, secret) {
			t.Fatalf("ParseToken(%q) = %q, %v; verify failed", k.Token, id, err)
		}
		if seen[k.ID] {
			t.Fatalf("duplicate key id %s", k.ID)
		}
		seen[k.ID] = true
	}
}

func TestVerifySecret_RejectsTamperedSecret(t *testing.T) {
	t.Parallel()
	k, err := GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	_, secret, _ := ParseToken(k.Token)
	flipped := []byte(secret)
	flipped[0] ^= 1
	for _, s := range []string{string(flipped), secret[:42], secret + "A", ""} {
		if verifySecret(k.Salt, k.Hash, s) {
			t.Errorf("tampered secret %q verified", s)
		}
	}
	otherSalt := append([]byte(nil), k.Salt...)
	otherSalt[0] ^= 1
	if verifySecret(otherSalt, k.Hash, secret) {
		t.Error("secret verified against a different salt")
	}
}

func TestParseToken_RejectsMalformed(t *testing.T) {
	t.Parallel()
	good, _ := GenerateKey(rand.Reader)
	_, secret, _ := ParseToken(good.Token)
	for _, tok := range []string{
		"", "ff_", "xx_" + good.Token[3:], "ff_ABCDEFGHIJKL_" + secret, "ff_abcdefghijk_" + secret,
		"ff_abcdefghijkl" + secret, "ff_abcdefghijkl_short", "ff_abcdefghijkl_" + secret + "AA",
	} {
		if _, _, err := ParseToken(tok); !errors.Is(err, ErrMalformedKey) {
			t.Errorf("ParseToken(%q) = %v, want ErrMalformedKey", tok, err)
		}
	}
}

// fakeKeys is an in-memory persistence.APIKeyRepo; Get calls are counted.
type fakeKeys struct {
	persistence.APIKeyRepo
	mu    sync.Mutex
	keys  map[string]persistence.APIKeyView
	gets  int
	fails error
}

func newFakeKeys() *fakeKeys { return &fakeKeys{keys: map[string]persistence.APIKeyView{}} }

func (f *fakeKeys) add(t *testing.T, role string) string {
	t.Helper()
	k, err := GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.keys[k.ID] = persistence.APIKeyView{ID: k.ID, Name: role + "-key", Role: role, Salt: k.Salt, SecretHash: k.Hash}
	return k.Token
}

func (f *fakeKeys) Get(_ context.Context, _ persistence.Querier, id string) (persistence.APIKeyView, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gets++
	if f.fails != nil {
		return persistence.APIKeyView{}, f.fails
	}
	k, ok := f.keys[id]
	if !ok {
		return persistence.APIKeyView{}, apperrors.NotFound("api key not found")
	}
	return k, nil
}

func (f *fakeKeys) update(id string, fn func(*persistence.APIKeyView)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := f.keys[id]
	fn(&k)
	f.keys[id] = k
}

func (f *fakeKeys) HasActiveAdmin(context.Context, persistence.Querier) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, k := range f.keys {
		if k.Role == RoleAdmin && k.RevokedAt == nil {
			return true, nil
		}
	}
	return false, nil
}

func (f *fakeKeys) Create(_ context.Context, _ persistence.Querier, k persistence.NewAPIKey) (persistence.APIKeyView, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v := persistence.APIKeyView{ID: k.ID, Name: k.Name, Role: k.Role, Salt: k.Salt, SecretHash: k.SecretHash}
	f.keys[k.ID] = v
	return v, nil
}

func (f *fakeKeys) CreateIfAbsent(ctx context.Context, q persistence.Querier, k persistence.NewAPIKey) (bool, error) {
	f.mu.Lock()
	_, exists := f.keys[k.ID]
	f.mu.Unlock()
	if exists {
		return false, nil
	}
	_, err := f.Create(ctx, q, k)
	return true, err
}

func newTestAuth(keys *fakeKeys) (*KeyAuthenticator, *clock.Fake) {
	clk := clock.NewFake(time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC))
	return NewKeyAuthenticator(nil, keys, clk), clk
}

func idOf(token string) string { id, _, _ := ParseToken(token); return id }

func TestVerifyToken_AcceptsValidRejectsInvalid(t *testing.T) {
	t.Parallel()
	keys := newFakeKeys()
	tok := keys.add(t, RoleOperator)
	a, _ := newTestAuth(keys)
	p, err := a.VerifyToken(t.Context(), tok)
	if err != nil || p.Role != RoleOperator || p.KeyID != idOf(tok) || p.Name != "operator-key" {
		t.Fatalf("VerifyToken = %+v, %v", p, err)
	}
	unknown, _ := GenerateKey(rand.Reader)
	wrongSecret := tok[:16] + unknown.Token[16:]
	for name, bad := range map[string]string{"garbage": "nope", "unknown id": unknown.Token, "wrong secret": wrongSecret} {
		_, err := a.VerifyToken(t.Context(), bad)
		if e := apperrors.FromError(err); e.Code != apperrors.CodeUnauthenticated || e.Message != "invalid API key" {
			t.Errorf("%s: %v, want the generic UNAUTHENTICATED", name, err)
		}
	}
}

// TestVerifyToken_UnknownIDStillComparesAHash: the unknown-key path performs one comparison, so
// its timing matches a wrong secret for a real key.
func TestVerifyToken_UnknownIDStillComparesAHash(t *testing.T) {
	t.Parallel()
	a, _ := newTestAuth(newFakeKeys())
	calls := 0
	a.compare = func(salt, hash []byte, secret string) bool { calls++; return verifySecret(salt, hash, secret) }
	unknown, _ := GenerateKey(rand.Reader)
	if _, err := a.VerifyToken(t.Context(), unknown.Token); apperrors.CodeOf(err) != apperrors.CodeUnauthenticated {
		t.Fatalf("unknown key = %v", err)
	}
	if calls != 1 {
		t.Errorf("hash comparisons on the unknown-key path = %d, want 1", calls)
	}
}

func TestVerifyToken_RevokedExpiredAndCacheTTL(t *testing.T) {
	t.Parallel()
	keys := newFakeKeys()
	tok := keys.add(t, RoleViewer)
	a, clk := newTestAuth(keys)
	if _, err := a.VerifyToken(t.Context(), tok); err != nil {
		t.Fatal(err)
	}
	revoked := clk.Now()
	keys.update(idOf(tok), func(k *persistence.APIKeyView) { k.RevokedAt = &revoked })

	if _, err := a.VerifyToken(t.Context(), tok); err != nil {
		t.Errorf("revoked elsewhere, within the cache TTL: %v, want still accepted from cache", err)
	}
	clk.Advance(CacheTTL)
	if _, err := a.VerifyToken(t.Context(), tok); apperrors.CodeOf(err) != apperrors.CodeUnauthenticated {
		t.Errorf("revoked, after the cache TTL: %v, want UNAUTHENTICATED", err)
	}

	tok2 := keys.add(t, RoleViewer)
	if _, err := a.VerifyToken(t.Context(), tok2); err != nil {
		t.Fatal(err)
	}
	keys.update(idOf(tok2), func(k *persistence.APIKeyView) { k.RevokedAt = &revoked })
	a.Invalidate(idOf(tok2))
	if _, err := a.VerifyToken(t.Context(), tok2); apperrors.CodeOf(err) != apperrors.CodeUnauthenticated {
		t.Errorf("revoked and invalidated: %v, want UNAUTHENTICATED immediately", err)
	}

	tok3 := keys.add(t, RoleViewer)
	expired := clk.Now().Add(-time.Second)
	keys.update(idOf(tok3), func(k *persistence.APIKeyView) { k.ExpiresAt = &expired })
	if _, err := a.VerifyToken(t.Context(), tok3); apperrors.CodeOf(err) != apperrors.CodeUnauthenticated {
		t.Errorf("expired key: %v, want UNAUTHENTICATED", err)
	}
}

func TestVerifyToken_CachesLookupsAndSurfacesStoreErrors(t *testing.T) {
	t.Parallel()
	keys := newFakeKeys()
	tok := keys.add(t, RoleAdmin)
	a, _ := newTestAuth(keys)
	for range 5 {
		if _, err := a.VerifyToken(t.Context(), tok); err != nil {
			t.Fatal(err)
		}
	}
	if keys.gets != 1 {
		t.Errorf("store lookups = %d, want 1 (cached)", keys.gets)
	}
	keys.fails = errors.New("connection refused")
	other := keys.add(t, RoleAdmin)
	if _, err := a.VerifyToken(t.Context(), other); apperrors.CodeOf(err) == apperrors.CodeUnauthenticated || err == nil {
		t.Errorf("store failure = %v, want the store error, not UNAUTHENTICATED", err)
	}
}

func TestAuthenticate_ReadsBearerHeader(t *testing.T) {
	t.Parallel()
	keys := newFakeKeys()
	tok := keys.add(t, RoleViewer)
	a, _ := newTestAuth(keys)
	for header, ok := range map[string]bool{"Bearer " + tok: true, tok: false, "Bearer ": false, "": false, "Basic " + tok: false} {
		req := httptest.NewRequestWithContext(context.Background(), "GET", "/api/v1/runs", nil)
		req.Header.Set("Authorization", header)
		_, err := a.Authenticate(req)
		if (err == nil) != ok || (!ok && apperrors.CodeOf(err) != apperrors.CodeUnauthenticated) {
			t.Errorf("Authorization %q: %v, want ok=%v", header, err, ok)
		}
	}
}

func TestKeyCache_LRUEviction(t *testing.T) {
	t.Parallel()
	clk := clock.NewFake(time.Unix(0, 0))
	c := newKeyCache(clk, time.Minute, 2)
	for _, id := range []string{"a", "b"} {
		c.put(persistence.APIKeyView{ID: id})
	}
	c.get("a") // a is now most recent
	c.put(persistence.APIKeyView{ID: "c"})
	if _, ok := c.get("b"); ok {
		t.Error("least recently used entry b survived")
	}
	for _, id := range []string{"a", "c"} {
		if _, ok := c.get(id); !ok {
			t.Errorf("entry %s evicted", id)
		}
	}
}

func TestAllowed_RoleMatrix(t *testing.T) {
	t.Parallel()
	want := map[string][]string{ // role → requirements it satisfies
		RoleViewer:     {RoleViewer},
		RoleOperator:   {RoleViewer, RoleOperator},
		RoleAdmin:      {RoleViewer, RoleOperator, RoleAdmin},
		RoleWorker:     {RoleWorker},
		RoleAIReadonly: {RoleAIReadonly},
		"":             {},
		"root":         {},
	}
	for role, ok := range want {
		for _, required := range Roles() {
			if got := Allowed(role, required); got != slices.Contains(ok, required) {
				t.Errorf("Allowed(%q, %q) = %v", role, required, got)
			}
		}
	}
}

func TestBootstrap(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	cfg, _ := GenerateKey(rand.Reader)

	keys := newFakeKeys()
	for range 2 { // idempotent
		if gen, err := Bootstrap(ctx, nil, keys, rand.Reader, "prod", config.Secret(cfg.Token)); err != nil || gen != "" {
			t.Fatalf("Bootstrap(configured) = %q, %v", gen, err)
		}
	}
	if len(keys.keys) != 1 || keys.keys[cfg.ID].Role != RoleAdmin {
		t.Fatalf("keys after two bootstraps = %+v, want the one configured admin", keys.keys)
	}
	a, _ := newTestAuth(keys)
	if p, err := a.VerifyToken(ctx, cfg.Token); err != nil || p.Role != RoleAdmin {
		t.Errorf("configured bootstrap key = %+v, %v", p, err)
	}

	dev := newFakeKeys()
	gen, err := Bootstrap(ctx, nil, dev, rand.Reader, "dev", "")
	if err != nil || !tokenShape.MatchString(gen) || len(dev.keys) != 1 {
		t.Fatalf("dev bootstrap = %q, %v (%d keys)", gen, err, len(dev.keys))
	}
	if again, err := Bootstrap(ctx, nil, dev, rand.Reader, "dev", ""); err != nil || again != "" || len(dev.keys) != 1 {
		t.Errorf("second dev bootstrap = %q, %v (%d keys), want a no-op", again, err, len(dev.keys))
	}
	if gen, err := Bootstrap(ctx, nil, newFakeKeys(), rand.Reader, "test", ""); err != nil || gen != "" {
		t.Errorf("non-dev without a key = %q, %v, want nothing generated", gen, err)
	}
	if _, err := Bootstrap(ctx, nil, newFakeKeys(), rand.Reader, "prod", "ff_not_a_key"); err == nil || strings.Contains(err.Error(), "ff_not_a_key") {
		t.Errorf("malformed bootstrap key = %v, want an error that does not echo the key", err)
	}
}
