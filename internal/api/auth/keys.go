package auth

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base32"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strings"
)

// API key format (ADR-015, task 02.05): ff_<keyid>_<secret>, keyid = 12 lowercase base32
// characters, secret = 32 random bytes in unpadded base64url (43 characters, which may contain
// '_' and '-'). The server stores only salt (16 bytes) and SHA-256(salt ‖ secret).
const (
	tokenPrefix = "ff_"
	keyIDLen    = 12
	secretBytes = 32
	saltBytes   = 16
)

const keyIDAlphabet = "abcdefghijklmnopqrstuvwxyz234567"

// ErrMalformedKey is returned by ParseToken for text that is not an API key.
var ErrMalformedKey = errors.New("malformed API key")

// NewKey is a freshly generated or parsed key, ready to store. Token is the only place the
// secret appears: show it once, never log or store it.
type NewKey struct {
	ID    string
	Token string
	Salt  []byte
	Hash  []byte
}

// GenerateKey returns a new random key; rnd must be a cryptographic source (crypto/rand.Reader).
func GenerateKey(rnd io.Reader) (NewKey, error) {
	raw := make([]byte, 8+secretBytes)
	if _, err := io.ReadFull(rnd, raw); err != nil {
		return NewKey{}, fmt.Errorf("read key randomness: %w", err)
	}
	id := base32.NewEncoding(keyIDAlphabet).WithPadding(base32.NoPadding).EncodeToString(raw[:8])[:keyIDLen]
	secret := base64.RawURLEncoding.EncodeToString(raw[8:])
	return withSalt(rnd, id, secret)
}

// HashToken validates an existing token (the configured bootstrap key) and salts and hashes it.
func HashToken(rnd io.Reader, token string) (NewKey, error) {
	id, secret, err := ParseToken(token)
	if err != nil {
		return NewKey{}, err
	}
	return withSalt(rnd, id, secret)
}

func withSalt(rnd io.Reader, id, secret string) (NewKey, error) {
	salt := make([]byte, saltBytes)
	if _, err := io.ReadFull(rnd, salt); err != nil {
		return NewKey{}, fmt.Errorf("read salt: %w", err)
	}
	return NewKey{ID: id, Token: tokenPrefix + id + "_" + secret, Salt: salt, Hash: hashSecret(salt, secret)}, nil
}

// ParseToken splits ff_<keyid>_<secret> and checks both parts' shape. The error never contains
// the token.
func ParseToken(token string) (id, secret string, err error) {
	rest, ok := strings.CutPrefix(token, tokenPrefix)
	if !ok || len(rest) < keyIDLen+1 || rest[keyIDLen] != '_' {
		return "", "", ErrMalformedKey
	}
	id, secret = rest[:keyIDLen], rest[keyIDLen+1:]
	if strings.Trim(id, keyIDAlphabet) != "" {
		return "", "", ErrMalformedKey
	}
	if b, err := base64.RawURLEncoding.DecodeString(secret); err != nil || len(b) != secretBytes {
		return "", "", ErrMalformedKey
	}
	return id, secret, nil
}

func hashSecret(salt []byte, secret string) []byte {
	h := sha256.New()
	h.Write(salt)
	h.Write([]byte(secret))
	return h.Sum(nil)
}

// verifySecret reports whether secret matches salt and hash, in constant time.
func verifySecret(salt, hash []byte, secret string) bool {
	return subtle.ConstantTimeCompare(hashSecret(salt, secret), hash) == 1
}
