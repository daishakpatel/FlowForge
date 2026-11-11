package persistence

import (
	"encoding/base64"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/daishakpatel/flowforge/internal/apperrors"
)

// Page limits (task 02.04: default 50, max 200).
const (
	DefaultPageLimit = 50
	MaxPageLimit     = 200
)

// Page asks for one keyset page: up to Limit rows after Cursor ("" for the first page).
type Page struct {
	Limit  int // <= 0 means DefaultPageLimit; above MaxPageLimit is clamped
	Cursor string
}

// EffectiveLimit returns Limit with the default and the maximum applied.
func (p Page) EffectiveLimit() int {
	switch {
	case p.Limit <= 0:
		return DefaultPageLimit
	case p.Limit > MaxPageLimit:
		return MaxPageLimit
	}
	return p.Limit
}

// EncodeCursor returns the opaque cursor for the keyset position (createdAt, id) of lists
// ordered (created_at DESC, id DESC). It keeps microseconds, the precision of timestamptz.
func EncodeCursor(createdAt time.Time, id string) string {
	raw := strconv.FormatInt(createdAt.UnixMicro(), 10) + "|" + id
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// DecodeCursor reverses EncodeCursor. A cursor that is not one is INVALID_ARGUMENT.
func DecodeCursor(cursor string) (time.Time, string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err == nil {
		micros, id, ok := strings.Cut(string(raw), "|")
		if n, perr := strconv.ParseInt(micros, 10, 64); ok && perr == nil && id != "" {
			return time.UnixMicro(n).UTC(), id, nil
		}
		err = errors.New("malformed cursor")
	}
	return time.Time{}, "", &apperrors.Error{Code: apperrors.CodeInvalidArgument, Message: "invalid cursor", Cause: err}
}
