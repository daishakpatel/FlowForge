package rest

import (
	"encoding/base64"
	"net/http"
	"strconv"

	"github.com/daishakpatel/flowforge/internal/apperrors"
	"github.com/daishakpatel/flowforge/internal/persistence"
)

// listResponse is every list endpoint's body: {"items":[…],"next_cursor":"…"|null} (00E §9).
type listResponse[T any] struct {
	Items      []T     `json:"items"`
	NextCursor *string `json:"next_cursor"`
}

func newList[T any](items []T, next string) listResponse[T] {
	if items == nil {
		items = []T{}
	}
	l := listResponse[T]{Items: items}
	if next != "" {
		l.NextCursor = &next
	}
	return l
}

// mapList converts views to response items.
func mapList[V, T any](views []V, f func(V) T) []T {
	out := make([]T, len(views))
	for i, v := range views {
		out[i] = f(v)
	}
	return out
}

// pageParams reads ?limit= and ?cursor=. limit is optional (default 50); a value above 200 is
// clamped to 200, and anything that is not a positive integer is INVALID_ARGUMENT (FR-API-05).
func pageParams(r *http.Request) (persistence.Page, error) {
	q := r.URL.Query()
	limit, err := parseLimit(q.Get("limit"))
	return persistence.Page{Limit: limit, Cursor: q.Get("cursor")}, err
}

func parseLimit(s string) (int, error) {
	if s == "" {
		return persistence.DefaultPageLimit, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 1 {
		return 0, apperrors.InvalidArgument("limit must be a positive integer")
	}
	return min(n, persistence.MaxPageLimit), nil
}

// The events list pages on workflow_events.id, encoded opaquely like the keyset cursors.

func encodeEventCursor(id int64) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatInt(id, 10)))
}

func decodeEventCursor(cursor string) (int64, error) {
	if cursor == "" {
		return 0, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err == nil {
		if id, perr := strconv.ParseInt(string(raw), 10, 64); perr == nil && id > 0 {
			return id, nil
		}
	}
	return 0, apperrors.InvalidArgument("invalid cursor")
}
