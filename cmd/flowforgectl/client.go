package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// client is a thin typed client of the FlowForge REST API (/api/v1). Responses are decoded into
// generic JSON values: the CLI only prints them, so it needs no copy of the API's types.
type client struct {
	base string // FF_API_URL, without a trailing slash
	key  string // FF_API_KEY, sent as a bearer token
	http *http.Client
}

func newClient(base, key string) *client {
	return &client{base: strings.TrimRight(base, "/"), key: key, http: &http.Client{Timeout: 60 * time.Second}}
}

// apiError is a non-2xx answer: its status and the 00E §4 error envelope.
type apiError struct {
	Status    int    `json:"-"` // the HTTP status, printed in the summary line only
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id"`
	Details   []any  `json:"details"`
}

func (e *apiError) Error() string {
	msg := fmt.Sprintf("%d %s: %s", e.Status, e.Code, e.Message)
	if e.RequestID != "" {
		msg += " (request_id " + e.RequestID + ")"
	}
	return msg
}

// do sends one request with an optional JSON body (raw []byte is sent as is) and decodes a JSON
// answer into out (when non-nil). A non-2xx answer is an *apiError.
func (c *client) do(ctx context.Context, method, path string, body any, out any, headers ...string) error {
	var reader io.Reader
	switch b := body.(type) {
	case nil:
	case []byte:
		reader = bytes.NewReader(b)
	default:
		raw, err := json.Marshal(b)
		if err != nil {
			return fmt.Errorf("encode request: %w", err)
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, reader)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	if reader != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.key != "" {
		req.Header.Set("Authorization", "Bearer "+c.key)
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }() // the body is fully read below
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode >= 300 {
		var env struct {
			Error apiError `json:"error"`
		}
		if json.Unmarshal(raw, &env) != nil || env.Error.Code == "" {
			env.Error = apiError{Code: http.StatusText(resp.StatusCode), Message: strings.TrimSpace(string(raw))}
		}
		env.Error.Status = resp.StatusCode
		return &env.Error
	}
	if out != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
	}
	return nil
}
