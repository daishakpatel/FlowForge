package rest

import (
	"encoding/json"
	"fmt"
	"net/http"

	"go.yaml.in/yaml/v3"

	"github.com/daishakpatel/flowforge/api/openapi"
)

// specJSON converts the embedded OpenAPI YAML to JSON, once at router construction.
func specJSON() ([]byte, error) {
	var doc any
	if err := yaml.Unmarshal(openapi.Spec, &doc); err != nil {
		return nil, fmt.Errorf("parse openapi spec: %w", err)
	}
	b, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("encode openapi spec as JSON: %w", err)
	}
	return b, nil
}

// serveSpec serves GET /openapi.json (FR-API-03).
func serveSpec(spec []byte) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(spec) // the client may be gone; nothing to do
	}
}
