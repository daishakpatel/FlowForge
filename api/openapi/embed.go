// Package openapi embeds the FlowForge REST contract (task 02.04), the source of truth for the
// API: flowforge-api serves it at /openapi.json and its tests check routes and responses against it.
package openapi

import _ "embed"

// Spec is flowforge.v1.yaml, the OpenAPI 3.1 document.
//
//go:embed flowforge.v1.yaml
var Spec []byte
