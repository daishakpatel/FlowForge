// Package schemas embeds the event JSON Schemas (task 05.01) so every binary validates against the
// committed files. It lives beside them because //go:embed can't reach parent directories; use it
// through pkg/events (NewRegistry). baseline/ is read only by the compatibility test.
package schemas

import "embed"

// Events holds events/<name>.schema.json (JSON Schema draft 2020-12), one file per schema version.
//
//go:embed events/*.schema.json
var Events embed.FS
