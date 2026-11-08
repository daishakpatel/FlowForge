// Package migrations embeds the goose SQL migrations so every binary carries its schema.
// It lives beside the SQL because //go:embed can't reach parent directories; use it through
// internal/persistence/migrate.
package migrations

import "embed"

// FS holds one directory per database: flowforge/ and marketdata/ (00E §2, §12).
//
//go:embed flowforge marketdata
var FS embed.FS
