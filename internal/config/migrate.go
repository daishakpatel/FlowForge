package config

// Migrate configures cmd/flowforge-migrate: the DSN of each migration set. It reads no
// variable of its own, so it adds nothing to 00E §6.
type Migrate struct {
	Common
	PGDSN Secret `env:"FF_PG_DSN" default:"postgres://flowforge:flowforge@localhost:5432/flowforge?sslmode=disable" validate:"dsn"`
	MDDSN Secret `env:"FF_MD_PG_DSN" default:"postgres://md:md@localhost:5433/marketdata?sslmode=disable" validate:"dsn"`
}

// Redacted returns every variable with secrets masked, for the startup log.
func (c Migrate) Redacted() map[string]string { return redacted(c) }
