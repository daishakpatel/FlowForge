package config

// Processor configures marketdata-processor (00E §6).
type Processor struct {
	Common
	MDDSN        Secret   `env:"FF_MD_PG_DSN" default:"postgres://md:md@localhost:5433/marketdata?sslmode=disable" validate:"dsn"`
	KafkaBrokers []string `env:"FF_KAFKA_BROKERS" default:"localhost:9092" validate:"addr"`
}

// Redacted returns every variable with secrets masked, for the startup log.
func (c Processor) Redacted() map[string]string { return redacted(c) }
