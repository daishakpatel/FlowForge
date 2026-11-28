package config

// Producer configures flowforge-producer, the market-data tick producer (task 05.03; 00E §6). Its
// flags (docs/architecture/market-data-generator.md) override Rate.
type Producer struct {
	Common
	Kafka
	Rate     int    `env:"FF_PRODUCER_RATE" default:"100" validate:"positive"`    // ticks per second
	HTTPAddr string `env:"FF_PRODUCER_HTTP_ADDR" default:":8084" validate:"addr"` // /metrics, /healthz, /readyz
}

// Redacted returns every variable with secrets masked, for the startup log.
func (c Producer) Redacted() map[string]string { return redacted(c) }
