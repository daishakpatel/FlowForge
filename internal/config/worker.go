package config

import "time"

// Worker configures flowforge-worker (00E §6). Identity, queues, capacity and the API target
// have no default: every worker instance sets them.
type Worker struct {
	Common
	ID            string        `env:"FF_WORKER_ID" validate:"required"`
	Queues        []string      `env:"FF_WORKER_QUEUES" validate:"required"`
	Capacity      int           `env:"FF_WORKER_CAPACITY" validate:"positive"`
	APIGRPCTarget string        `env:"FF_API_GRPC_TARGET" validate:"required,addr"`
	Token         Secret        `env:"FF_WORKER_TOKEN"`
	LeaseTTL      time.Duration `env:"FF_WORKER_LEASE_TTL" default:"15s" validate:"positive"`
	Heartbeat     time.Duration `env:"FF_WORKER_HEARTBEAT" default:"5s" validate:"positive"`
	KafkaBrokers  []string      `env:"FF_KAFKA_BROKERS" default:"localhost:9092" validate:"addr"`
	AIURL         string        `env:"FF_AI_URL" default:"http://ai-ops:8000" validate:"url"`
	AIToken       Secret        `env:"FF_AI_TOKEN"`
}

// Redacted returns every variable with secrets masked, for the startup log.
func (c Worker) Redacted() map[string]string { return redacted(c) }

func (c Worker) crossCheck() []FieldError {
	return renewBelowHalf("FF_WORKER_HEARTBEAT", c.Heartbeat, "FF_WORKER_LEASE_TTL", c.LeaseTTL)
}
