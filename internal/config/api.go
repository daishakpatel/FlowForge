package config

import "time"

// API configures flowforge-api (00E §6).
type API struct {
	Common
	PGDSN             Secret        `env:"FF_PG_DSN" default:"postgres://flowforge:flowforge@localhost:5432/flowforge?sslmode=disable" validate:"dsn"`
	MDDSN             Secret        `env:"FF_MD_PG_DSN" default:"postgres://md:md@localhost:5433/marketdata?sslmode=disable" validate:"dsn"` // analytics read API
	HTTPAddr          string        `env:"FF_API_HTTP_ADDR" default:":8080" validate:"addr"`
	GRPCAddr          string        `env:"FF_API_GRPC_ADDR" default:":9090" validate:"addr"`
	AdminBootstrapKey Secret        `env:"FF_ADMIN_BOOTSTRAP_KEY" validate:"required_in_prod"`
	RaftEndpoints     []string      `env:"FF_RAFT_ENDPOINTS" default:"raft-1:7000,raft-2:7000,raft-3:7000" validate:"addr"`
	WorkerLeaseTTL    time.Duration `env:"FF_WORKER_LEASE_TTL" default:"15s" validate:"positive"`
	WorkerHeartbeat   time.Duration `env:"FF_WORKER_HEARTBEAT" default:"5s" validate:"positive"`
}

// Redacted returns every variable with secrets masked, for the startup log.
func (c API) Redacted() map[string]string { return redacted(c) }

func (c API) crossCheck() []FieldError {
	return renewBelowHalf("FF_WORKER_HEARTBEAT", c.WorkerHeartbeat, "FF_WORKER_LEASE_TTL", c.WorkerLeaseTTL)
}
