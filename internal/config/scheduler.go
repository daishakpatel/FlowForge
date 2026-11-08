package config

import "time"

// Scheduler configures flowforge-scheduler (00E §6), including the outbox relay it hosts.
type Scheduler struct {
	Common
	PGDSN         Secret        `env:"FF_PG_DSN" default:"postgres://flowforge:flowforge@localhost:5432/flowforge?sslmode=disable" validate:"dsn"`
	RaftEndpoints []string      `env:"FF_RAFT_ENDPOINTS" default:"raft-1:7000,raft-2:7000,raft-3:7000" validate:"addr"`
	Tick          time.Duration `env:"FF_SCHED_TICK" default:"500ms" validate:"positive"`
	LeaderTTL     time.Duration `env:"FF_LEADER_TTL" default:"10s" validate:"positive"`
	LeaderRenew   time.Duration `env:"FF_LEADER_RENEW" default:"3s" validate:"positive"`
	KafkaBrokers  []string      `env:"FF_KAFKA_BROKERS" default:"localhost:9092" validate:"addr"` // outbox relay
}

// Redacted returns every variable with secrets masked, for the startup log.
func (c Scheduler) Redacted() map[string]string { return redacted(c) }

func (c Scheduler) crossCheck() []FieldError {
	return renewBelowHalf("FF_LEADER_RENEW", c.LeaderRenew, "FF_LEADER_TTL", c.LeaderTTL)
}
