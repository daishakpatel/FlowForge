package config

import (
	"strconv"
	"strings"
	"time"
)

// Scheduler configures flowforge-scheduler (00E §6), including the outbox relay it hosts.
type Scheduler struct {
	Common
	PGDSN         Secret        `env:"FF_PG_DSN" default:"postgres://flowforge:flowforge@localhost:5432/flowforge?sslmode=disable" validate:"dsn"`
	RaftEndpoints []string      `env:"FF_RAFT_ENDPOINTS" default:"raft-1:7000,raft-2:7000,raft-3:7000" validate:"addr"`
	HTTPAddr      string        `env:"FF_SCHED_HTTP_ADDR" default:":8081" validate:"addr"` // /metrics, /healthz, /readyz
	Tick          time.Duration `env:"FF_SCHED_TICK" default:"500ms" validate:"positive"`
	QueueLimits   []string      `env:"FF_QUEUE_LIMITS" default:"" validate:"queue_limits"` // queue=n,…; see QueueLimitMap
	// WorkerLeaseTTL and WorkerHeartbeat are the workers' settings, which worker-loss recovery
	// (03.04) uses: a lease expires after the TTL, and an attempt with no lease is abandoned
	// after 3×heartbeat + TTL of silence.
	WorkerLeaseTTL  time.Duration `env:"FF_WORKER_LEASE_TTL" default:"15s" validate:"positive"`
	WorkerHeartbeat time.Duration `env:"FF_WORKER_HEARTBEAT" default:"5s" validate:"positive"`
	LeaderTTL       time.Duration `env:"FF_LEADER_TTL" default:"10s" validate:"positive"`
	LeaderRenew     time.Duration `env:"FF_LEADER_RENEW" default:"3s" validate:"positive"`
	KafkaBrokers    []string      `env:"FF_KAFKA_BROKERS" default:"localhost:9092" validate:"addr"` // outbox relay
}

// QueueLimitMap returns FF_QUEUE_LIMITS as queue → the most QUEUED+RUNNING tasks that queue may
// hold across all runs. A queue not listed is unlimited; a queue listed twice keeps its last
// value. Load has validated every item, so none is skipped here.
func (c Scheduler) QueueLimitMap() map[string]int {
	out := make(map[string]int, len(c.QueueLimits))
	for _, item := range c.QueueLimits {
		if queue, n, ok := parseQueueLimit(item); ok {
			out[queue] = n
		}
	}
	return out
}

// parseQueueLimit parses one "queue=n" item with a non-empty queue and n > 0.
func parseQueueLimit(item string) (string, int, bool) {
	queue, num, found := strings.Cut(item, "=")
	queue = strings.TrimSpace(queue)
	n, err := strconv.Atoi(strings.TrimSpace(num))
	return queue, n, found && queue != "" && err == nil && n > 0
}

// Redacted returns every variable with secrets masked, for the startup log.
func (c Scheduler) Redacted() map[string]string { return redacted(c) }

func (c Scheduler) crossCheck() []FieldError {
	return append(renewBelowHalf("FF_LEADER_RENEW", c.LeaderRenew, "FF_LEADER_TTL", c.LeaderTTL),
		renewBelowHalf("FF_WORKER_HEARTBEAT", c.WorkerHeartbeat, "FF_WORKER_LEASE_TTL", c.WorkerLeaseTTL)...)
}
