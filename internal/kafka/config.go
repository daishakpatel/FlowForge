// Package kafka wraps franz-go (ADR-010) for every FlowForge producer and consumer (task 05.01):
// clients with the project's delivery settings, W3C traceparent headers, and the idempotent topic
// bootstrap behind `make topics`. Topics and their keys are in 00E §7 and deploy/kafka/topics.yaml.
package kafka

import (
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/daishakpatel/flowforge/internal/config"
)

// Config is the client configuration, loaded from the environment with config.Load[config.Kafka]
// (FF_KAFKA_BROKERS, FF_KAFKA_CLIENT_ID, FF_KAFKA_SECURITY_PROTOCOL; 00E §6).
type Config = config.Kafka

// dialTimeout bounds each connection attempt to a broker.
const dialTimeout = 5 * time.Second

// clientOpts returns the options every client shares. It rejects a config the loader would have
// rejected too, for callers that build a Config by hand.
func clientOpts(cfg Config) ([]kgo.Opt, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("kafka: no brokers (FF_KAFKA_BROKERS)")
	}
	if cfg.SecurityProtocol != "PLAINTEXT" {
		return nil, fmt.Errorf("kafka: security protocol %q is not supported yet (only PLAINTEXT until task 09.01)", cfg.SecurityProtocol)
	}
	return []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.DialTimeout(dialTimeout),
	}, nil
}

// NewAdmin returns an admin client (topic management). Close it when done.
func NewAdmin(cfg Config) (*kadm.Client, error) {
	opts, err := clientOpts(cfg)
	if err != nil {
		return nil, err
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka admin client: %w", err)
	}
	return kadm.NewClient(cl), nil
}
