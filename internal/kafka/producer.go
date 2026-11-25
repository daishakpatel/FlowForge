package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer delivery settings (ADR-009/ADR-010). Idempotent writes are franz-go's default and stay
// on: with acks=all, a retried batch is written once and per-partition order holds across retries.
const (
	producerLinger       = 5 * time.Millisecond // batch briefly; bounded so latency stays low
	producerBatchBytes   = 1 << 20              // 1 MiB, the broker's default max message size
	producerBufferedRecs = 10_000               // Produce blocks beyond this: memory is bounded
	produceTimeout       = 10 * time.Second     // one produce request
	deliveryTimeout      = 30 * time.Second     // a record's total time, retries included
	closeFlushTimeout    = 10 * time.Second     // Close waits this long for buffered records
)

// Producer publishes records with acks=all and idempotence. It is safe for concurrent use.
// Records go to the partition of their key (the same murmur2 hashing as the Java client), so one
// key's records stay in order (00E §7: key = symbol or run_id).
type Producer struct {
	cl *kgo.Client
}

// NewProducer returns a producer for cfg; opts are appended and override the defaults (tests,
// 05.03's rate settings). It does not dial: the first Produce connects.
func NewProducer(cfg Config, opts ...kgo.Opt) (*Producer, error) {
	base, err := clientOpts(cfg)
	if err != nil {
		return nil, err
	}
	base = append(base,
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(producerLinger),
		kgo.ProducerBatchMaxBytes(producerBatchBytes),
		kgo.MaxBufferedRecords(producerBufferedRecs),
		kgo.ProduceRequestTimeout(produceTimeout),
		kgo.RecordDeliveryTimeout(deliveryTimeout),
	)
	cl, err := kgo.NewClient(append(base, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}
	return &Producer{cl: cl}, nil
}

// Produce publishes recs and waits until every one is acknowledged by all in-sync replicas, ctx
// ends, or a record fails. Each record gets the traceparent in ctx (InjectTrace). On error some
// records may have been written: callers retry the whole call, and consumers dedupe on event_id.
func (p *Producer) Produce(ctx context.Context, recs ...*kgo.Record) error {
	for _, r := range recs {
		InjectTrace(ctx, r)
	}
	if err := p.cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		return fmt.Errorf("kafka produce: %w", err)
	}
	return nil
}

// Close flushes buffered records (for at most closeFlushTimeout, or until ctx ends) and closes the
// client. Records still unacknowledged then fail.
func (p *Producer) Close(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, closeFlushTimeout)
	defer cancel()
	_ = p.cl.Flush(ctx) // best effort: Close fails whatever is still buffered, and Produce reported each record's fate
	p.cl.Close()
}
