package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Consumer fetch bounds: each Poll holds at most this much data in memory.
const (
	fetchMaxBytes          = 16 << 20 // 16 MiB per fetch response
	fetchMaxPartitionBytes = 1 << 20  // 1 MiB per partition
)

// ErrClosed is returned by Poll after Close.
var ErrClosed = errors.New("kafka consumer closed")

// Consumer reads topics as a member of a consumer group. Offsets are committed only by Commit,
// after the caller has durably handled the records (commit-after-write, ADR-011): a crash before
// Commit redelivers them, so delivery is at least once and handlers dedupe on event_id. A new
// group starts at the earliest offset.
//
// A batch returned by Poll keeps its partitions until the next Poll: rebalances wait, so the batch
// is never handled by two members at once and its Commit can never land after a newer owner's,
// moving the group's offset backwards. The loop is therefore Poll, handle, Commit, Poll; handling a
// batch must take less than the group's rebalance timeout (franz-go's default, 60 s), or the broker
// removes the member. Poll and Commit must be called from one goroutine.
type Consumer struct {
	cl *kgo.Client
}

// NewConsumer returns a consumer of topics in group. It does not dial: the first Poll joins the
// group.
func NewConsumer(cfg Config, group string, topics ...string) (*Consumer, error) {
	if group == "" || len(topics) == 0 {
		return nil, errors.New("kafka consumer: a group and at least one topic are required")
	}
	base, err := clientOpts(cfg)
	if err != nil {
		return nil, err
	}
	base = append(base,
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxBytes(fetchMaxBytes),
		kgo.FetchMaxPartitionBytes(fetchMaxPartitionBytes),
	)
	cl, err := kgo.NewClient(base...)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer: %w", err)
	}
	return &Consumer{cl: cl}, nil
}

// Poll waits for records (at most maxRecords; <= 0 means all buffered) until ctx ends. Records of
// one partition come in offset order. It first releases the previous batch's partitions, so a
// pending rebalance can run; commit that batch before calling Poll again. It returns the records fetched so far together with any
// fetch error; ctx ending is not an error. Use ExtractTrace on a record to continue its trace.
func (c *Consumer) Poll(ctx context.Context, maxRecords int) ([]*kgo.Record, error) {
	c.cl.AllowRebalance()
	fs := c.cl.PollRecords(ctx, maxRecords)
	if fs.IsClientClosed() {
		return nil, ErrClosed
	}
	var errs []error
	fs.EachError(func(topic string, partition int32, err error) {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			errs = append(errs, fmt.Errorf("fetch %s/%d: %w", topic, partition, err))
		}
	})
	return fs.Records(), errors.Join(errs...)
}

// Commit commits the offsets after recs (for each partition, the highest offset in recs plus one).
// Call it only once the records' effects are durable.
func (c *Consumer) Commit(ctx context.Context, recs ...*kgo.Record) error {
	if len(recs) == 0 {
		return nil
	}
	if err := c.cl.CommitRecords(ctx, recs...); err != nil {
		return fmt.Errorf("kafka commit: %w", err)
	}
	return nil
}

// Close leaves the group (its partitions move to other members) and closes the client.
// Uncommitted records are redelivered to the next owner.
func (c *Consumer) Close() {
	c.cl.AllowRebalance() // leaving the group waits for a held batch otherwise
	c.cl.Close()
}
