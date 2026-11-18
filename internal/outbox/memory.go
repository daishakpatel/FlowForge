package outbox

import (
	"context"
	"slices"
	"sync"
)

// MemoryPublisher records every message it is given, in order, and never fails. Tests use it,
// wrapped to inject failures. Safe for concurrent use; the zero value is ready.
type MemoryPublisher struct {
	mu   sync.Mutex
	msgs []Message
}

var _ Publisher = (*MemoryPublisher)(nil)

// Publish implements Publisher.
func (p *MemoryPublisher) Publish(ctx context.Context, msgs []Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.msgs = append(p.msgs, msgs...)
	return nil
}

// Messages returns a copy of everything published so far, duplicates included.
func (p *MemoryPublisher) Messages() []Message {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.msgs)
}
