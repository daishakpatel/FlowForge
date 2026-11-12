package auth

import (
	"container/list"
	"sync"
	"time"

	"github.com/daishakpatel/flowforge/internal/clock"
	"github.com/daishakpatel/flowforge/internal/persistence"
)

// keyCache is a small LRU of key lookups whose entries expire after ttl, so a key revoked on
// another API instance stops working within ttl. Safe for concurrent use.
type keyCache struct {
	mu    sync.Mutex
	clock clock.Clock
	ttl   time.Duration
	max   int
	order *list.List               // front = most recently used; values are *cacheEntry
	items map[string]*list.Element // key id → element
}

type cacheEntry struct {
	key     persistence.APIKeyView
	expires time.Time
}

func newKeyCache(c clock.Clock, ttl time.Duration, maxEntries int) *keyCache {
	return &keyCache{clock: c, ttl: ttl, max: maxEntries, order: list.New(), items: map[string]*list.Element{}}
}

func (c *keyCache) get(id string) (persistence.APIKeyView, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[id]
	if !ok {
		return persistence.APIKeyView{}, false
	}
	e := el.Value.(*cacheEntry)
	if !c.clock.Now().Before(e.expires) {
		c.order.Remove(el)
		delete(c.items, id)
		return persistence.APIKeyView{}, false
	}
	c.order.MoveToFront(el)
	return e.key, true
}

func (c *keyCache) put(k persistence.APIKeyView) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := &cacheEntry{key: k, expires: c.clock.Now().Add(c.ttl)}
	if el, ok := c.items[k.ID]; ok {
		el.Value = entry
		c.order.MoveToFront(el)
		return
	}
	c.items[k.ID] = c.order.PushFront(entry)
	if c.order.Len() > c.max {
		oldest := c.order.Back()
		c.order.Remove(oldest)
		delete(c.items, oldest.Value.(*cacheEntry).key.ID)
	}
}

func (c *keyCache) remove(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[id]; ok {
		c.order.Remove(el)
		delete(c.items, id)
	}
}
