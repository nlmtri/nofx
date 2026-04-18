package wsoverride

import (
	"sync"
	"time"
)

// restResponseCache is a simple keyed TTL cache for raw JSON responses from
// endpoints that don't have a WS equivalent (openInterest, exchangeInfo).
type restResponseCache struct {
	entries sync.Map // key -> *restCacheEntry
}

type restCacheEntry struct {
	body      []byte
	fetchedAt time.Time
	ttl       time.Duration
}

func newRESTResponseCache() *restResponseCache {
	return &restResponseCache{}
}

func (c *restResponseCache) get(key string) ([]byte, bool) {
	v, ok := c.entries.Load(key)
	if !ok {
		return nil, false
	}
	e := v.(*restCacheEntry)
	if time.Since(e.fetchedAt) > e.ttl {
		return nil, false
	}
	return e.body, true
}

func (c *restResponseCache) set(key string, body []byte, ttl time.Duration) {
	cp := make([]byte, len(body))
	copy(cp, body)
	c.entries.Store(key, &restCacheEntry{
		body:      cp,
		fetchedAt: time.Now(),
		ttl:       ttl,
	})
}
