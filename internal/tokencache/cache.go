// Package tokencache tracks which identity (namespace + task queue) a
// Temporal task token was issued to, so that the proxy can validate
// Respond*/Heartbeat calls that reference an opaque token it cannot decode
// itself.
package tokencache

import (
	"container/list"
	"hash/maphash"
	"sync"
	"time"
)

const numShards = 32

// Entry is the identity a task token was issued to.
type Entry struct {
	Namespace string
	TaskQueue string
}

type item struct {
	key       string
	entry     Entry
	expiresAt time.Time
}

type shard struct {
	mu      sync.Mutex
	items   map[string]*list.Element // key -> element in ll
	ll      *list.List               // front = most recently used
	maxSize int
}

func newShard(maxSize int) *shard {
	return &shard{
		items:   make(map[string]*list.Element),
		ll:      list.New(),
		maxSize: maxSize,
	}
}

func (s *shard) put(key string, entry Entry, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	expiresAt := time.Now().Add(ttl)
	if el, ok := s.items[key]; ok {
		el.Value.(*item).entry = entry
		el.Value.(*item).expiresAt = expiresAt
		s.ll.MoveToFront(el)
		return
	}

	el := s.ll.PushFront(&item{key: key, entry: entry, expiresAt: expiresAt})
	s.items[key] = el

	for s.maxSize > 0 && len(s.items) > s.maxSize {
		back := s.ll.Back()
		if back == nil {
			break
		}
		s.removeElement(back)
	}
}

// get returns the entry for key, sliding its TTL forward and marking it most
// recently used on a hit.
func (s *shard) get(key string, ttl time.Duration) (Entry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	el, ok := s.items[key]
	if !ok {
		return Entry{}, false
	}
	it := el.Value.(*item)
	if time.Now().After(it.expiresAt) {
		s.removeElement(el)
		return Entry{}, false
	}
	it.expiresAt = time.Now().Add(ttl)
	s.ll.MoveToFront(el)
	return it.entry, true
}

func (s *shard) delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if el, ok := s.items[key]; ok {
		s.removeElement(el)
	}
}

// removeElement must be called with s.mu held.
func (s *shard) removeElement(el *list.Element) {
	s.ll.Remove(el)
	delete(s.items, el.Value.(*item).key)
}

func (s *shard) evictExpired(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for el := s.ll.Back(); el != nil; {
		prev := el.Prev()
		if now.After(el.Value.(*item).expiresAt) {
			s.removeElement(el)
		}
		el = prev
	}
}

// Cache is a sharded, TTL + LRU-bounded map from task token to the identity
// it was issued to. It is safe for concurrent use by many goroutines.
//
// A proxy restart loses all entries: any in-flight Respond/Heartbeat call
// against a task polled before the restart will be denied as an unknown
// token until the worker's next successful Poll re-establishes one. Set the
// TTL to at least as long as the longest expected activity duration if that
// activity does not heartbeat more often than the TTL, since Get slides the
// TTL forward on every heartbeat but an idle token will otherwise expire.
type Cache struct {
	shards      [numShards]*shard
	ttl         time.Duration
	seed        maphash.Seed
	stopJanitor chan struct{}
}

// New creates a Cache with the given sliding TTL per entry and an
// approximate global size cap (enforced per-shard via LRU eviction, so the
// effective cap is maxSize rounded up to a multiple of the shard count).
func New(ttl time.Duration, maxSize int) *Cache {
	perShard := maxSize / numShards
	if perShard < 1 {
		perShard = 1
	}

	c := &Cache{
		ttl:         ttl,
		seed:        maphash.MakeSeed(),
		stopJanitor: make(chan struct{}),
	}
	for i := range c.shards {
		c.shards[i] = newShard(perShard)
	}

	interval := ttl / 4
	if interval < 15*time.Second {
		interval = 15 * time.Second
	}
	go c.runJanitor(interval)

	return c
}

// Close stops the background janitor goroutine. Safe to call once.
func (c *Cache) Close() {
	close(c.stopJanitor)
}

func (c *Cache) runJanitor(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-c.stopJanitor:
			return
		case now := <-ticker.C:
			for _, s := range c.shards {
				s.evictExpired(now)
			}
		}
	}
}

func (c *Cache) shardFor(token []byte) *shard {
	var h maphash.Hash
	h.SetSeed(c.seed)
	h.Write(token)
	return c.shards[h.Sum64()%numShards]
}

// Put registers token as belonging to entry, sliding its expiry ttl forward
// from now.
func (c *Cache) Put(token []byte, entry Entry) {
	c.shardFor(token).put(string(token), entry, c.ttl)
}

// Get looks up token, returning its entry and true on a live hit. A hit
// slides the token's TTL forward from now.
func (c *Cache) Get(token []byte) (Entry, bool) {
	return c.shardFor(token).get(string(token), c.ttl)
}

// Delete removes token, e.g. once a terminal Respond call has consumed it.
func (c *Cache) Delete(token []byte) {
	c.shardFor(token).delete(string(token))
}
