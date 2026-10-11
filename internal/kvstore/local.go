// Package kvstore provides the storage primitives behind the proxy's caches
// (the task token cache and the tool verifier's input-event state): a local,
// sharded TTL + LRU map, and a TTL-aware Valkey string client.
package kvstore

import (
	"container/list"
	"hash/maphash"
	"sync"
	"time"
)

const numShards = 32

type item[V any] struct {
	key       string
	value     V
	expiresAt time.Time
}

type shard[V any] struct {
	mu      sync.Mutex
	items   map[string]*list.Element // key -> element in ll
	ll      *list.List               // front = most recently used
	maxSize int
}

func newShard[V any](maxSize int) *shard[V] {
	return &shard[V]{
		items:   make(map[string]*list.Element),
		ll:      list.New(),
		maxSize: maxSize,
	}
}

func (s *shard[V]) put(key string, value V, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	expiresAt := time.Now().Add(ttl)
	if el, ok := s.items[key]; ok {
		it := el.Value.(*item[V])
		it.value = value
		it.expiresAt = expiresAt
		s.ll.MoveToFront(el)
		return
	}

	el := s.ll.PushFront(&item[V]{key: key, value: value, expiresAt: expiresAt})
	s.items[key] = el

	for s.maxSize > 0 && len(s.items) > s.maxSize {
		back := s.ll.Back()
		if back == nil {
			break
		}
		s.removeElement(back)
	}
}

// get returns the value for key, sliding its TTL forward and marking it most
// recently used on a hit.
func (s *shard[V]) get(key string, ttl time.Duration) (V, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	el, ok := s.items[key]
	if !ok {
		var zero V
		return zero, false
	}
	it := el.Value.(*item[V])
	if time.Now().After(it.expiresAt) {
		s.removeElement(el)
		var zero V
		return zero, false
	}
	it.expiresAt = time.Now().Add(ttl)
	s.ll.MoveToFront(el)
	return it.value, true
}

func (s *shard[V]) delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if el, ok := s.items[key]; ok {
		s.removeElement(el)
	}
}

// removeElement must be called with s.mu held.
func (s *shard[V]) removeElement(el *list.Element) {
	s.ll.Remove(el)
	delete(s.items, el.Value.(*item[V]).key)
}

func (s *shard[V]) evictExpired(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for el := s.ll.Back(); el != nil; {
		prev := el.Prev()
		if now.After(el.Value.(*item[V]).expiresAt) {
			s.removeElement(el)
		}
		el = prev
	}
}

// Local is a sharded, TTL + LRU-bounded map. It is safe for concurrent use by
// many goroutines. Entries are lost when the process restarts.
type Local[V any] struct {
	shards      [numShards]*shard[V]
	ttl         time.Duration
	seed        maphash.Seed
	stopJanitor chan struct{}
}

// NewLocal creates a Local with the given sliding TTL per entry and an
// approximate global size cap (enforced per-shard via LRU eviction, so the
// effective cap is maxSize rounded up to a multiple of the shard count).
func NewLocal[V any](ttl time.Duration, maxSize int) *Local[V] {
	perShard := max(maxSize/numShards, 1)
	l := &Local[V]{
		ttl:         ttl,
		seed:        maphash.MakeSeed(),
		stopJanitor: make(chan struct{}),
	}
	for i := range l.shards {
		l.shards[i] = newShard[V](perShard)
	}

	interval := max(ttl/4, 15*time.Second)
	go l.runJanitor(interval)

	return l
}

// Close stops the background janitor goroutine. Safe to call once.
func (l *Local[V]) Close() error {
	close(l.stopJanitor)
	return nil
}

func (l *Local[V]) runJanitor(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-l.stopJanitor:
			return
		case now := <-ticker.C:
			for _, s := range l.shards {
				s.evictExpired(now)
			}
		}
	}
}

func (l *Local[V]) shardFor(key string) *shard[V] {
	var h maphash.Hash
	h.SetSeed(l.seed)
	h.WriteString(key)
	return l.shards[h.Sum64()%numShards]
}

// Put stores value under key, sliding its expiry ttl forward from now.
func (l *Local[V]) Put(key string, value V) {
	l.shardFor(key).put(key, value, l.ttl)
}

// Get looks up key, returning its value and true on a live hit. A hit slides
// the key's TTL forward from now.
func (l *Local[V]) Get(key string) (V, bool) {
	return l.shardFor(key).get(key, l.ttl)
}

// Delete removes key.
func (l *Local[V]) Delete(key string) {
	l.shardFor(key).delete(key)
}
