package kvstore

import (
	"fmt"
	"testing"
	"time"
)

func TestLocal_PutGetDelete(t *testing.T) {
	l := NewLocal[string](time.Hour, 1000)
	defer l.Close()

	l.Put("a", "1")
	if v, ok := l.Get("a"); !ok || v != "1" {
		t.Fatalf("Get: got (%q, %v)", v, ok)
	}
	l.Delete("a")
	if _, ok := l.Get("a"); ok {
		t.Fatalf("expected key to be deleted")
	}
}

func TestLocal_ExpiresAfterTTL(t *testing.T) {
	l := NewLocal[int](20*time.Millisecond, 1000)
	defer l.Close()

	l.Put("a", 1)
	time.Sleep(40 * time.Millisecond)
	if _, ok := l.Get("a"); ok {
		t.Fatalf("expected key to expire")
	}
}

func TestLocal_LRUEvictionUnderShardCap(t *testing.T) {
	// A tiny maxSize leaves each shard ~1 entry; inserting many distinct keys
	// must keep the map bounded (no panic, deadlock, or unbounded growth).
	l := NewLocal[int](time.Hour, numShards)
	defer l.Close()

	for i := range 10000 {
		l.Put(fmt.Sprintf("key-%d", i), i)
	}

	total := 0
	for _, s := range l.shards {
		s.mu.Lock()
		total += len(s.items)
		s.mu.Unlock()
	}
	if total > numShards*2 {
		t.Fatalf("expected bounded size, got %d entries", total)
	}
}

func TestLocal_JanitorRemovesExpiredEntries(t *testing.T) {
	l := NewLocal[int](30*time.Millisecond, 1000)
	defer l.Close()

	l.Put("a", 1)

	// The janitor interval is clamped to a minimum of 15s in NewLocal, so
	// directly invoke the sweep instead of waiting on the real ticker.
	time.Sleep(50 * time.Millisecond)
	for _, s := range l.shards {
		s.evictExpired(time.Now())
	}

	for _, s := range l.shards {
		s.mu.Lock()
		n := len(s.items)
		s.mu.Unlock()
		if n != 0 {
			t.Fatalf("expected janitor to remove expired entries, shard still has %d", n)
		}
	}
}
