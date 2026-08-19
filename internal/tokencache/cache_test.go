package tokencache

import (
	"context"
	"fmt"
	"testing"
	"time"
)

var testContext = context.Background()

func TestCache_PutGet(t *testing.T) {
	c := New(time.Hour, 1000)
	defer c.Close()

	token := []byte("token-a")
	if err := c.Put(testContext, token, Entry{Namespace: "ns", TaskQueue: "queue-a"}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	entry, ok, err := c.Get(testContext, token)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok {
		t.Fatalf("expected token to be found")
	}
	if entry.Namespace != "ns" || entry.TaskQueue != "queue-a" {
		t.Fatalf("unexpected entry: %+v", entry)
	}
}

func TestCache_UnknownToken(t *testing.T) {
	c := New(time.Hour, 1000)
	defer c.Close()

	if _, ok, err := c.Get(testContext, []byte("never-put")); err != nil {
		t.Fatalf("Get: %v", err)
	} else if ok {
		t.Fatalf("expected unknown token to miss")
	}
}

func TestCache_Delete(t *testing.T) {
	c := New(time.Hour, 1000)
	defer c.Close()

	token := []byte("token-a")
	if err := c.Put(testContext, token, Entry{Namespace: "ns", TaskQueue: "queue-a"}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := c.Delete(testContext, token); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if _, ok, err := c.Get(testContext, token); err != nil {
		t.Fatalf("Get: %v", err)
	} else if ok {
		t.Fatalf("expected deleted token to miss")
	}
}

func TestCache_ExpiresAfterTTL(t *testing.T) {
	c := New(50*time.Millisecond, 1000)
	defer c.Close()

	token := []byte("token-a")
	if err := c.Put(testContext, token, Entry{Namespace: "ns", TaskQueue: "queue-a"}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	if _, ok, err := c.Get(testContext, token); err != nil {
		t.Fatalf("Get: %v", err)
	} else if ok {
		t.Fatalf("expected expired token to miss")
	}
}

func TestCache_GetSlidesTTLForward(t *testing.T) {
	c := New(100*time.Millisecond, 1000)
	defer c.Close()

	token := []byte("token-a")
	if err := c.Put(testContext, token, Entry{Namespace: "ns", TaskQueue: "queue-a"}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Heartbeat every 60ms, well under the 100ms TTL, for longer than the
	// original TTL would have allowed - simulates an actively heartbeating
	// long-running activity.
	for i := range 4 {
		time.Sleep(60 * time.Millisecond)
		if _, ok, err := c.Get(testContext, token); err != nil {
			t.Fatalf("Get: %v", err)
		} else if !ok {
			t.Fatalf("expected token to still be live at iteration %d", i)
		}
	}
}

func TestCache_LRUEvictionUnderShardCap(t *testing.T) {
	// numShards shards; force everything into a deterministic small cap by
	// using a tiny maxSize so each shard holds at most ~1 entry, then insert
	// many distinct tokens and verify the cache never exceeds its bound (it
	// should not panic, deadlock, or grow unbounded).
	c := New(time.Hour, numShards) // ~1 per shard
	defer c.Close()

	for i := range 10000 {
		token := fmt.Appendf(nil, "token-%d", i)
		if err := c.Put(testContext, token, Entry{Namespace: "ns", TaskQueue: "queue-a"}); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	total := 0
	for _, s := range c.shards {
		s.mu.Lock()
		total += len(s.items)
		s.mu.Unlock()
	}
	if total > numShards*2 {
		t.Fatalf("expected bounded cache size, got %d entries", total)
	}
}

func TestCache_JanitorRemovesExpiredEntries(t *testing.T) {
	c := New(30*time.Millisecond, 1000)
	defer c.Close()

	token := []byte("token-a")
	if err := c.Put(testContext, token, Entry{Namespace: "ns", TaskQueue: "queue-a"}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Janitor interval is clamped to a minimum of 15s in New, so directly
	// invoke the sweep instead of waiting on the real ticker.
	time.Sleep(50 * time.Millisecond)
	for _, s := range c.shards {
		s.evictExpired(time.Now())
	}

	for _, s := range c.shards {
		s.mu.Lock()
		n := len(s.items)
		s.mu.Unlock()
		if n != 0 {
			t.Fatalf("expected janitor to remove expired entries, shard still has %d", n)
		}
	}
}
