package tokencache

import (
	"context"
	"testing"
	"time"
)

var testContext = context.Background()

func TestCache_PutGet(t *testing.T) {
	c := New(time.Hour, 1000)
	defer c.Close()

	token := []byte("token-a")
	if err := c.Put(testContext, token, Entry{Namespace: "ns", TaskQueue: "queue-a", WorkflowID: "wf-1", RunID: "run-1"}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	entry, ok, err := c.Get(testContext, token)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok {
		t.Fatalf("expected token to be found")
	}
	if entry.Namespace != "ns" || entry.TaskQueue != "queue-a" || entry.WorkflowID != "wf-1" || entry.RunID != "run-1" {
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
