// Package tokencache tracks which identity (namespace + task queue) a
// Temporal task token was issued to, so that the proxy can validate
// Respond*/Heartbeat calls that reference an opaque token it cannot decode
// itself.
package tokencache

import (
	"context"
	"time"

	"github.com/02strich/temporal-untrusted-workers/internal/kvstore"
)

// Entry is the identity a task token was issued to.
type Entry struct {
	Namespace string
	TaskQueue string
	// WorkflowID and RunID identify the workflow run the task belongs to;
	// empty for Nexus tasks.
	WorkflowID string
	RunID      string
}

// Store records which identity each issued Temporal task token belongs to.
// Implementations must be safe for concurrent use.
type Store interface {
	Put(context.Context, []byte, Entry) error
	Get(context.Context, []byte) (Entry, bool, error)
	Delete(context.Context, []byte) error
	Close() error
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
	local *kvstore.Local[Entry]
}

// New creates a Cache with the given sliding TTL per entry and an
// approximate global size cap (see kvstore.NewLocal).
func New(ttl time.Duration, maxSize int) *Cache {
	return &Cache{local: kvstore.NewLocal[Entry](ttl, maxSize)}
}

// Close stops the background janitor goroutine. Safe to call once.
func (c *Cache) Close() error {
	return c.local.Close()
}

// Put registers token as belonging to entry, sliding its expiry ttl forward
// from now.
func (c *Cache) Put(_ context.Context, token []byte, entry Entry) error {
	c.local.Put(string(token), entry)
	return nil
}

// Get looks up token, returning its entry and true on a live hit. A hit
// slides the token's TTL forward from now.
func (c *Cache) Get(_ context.Context, token []byte) (Entry, bool, error) {
	entry, ok := c.local.Get(string(token))
	return entry, ok, nil
}

// Delete removes token, e.g. once a terminal Respond call has consumed it.
func (c *Cache) Delete(_ context.Context, token []byte) error {
	c.local.Delete(string(token))
	return nil
}
