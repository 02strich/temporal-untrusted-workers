package tokencache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

type fakeValkeyClient struct {
	values map[string]string

	putErr    error
	getErr    error
	deleteErr error
	pingErr   error

	lastPutKey    string
	lastPutTTL    time.Duration
	lastGetKey    string
	lastGetTTL    time.Duration
	lastDeleteKey string
	pinged        bool
	closed        bool
}

func newFakeValkeyClient() *fakeValkeyClient {
	return &fakeValkeyClient{values: make(map[string]string)}
}

func (f *fakeValkeyClient) Put(_ context.Context, key, value string, ttl time.Duration) error {
	f.lastPutKey = key
	f.lastPutTTL = ttl
	if f.putErr != nil {
		return f.putErr
	}
	f.values[key] = value
	return nil
}

func (f *fakeValkeyClient) Get(_ context.Context, key string, ttl time.Duration) (string, bool, error) {
	f.lastGetKey = key
	f.lastGetTTL = ttl
	if f.getErr != nil {
		return "", false, f.getErr
	}
	value, ok := f.values[key]
	return value, ok, nil
}

func (f *fakeValkeyClient) Delete(_ context.Context, key string) error {
	f.lastDeleteKey = key
	if f.deleteErr != nil {
		return f.deleteErr
	}
	delete(f.values, key)
	return nil
}

func (f *fakeValkeyClient) Ping(_ context.Context) error {
	f.pinged = true
	return f.pingErr
}

func (f *fakeValkeyClient) Close() error {
	f.closed = true
	return nil
}

func TestValkeyTokenKeyHashesToken(t *testing.T) {
	token := []byte("token-a")
	sum := sha256.Sum256(token)
	want := valkeyKeyPrefix + hex.EncodeToString(sum[:])

	if got := tokenKey(token); got != want {
		t.Fatalf("unexpected token key %q, want %q", got, want)
	}
	if got := tokenKey(token); got == valkeyKeyPrefix+string(token) {
		t.Fatalf("token key must not include the raw task token")
	}
}

func TestValkeyStorePutWritesJSONWithTTL(t *testing.T) {
	client := newFakeValkeyClient()
	store := newValkeyStoreForClient(client, time.Hour)

	if err := store.Put(context.Background(), []byte("token-a"), Entry{Namespace: "ns", TaskQueue: "queue-a", WorkflowID: "wf-1"}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if client.lastPutKey != tokenKey([]byte("token-a")) {
		t.Fatalf("unexpected key %q", client.lastPutKey)
	}
	if client.lastPutTTL != time.Hour {
		t.Fatalf("unexpected ttl %s", client.lastPutTTL)
	}

	var stored valkeyEntry
	if err := json.Unmarshal([]byte(client.values[client.lastPutKey]), &stored); err != nil {
		t.Fatalf("stored value is not json: %v", err)
	}
	if stored.Namespace != "ns" || stored.TaskQueue != "queue-a" || stored.WorkflowID != "wf-1" {
		t.Fatalf("unexpected stored entry: %+v", stored)
	}
}

func TestValkeyStoreGetDecodesAndRefreshesTTL(t *testing.T) {
	client := newFakeValkeyClient()
	store := newValkeyStoreForClient(client, 30*time.Minute)

	value, err := encodeValkeyEntry(Entry{Namespace: "ns", TaskQueue: "queue-a", WorkflowID: "wf-1"})
	if err != nil {
		t.Fatalf("encodeValkeyEntry: %v", err)
	}
	client.values[tokenKey([]byte("token-a"))] = value

	entry, ok, err := store.Get(context.Background(), []byte("token-a"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok {
		t.Fatalf("expected token to be found")
	}
	if entry.Namespace != "ns" || entry.TaskQueue != "queue-a" || entry.WorkflowID != "wf-1" {
		t.Fatalf("unexpected entry: %+v", entry)
	}
	if client.lastGetKey != tokenKey([]byte("token-a")) {
		t.Fatalf("unexpected key %q", client.lastGetKey)
	}
	if client.lastGetTTL != 30*time.Minute {
		t.Fatalf("unexpected ttl %s", client.lastGetTTL)
	}
}

func TestValkeyStoreGetMiss(t *testing.T) {
	client := newFakeValkeyClient()
	store := newValkeyStoreForClient(client, time.Hour)

	if _, ok, err := store.Get(context.Background(), []byte("missing")); err != nil {
		t.Fatalf("Get: %v", err)
	} else if ok {
		t.Fatalf("expected missing token to miss")
	}
}

func TestValkeyStoreDelete(t *testing.T) {
	client := newFakeValkeyClient()
	store := newValkeyStoreForClient(client, time.Hour)
	key := tokenKey([]byte("token-a"))
	client.values[key] = "value"

	if err := store.Delete(context.Background(), []byte("token-a")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if client.lastDeleteKey != key {
		t.Fatalf("unexpected key %q", client.lastDeleteKey)
	}
	if _, ok := client.values[key]; ok {
		t.Fatalf("expected token to be deleted")
	}
}

func TestValkeyStoreClientErrors(t *testing.T) {
	client := newFakeValkeyClient()
	client.putErr = errors.New("put failed")
	client.getErr = errors.New("get failed")
	client.deleteErr = errors.New("delete failed")
	store := newValkeyStoreForClient(client, time.Hour)

	if err := store.Put(context.Background(), []byte("token-a"), Entry{Namespace: "ns", TaskQueue: "queue-a"}); err == nil {
		t.Fatalf("expected put error")
	}
	if _, _, err := store.Get(context.Background(), []byte("token-a")); err == nil {
		t.Fatalf("expected get error")
	}
	if err := store.Delete(context.Background(), []byte("token-a")); err == nil {
		t.Fatalf("expected delete error")
	}
}

func TestValkeyStoreGetInvalidJSON(t *testing.T) {
	client := newFakeValkeyClient()
	store := newValkeyStoreForClient(client, time.Hour)
	client.values[tokenKey([]byte("token-a"))] = "{"

	if _, _, err := store.Get(context.Background(), []byte("token-a")); err == nil {
		t.Fatalf("expected decode error")
	}
}

func TestValkeyStoreClose(t *testing.T) {
	client := newFakeValkeyClient()
	store := newValkeyStoreForClient(client, time.Hour)

	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !client.closed {
		t.Fatalf("expected client to be closed")
	}
}
