package tokencache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/02strich/temporal-untrusted-workers/internal/kvstore"
)

const valkeyKeyPrefix = "temporal-proxy:task-token:"

type valkeyEntry struct {
	Namespace  string `json:"namespace"`
	TaskQueue  string `json:"task_queue"`
	WorkflowID string `json:"workflow_id"`
	RunID      string `json:"run_id"`
}

// ValkeyStore is a Store backed by Valkey or a Valkey-compatible service such
// as GCP Memorystore for Valkey.
type ValkeyStore struct {
	client kvstore.Client
	ttl    time.Duration
}

// NewValkeyStore returns a Store keeping tokens in client with the given
// sliding TTL. The caller owns client: closing the store doesn't close it.
func NewValkeyStore(client kvstore.Client, ttl time.Duration) *ValkeyStore {
	return &ValkeyStore{client: client, ttl: ttl}
}

func (s *ValkeyStore) Put(ctx context.Context, token []byte, entry Entry) error {
	value, err := encodeValkeyEntry(entry)
	if err != nil {
		return err
	}
	if err := s.client.Put(ctx, tokenKey(token), value, s.ttl); err != nil {
		return fmt.Errorf("put valkey token: %w", err)
	}
	return nil
}

func (s *ValkeyStore) Get(ctx context.Context, token []byte) (Entry, bool, error) {
	value, found, err := s.client.Get(ctx, tokenKey(token), s.ttl)
	if err != nil {
		return Entry{}, false, fmt.Errorf("get valkey token: %w", err)
	}
	if !found {
		return Entry{}, false, nil
	}

	entry, err := decodeValkeyEntry(value)
	if err != nil {
		return Entry{}, false, err
	}
	return entry, true, nil
}

func (s *ValkeyStore) Delete(ctx context.Context, token []byte) error {
	if err := s.client.Delete(ctx, tokenKey(token)); err != nil {
		return fmt.Errorf("delete valkey token: %w", err)
	}
	return nil
}

// Close is a no-op: the client belongs to the caller, which may share it
// (e.g. with the input-event state store) and closes it itself.
func (s *ValkeyStore) Close() error {
	return nil
}

func encodeValkeyEntry(entry Entry) (string, error) {
	b, err := json.Marshal(valkeyEntry{
		Namespace:  entry.Namespace,
		TaskQueue:  entry.TaskQueue,
		WorkflowID: entry.WorkflowID,
		RunID:      entry.RunID,
	})
	if err != nil {
		return "", fmt.Errorf("encode valkey token entry: %w", err)
	}
	return string(b), nil
}

func decodeValkeyEntry(value string) (Entry, error) {
	var stored valkeyEntry
	if err := json.Unmarshal([]byte(value), &stored); err != nil {
		return Entry{}, fmt.Errorf("decode valkey token entry: %w", err)
	}
	return Entry{Namespace: stored.Namespace, TaskQueue: stored.TaskQueue, WorkflowID: stored.WorkflowID, RunID: stored.RunID}, nil
}

func tokenKey(token []byte) string {
	sum := sha256.Sum256(token)
	return valkeyKeyPrefix + hex.EncodeToString(sum[:])
}
