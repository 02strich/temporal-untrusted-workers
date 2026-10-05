package tokencache

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"time"

	valkey "github.com/valkey-io/valkey-go"
)

const valkeyKeyPrefix = "temporal-proxy:task-token:"

type valkeyEntry struct {
	Namespace  string `json:"namespace"`
	TaskQueue  string `json:"task_queue"`
	WorkflowID string `json:"workflow_id"`
}

type valkeyClient interface {
	Put(context.Context, string, string, time.Duration) error
	Get(context.Context, string, time.Duration) (string, bool, error)
	Delete(context.Context, string) error
	Ping(context.Context) error
	Close() error
}

// ValkeyOptions configures a Valkey-backed token cache store.
type ValkeyOptions struct {
	Addrs        []string
	Password     string
	TLSConfig    *tls.Config
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	TTL          time.Duration
}

// ValkeyStore is a Store backed by Valkey or a Valkey-compatible service such
// as GCP Memorystore for Valkey.
type ValkeyStore struct {
	client valkeyClient
	ttl    time.Duration
}

// NewValkeyStore constructs and pings a Valkey-backed Store.
func NewValkeyStore(ctx context.Context, opts ValkeyOptions) (*ValkeyStore, error) {
	client, err := newValkeyGoClient(opts)
	if err != nil {
		return nil, err
	}

	store := newValkeyStoreForClient(client, opts.TTL)
	if err := client.Ping(ctx); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("ping valkey: %w", err)
	}
	return store, nil
}

func newValkeyStoreForClient(client valkeyClient, ttl time.Duration) *ValkeyStore {
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

func (s *ValkeyStore) Close() error {
	return s.client.Close()
}

func encodeValkeyEntry(entry Entry) (string, error) {
	b, err := json.Marshal(valkeyEntry{
		Namespace:  entry.Namespace,
		TaskQueue:  entry.TaskQueue,
		WorkflowID: entry.WorkflowID,
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
	return Entry{Namespace: stored.Namespace, TaskQueue: stored.TaskQueue, WorkflowID: stored.WorkflowID}, nil
}

func tokenKey(token []byte) string {
	sum := sha256.Sum256(token)
	return valkeyKeyPrefix + hex.EncodeToString(sum[:])
}

type valkeyGoClient struct {
	client       valkey.Client
	readTimeout  time.Duration
	writeTimeout time.Duration
}

func newValkeyGoClient(opts ValkeyOptions) (*valkeyGoClient, error) {
	if len(opts.Addrs) == 0 {
		return nil, errors.New("valkey addrs are required")
	}
	if opts.TTL <= 0 {
		return nil, errors.New("valkey token cache ttl must be positive")
	}

	client, err := valkey.NewClient(valkey.ClientOption{
		InitAddress:      opts.Addrs,
		Password:         opts.Password,
		TLSConfig:        opts.TLSConfig,
		Dialer:           net.Dialer{Timeout: opts.DialTimeout},
		ConnWriteTimeout: maxDuration(opts.ReadTimeout, opts.WriteTimeout),
		ClientName:       "temporal-proxy",
	})
	if err != nil {
		return nil, fmt.Errorf("create valkey client: %w", err)
	}
	return &valkeyGoClient{
		client:       client,
		readTimeout:  opts.ReadTimeout,
		writeTimeout: opts.WriteTimeout,
	}, nil
}

func (c *valkeyGoClient) Put(ctx context.Context, key, value string, ttl time.Duration) error {
	ctx, cancel := contextWithOptionalTimeout(ctx, c.writeTimeout)
	defer cancel()
	return c.client.Do(ctx, c.client.B().Set().Key(key).Value(value).PxMilliseconds(ttlMilliseconds(ttl)).Build()).Error()
}

func (c *valkeyGoClient) Get(ctx context.Context, key string, ttl time.Duration) (string, bool, error) {
	ctx, cancel := contextWithOptionalTimeout(ctx, c.readTimeout)
	defer cancel()
	value, err := c.client.Do(ctx, c.client.B().Getex().Key(key).PxMilliseconds(ttlMilliseconds(ttl)).Build()).ToString()
	if valkey.IsValkeyNil(err) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return value, true, nil
}

func (c *valkeyGoClient) Delete(ctx context.Context, key string) error {
	ctx, cancel := contextWithOptionalTimeout(ctx, c.writeTimeout)
	defer cancel()
	return c.client.Do(ctx, c.client.B().Del().Key(key).Build()).Error()
}

func (c *valkeyGoClient) Ping(ctx context.Context) error {
	ctx, cancel := contextWithOptionalTimeout(ctx, maxDuration(c.readTimeout, c.writeTimeout))
	defer cancel()
	return c.client.Do(ctx, c.client.B().Ping().Build()).Error()
}

func (c *valkeyGoClient) Close() error {
	c.client.Close()
	return nil
}

func ttlMilliseconds(ttl time.Duration) int64 {
	ms := ttl / time.Millisecond
	if ttl%time.Millisecond != 0 {
		ms++
	}
	return max(int64(ms), 1)
}

func contextWithOptionalTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}
