package kvstore

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"time"

	valkey "github.com/valkey-io/valkey-go"
)

// Client is a string key-value store with per-key TTLs, as the Valkey
// backends of the proxy's caches need it.
type Client interface {
	// Put stores value under key with the given TTL.
	Put(ctx context.Context, key, value string, ttl time.Duration) error
	// Get returns the value under key and slides its TTL forward.
	Get(ctx context.Context, key string, ttl time.Duration) (string, bool, error)
	Delete(ctx context.Context, key string) error
	Ping(ctx context.Context) error
	Close() error
}

// ValkeyOptions configures a Valkey client.
type ValkeyOptions struct {
	Addrs        []string
	Password     string
	TLSConfig    *tls.Config
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// NewValkeyClient creates a Client for Valkey or a Valkey-compatible service
// such as GCP Memorystore for Valkey, and pings it.
func NewValkeyClient(ctx context.Context, opts ValkeyOptions) (Client, error) {
	if len(opts.Addrs) == 0 {
		return nil, errors.New("valkey addrs are required")
	}
	client, err := valkey.NewClient(valkey.ClientOption{
		InitAddress:      opts.Addrs,
		Password:         opts.Password,
		TLSConfig:        opts.TLSConfig,
		Dialer:           net.Dialer{Timeout: opts.DialTimeout},
		ConnWriteTimeout: max(opts.ReadTimeout, opts.WriteTimeout),
		ClientName:       "temporal-proxy",
	})
	if err != nil {
		return nil, fmt.Errorf("create valkey client: %w", err)
	}
	c := &valkeyGoClient{
		client:       client,
		readTimeout:  opts.ReadTimeout,
		writeTimeout: opts.WriteTimeout,
	}
	if err := c.Ping(ctx); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("ping valkey: %w", err)
	}
	return c, nil
}

type valkeyGoClient struct {
	client       valkey.Client
	readTimeout  time.Duration
	writeTimeout time.Duration
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
	ctx, cancel := contextWithOptionalTimeout(ctx, max(c.readTimeout, c.writeTimeout))
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
