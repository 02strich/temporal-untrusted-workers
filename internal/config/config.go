// Package config loads and validates the proxy's environment-variable
// configuration.
package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

const (
	AuthModeNone   = "none"
	AuthModeAPIKey = "api-key"

	TLSModePlaintext = "plaintext"
	TLSModeTLS       = "tls"
)

// UpstreamConfig configures the proxy's connection to the real Temporal
// server.
type UpstreamConfig struct {
	Addr string

	AuthMode string // AuthModeNone | AuthModeAPIKey
	APIKey   string // required when AuthMode == AuthModeAPIKey

	TLSMode       string // TLSModePlaintext | TLSModeTLS
	TLSCAFile     string
	TLSSkipVerify bool
	TLSServerName string
}

// DownstreamConfig configures the proxy's worker-facing listener.
type DownstreamConfig struct {
	ListenAddr string

	TLSMode  string // TLSModePlaintext | TLSModeTLS
	CertFile string
	KeyFile  string
}

// Config is the fully validated proxy configuration.
type Config struct {
	Upstream   UpstreamConfig
	Downstream DownstreamConfig

	StaticAuthFile string

	TokenCacheTTL     time.Duration
	TokenCacheMaxSize int

	LogLevel string
}

// Load reads configuration from environment variables, applies defaults,
// and validates the result. All problems are collected and returned
// together so a misconfigured deployment fails with one clear message
// rather than one env var at a time.
func Load() (Config, error) {
	cfg := Config{
		Upstream: UpstreamConfig{
			Addr:     getEnv("TEMPORAL_PROXY_UPSTREAM_ADDR", "127.0.0.1:7233"),
			AuthMode: getEnv("TEMPORAL_PROXY_UPSTREAM_AUTH_MODE", AuthModeNone),
			APIKey:   os.Getenv("TEMPORAL_PROXY_UPSTREAM_API_KEY"),

			TLSMode:       getEnv("TEMPORAL_PROXY_UPSTREAM_TLS_MODE", TLSModePlaintext),
			TLSCAFile:     os.Getenv("TEMPORAL_PROXY_UPSTREAM_TLS_CA_FILE"),
			TLSServerName: os.Getenv("TEMPORAL_PROXY_UPSTREAM_TLS_SERVER_NAME"),
		},
		Downstream: DownstreamConfig{
			ListenAddr: getEnv("TEMPORAL_PROXY_LISTEN_ADDR", "127.0.0.1:7243"),
			TLSMode:    getEnv("TEMPORAL_PROXY_DOWNSTREAM_TLS_MODE", TLSModePlaintext),
			CertFile:   os.Getenv("TEMPORAL_PROXY_DOWNSTREAM_TLS_CERT_FILE"),
			KeyFile:    os.Getenv("TEMPORAL_PROXY_DOWNSTREAM_TLS_KEY_FILE"),
		},
		StaticAuthFile: getEnv("TEMPORAL_PROXY_STATIC_AUTH_FILE", defaultStaticAuthFile()),
		LogLevel:       getEnv("TEMPORAL_PROXY_LOG_LEVEL", "info"),
	}

	var errs []error

	skipVerify, err := getEnvBool("TEMPORAL_PROXY_UPSTREAM_TLS_SKIP_VERIFY", false)
	if err != nil {
		errs = append(errs, err)
	}
	cfg.Upstream.TLSSkipVerify = skipVerify

	ttl, err := getEnvDuration("TEMPORAL_PROXY_TOKEN_CACHE_TTL", time.Hour)
	if err != nil {
		errs = append(errs, err)
	}
	cfg.TokenCacheTTL = ttl

	maxSize, err := getEnvInt("TEMPORAL_PROXY_TOKEN_CACHE_MAX_SIZE", 100_000)
	if err != nil {
		errs = append(errs, err)
	}
	cfg.TokenCacheMaxSize = maxSize

	errs = append(errs, cfg.validate()...)

	if len(errs) > 0 {
		return Config{}, errors.Join(errs...)
	}
	return cfg, nil
}

func (c Config) validate() []error {
	var errs []error

	switch c.Upstream.AuthMode {
	case AuthModeNone:
	case AuthModeAPIKey:
		if c.Upstream.APIKey == "" {
			errs = append(errs, errors.New("TEMPORAL_PROXY_UPSTREAM_API_KEY is required when TEMPORAL_PROXY_UPSTREAM_AUTH_MODE=api-key"))
		}
	default:
		errs = append(errs, fmt.Errorf("TEMPORAL_PROXY_UPSTREAM_AUTH_MODE: invalid value %q (want %q or %q)", c.Upstream.AuthMode, AuthModeNone, AuthModeAPIKey))
	}

	switch c.Upstream.TLSMode {
	case TLSModePlaintext, TLSModeTLS:
	default:
		errs = append(errs, fmt.Errorf("TEMPORAL_PROXY_UPSTREAM_TLS_MODE: invalid value %q (want %q or %q)", c.Upstream.TLSMode, TLSModePlaintext, TLSModeTLS))
	}

	switch c.Downstream.TLSMode {
	case TLSModePlaintext:
	case TLSModeTLS:
		if c.Downstream.CertFile == "" || c.Downstream.KeyFile == "" {
			errs = append(errs, errors.New("TEMPORAL_PROXY_DOWNSTREAM_TLS_CERT_FILE and TEMPORAL_PROXY_DOWNSTREAM_TLS_KEY_FILE are required when TEMPORAL_PROXY_DOWNSTREAM_TLS_MODE=tls"))
		}
	default:
		errs = append(errs, fmt.Errorf("TEMPORAL_PROXY_DOWNSTREAM_TLS_MODE: invalid value %q (want %q or %q)", c.Downstream.TLSMode, TLSModePlaintext, TLSModeTLS))
	}

	if c.StaticAuthFile == "" {
		errs = append(errs, errors.New("TEMPORAL_PROXY_STATIC_AUTH_FILE is required (it configures the only shipped Authenticator)"))
	}

	if c.TokenCacheTTL <= 0 {
		errs = append(errs, errors.New("TEMPORAL_PROXY_TOKEN_CACHE_TTL must be positive"))
	}

	if c.TokenCacheMaxSize <= 0 {
		errs = append(errs, errors.New("TEMPORAL_PROXY_TOKEN_CACHE_MAX_SIZE must be positive"))
	}

	return errs
}

// defaultStaticAuthFile returns the path to the static auth file bundled into
// the container image. ko copies cmd/temporal-proxy/kodata into the image and
// sets KO_DATA_PATH to its location at runtime, so the shipped default works
// out of the box; outside a ko image KO_DATA_PATH is unset and the file must be
// provided explicitly via TEMPORAL_PROXY_STATIC_AUTH_FILE.
func defaultStaticAuthFile() string {
	if dir := os.Getenv("KO_DATA_PATH"); dir != "" {
		return filepath.Join(dir, "static-auth.json")
	}
	return ""
}

func getEnv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

func getEnvBool(key string, def bool) (bool, error) {
	v, ok := os.LookupEnv(key)
	if !ok {
		return def, nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, fmt.Errorf("%s: invalid boolean value %q", key, v)
	}
	return b, nil
}

func getEnvInt(key string, def int) (int, error) {
	v, ok := os.LookupEnv(key)
	if !ok {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s: invalid integer value %q", key, v)
	}
	return n, nil
}

func getEnvDuration(key string, def time.Duration) (time.Duration, error) {
	v, ok := os.LookupEnv(key)
	if !ok {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: invalid duration value %q", key, v)
	}
	return d, nil
}
