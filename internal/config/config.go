// Package config loads and validates the proxy's environment-variable
// configuration.
package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	// Upstream (proxy -> Temporal server) auth modes.
	AuthModeNone   = "none"
	AuthModeAPIKey = "api-key"

	// Worker-facing auth modes: how downstream workers authenticate to the
	// proxy. Distinct from the upstream AuthMode* values above.
	WorkerAuthModeStatic = "static"
	WorkerAuthModeJWT    = "jwt"

	TLSModePlaintext = "plaintext"
	TLSModeTLS       = "tls"

	TokenCacheBackendLocal  = "local"
	TokenCacheBackendValkey = "valkey"

	ToolVerifierNone  = "none"
	ToolVerifierNexus = "nexus"

	defaultToolVerifierNexusOperation = "VerifyToolCalls"
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

// ValkeyConfig configures the remote token-cache backend.
type ValkeyConfig struct {
	Addrs        []string
	Password     string
	TLSMode      string // TLSModePlaintext | TLSModeTLS
	TLSCAFile    string
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// ToolVerifierConfig selects how the tool calls workflows make outside their
// task queue (Nexus operations, activities on other task queues) are judged:
// by the proxy's default rule, or by an operator-provided Nexus service.
type ToolVerifierConfig struct {
	Mode string // ToolVerifierNone | ToolVerifierNexus

	// The fields below apply only when Mode == ToolVerifierNexus.
	NexusNamespace string
	NexusEndpoint  string
	NexusService   string
	NexusOperation string
	Timeout        time.Duration
}

// Config is the fully validated proxy configuration.
type Config struct {
	Upstream   UpstreamConfig
	Downstream DownstreamConfig

	// WorkerAuthMode selects how downstream workers authenticate:
	// WorkerAuthModeStatic (API keys) or WorkerAuthModeJWT (Google ID tokens).
	WorkerAuthMode string
	StaticAuthFile string
	JWTAudience    string

	TokenCacheBackend string
	TokenCacheTTL     time.Duration
	TokenCacheMaxSize int
	Valkey            ValkeyConfig

	ToolVerifier ToolVerifierConfig

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
		WorkerAuthMode: getEnv("TEMPORAL_PROXY_AUTH_MODE", WorkerAuthModeStatic),
		StaticAuthFile: getEnv("TEMPORAL_PROXY_STATIC_AUTH_FILE", defaultStaticAuthFile()),
		JWTAudience:    os.Getenv("TEMPORAL_PROXY_JWT_AUDIENCE"),
		TokenCacheBackend: getEnv(
			"TEMPORAL_PROXY_TOKEN_CACHE_BACKEND",
			TokenCacheBackendLocal,
		),
		Valkey: ValkeyConfig{
			Addrs:     getEnvList("TEMPORAL_PROXY_VALKEY_ADDRS"),
			Password:  os.Getenv("TEMPORAL_PROXY_VALKEY_PASSWORD"),
			TLSMode:   getEnv("TEMPORAL_PROXY_VALKEY_TLS_MODE", TLSModeTLS),
			TLSCAFile: os.Getenv("TEMPORAL_PROXY_VALKEY_TLS_CA_FILE"),
		},
		ToolVerifier: ToolVerifierConfig{
			Mode:           getEnv("TEMPORAL_PROXY_TOOL_VERIFIER", ToolVerifierNone),
			NexusNamespace: os.Getenv("TEMPORAL_PROXY_TOOL_VERIFIER_NEXUS_NAMESPACE"),
			NexusEndpoint:  os.Getenv("TEMPORAL_PROXY_TOOL_VERIFIER_NEXUS_ENDPOINT"),
			NexusService:   os.Getenv("TEMPORAL_PROXY_TOOL_VERIFIER_NEXUS_SERVICE"),
			NexusOperation: getEnv("TEMPORAL_PROXY_TOOL_VERIFIER_NEXUS_OPERATION", defaultToolVerifierNexusOperation),
		},
		LogLevel: getEnv("TEMPORAL_PROXY_LOG_LEVEL", "info"),
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

	valkeyDialTimeout, err := getEnvDuration("TEMPORAL_PROXY_VALKEY_DIAL_TIMEOUT", 5*time.Second)
	if err != nil {
		errs = append(errs, err)
	}
	cfg.Valkey.DialTimeout = valkeyDialTimeout

	valkeyReadTimeout, err := getEnvDuration("TEMPORAL_PROXY_VALKEY_READ_TIMEOUT", 2*time.Second)
	if err != nil {
		errs = append(errs, err)
	}
	cfg.Valkey.ReadTimeout = valkeyReadTimeout

	valkeyWriteTimeout, err := getEnvDuration("TEMPORAL_PROXY_VALKEY_WRITE_TIMEOUT", 2*time.Second)
	if err != nil {
		errs = append(errs, err)
	}
	cfg.Valkey.WriteTimeout = valkeyWriteTimeout

	verifierTimeout, err := getEnvDuration("TEMPORAL_PROXY_TOOL_VERIFIER_TIMEOUT", 5*time.Second)
	if err != nil {
		errs = append(errs, err)
	}
	cfg.ToolVerifier.Timeout = verifierTimeout

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
		errs = append(errs, errors.New("TEMPORAL_PROXY_STATIC_AUTH_FILE is required (it configures the shipped Authenticators)"))
	}

	switch c.WorkerAuthMode {
	case WorkerAuthModeStatic:
	case WorkerAuthModeJWT:
		if c.JWTAudience == "" {
			errs = append(errs, errors.New("TEMPORAL_PROXY_JWT_AUDIENCE is required when TEMPORAL_PROXY_AUTH_MODE=jwt"))
		}
	default:
		errs = append(errs, fmt.Errorf("TEMPORAL_PROXY_AUTH_MODE: invalid value %q (want %q or %q)", c.WorkerAuthMode, WorkerAuthModeStatic, WorkerAuthModeJWT))
	}

	if c.TokenCacheTTL <= 0 {
		errs = append(errs, errors.New("TEMPORAL_PROXY_TOKEN_CACHE_TTL must be positive"))
	}

	if c.TokenCacheMaxSize <= 0 {
		errs = append(errs, errors.New("TEMPORAL_PROXY_TOKEN_CACHE_MAX_SIZE must be positive"))
	}

	switch c.TokenCacheBackend {
	case TokenCacheBackendLocal:
	case TokenCacheBackendValkey:
		if len(c.Valkey.Addrs) == 0 {
			errs = append(errs, errors.New("TEMPORAL_PROXY_VALKEY_ADDRS is required when TEMPORAL_PROXY_TOKEN_CACHE_BACKEND=valkey"))
		}
	default:
		errs = append(errs, fmt.Errorf("TEMPORAL_PROXY_TOKEN_CACHE_BACKEND: invalid value %q (want %q or %q)", c.TokenCacheBackend, TokenCacheBackendLocal, TokenCacheBackendValkey))
	}

	switch c.Valkey.TLSMode {
	case TLSModePlaintext, TLSModeTLS:
	default:
		errs = append(errs, fmt.Errorf("TEMPORAL_PROXY_VALKEY_TLS_MODE: invalid value %q (want %q or %q)", c.Valkey.TLSMode, TLSModePlaintext, TLSModeTLS))
	}

	if c.Valkey.DialTimeout <= 0 {
		errs = append(errs, errors.New("TEMPORAL_PROXY_VALKEY_DIAL_TIMEOUT must be positive"))
	}
	if c.Valkey.ReadTimeout <= 0 {
		errs = append(errs, errors.New("TEMPORAL_PROXY_VALKEY_READ_TIMEOUT must be positive"))
	}
	if c.Valkey.WriteTimeout <= 0 {
		errs = append(errs, errors.New("TEMPORAL_PROXY_VALKEY_WRITE_TIMEOUT must be positive"))
	}

	switch c.ToolVerifier.Mode {
	case ToolVerifierNone:
	case ToolVerifierNexus:
		required := []struct{ name, value string }{
			{"TEMPORAL_PROXY_TOOL_VERIFIER_NEXUS_NAMESPACE", c.ToolVerifier.NexusNamespace},
			{"TEMPORAL_PROXY_TOOL_VERIFIER_NEXUS_ENDPOINT", c.ToolVerifier.NexusEndpoint},
			{"TEMPORAL_PROXY_TOOL_VERIFIER_NEXUS_SERVICE", c.ToolVerifier.NexusService},
			{"TEMPORAL_PROXY_TOOL_VERIFIER_NEXUS_OPERATION", c.ToolVerifier.NexusOperation},
		}
		for _, r := range required {
			if r.value == "" {
				errs = append(errs, fmt.Errorf("%s is required when TEMPORAL_PROXY_TOOL_VERIFIER=nexus", r.name))
			}
		}
		if c.ToolVerifier.Timeout <= 0 {
			errs = append(errs, errors.New("TEMPORAL_PROXY_TOOL_VERIFIER_TIMEOUT must be positive"))
		}
	default:
		errs = append(errs, fmt.Errorf("TEMPORAL_PROXY_TOOL_VERIFIER: invalid value %q (want %q or %q)", c.ToolVerifier.Mode, ToolVerifierNone, ToolVerifierNexus))
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

func getEnvList(key string) []string {
	v, ok := os.LookupEnv(key)
	if !ok {
		return nil
	}

	parts := strings.Split(v, ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		if value := strings.TrimSpace(part); value != "" {
			values = append(values, value)
		}
	}
	return values
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
