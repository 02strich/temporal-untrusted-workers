package config

import "testing"

func withEnv(t *testing.T, kv map[string]string, fn func()) {
	t.Helper()
	for k, v := range kv {
		t.Setenv(k, v)
	}
	fn()
}

func TestLoad_Defaults(t *testing.T) {
	withEnv(t, map[string]string{
		"TEMPORAL_PROXY_STATIC_AUTH_FILE": "/tmp/does-not-need-to-exist.json",
	}, func() {
		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if cfg.Upstream.Addr != "127.0.0.1:7233" {
			t.Fatalf("unexpected upstream addr: %s", cfg.Upstream.Addr)
		}
		if cfg.Downstream.ListenAddr != "127.0.0.1:7243" {
			t.Fatalf("unexpected listen addr: %s", cfg.Downstream.ListenAddr)
		}
		if cfg.Upstream.AuthMode != AuthModeNone {
			t.Fatalf("unexpected default auth mode: %s", cfg.Upstream.AuthMode)
		}
		if cfg.WorkerAuthMode != WorkerAuthModeStatic {
			t.Fatalf("unexpected default worker auth mode: %s", cfg.WorkerAuthMode)
		}
		if cfg.TokenCacheBackend != TokenCacheBackendLocal {
			t.Fatalf("unexpected token cache backend: %s", cfg.TokenCacheBackend)
		}
		if cfg.Valkey.TLSMode != TLSModeTLS {
			t.Fatalf("unexpected default valkey tls mode: %s", cfg.Valkey.TLSMode)
		}
	})
}

func TestLoad_JWTModeRequiresAudience(t *testing.T) {
	withEnv(t, map[string]string{
		"TEMPORAL_PROXY_STATIC_AUTH_FILE": "/tmp/does-not-need-to-exist.json",
		"TEMPORAL_PROXY_AUTH_MODE":        WorkerAuthModeJWT,
	}, func() {
		_, err := Load()
		if err == nil {
			t.Fatalf("expected error when jwt mode is set without TEMPORAL_PROXY_JWT_AUDIENCE")
		}
	})
}

func TestLoad_JWTModeWithAudience(t *testing.T) {
	withEnv(t, map[string]string{
		"TEMPORAL_PROXY_STATIC_AUTH_FILE": "/tmp/does-not-need-to-exist.json",
		"TEMPORAL_PROXY_AUTH_MODE":        WorkerAuthModeJWT,
		"TEMPORAL_PROXY_JWT_AUDIENCE":     "https://proxy.example.com",
	}, func() {
		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if cfg.WorkerAuthMode != WorkerAuthModeJWT || cfg.JWTAudience != "https://proxy.example.com" {
			t.Fatalf("unexpected jwt config: mode=%s audience=%s", cfg.WorkerAuthMode, cfg.JWTAudience)
		}
	})
}

func TestLoad_InvalidWorkerAuthMode(t *testing.T) {
	withEnv(t, map[string]string{
		"TEMPORAL_PROXY_STATIC_AUTH_FILE": "/tmp/does-not-need-to-exist.json",
		"TEMPORAL_PROXY_AUTH_MODE":        "bogus",
	}, func() {
		_, err := Load()
		if err == nil {
			t.Fatalf("expected error for invalid worker auth mode")
		}
	})
}

func TestLoad_MissingStaticAuthFile(t *testing.T) {
	_, err := Load()
	if err == nil {
		t.Fatalf("expected error when TEMPORAL_PROXY_STATIC_AUTH_FILE is unset")
	}
}

func TestLoad_APIKeyModeRequiresKey(t *testing.T) {
	withEnv(t, map[string]string{
		"TEMPORAL_PROXY_STATIC_AUTH_FILE":   "/tmp/does-not-need-to-exist.json",
		"TEMPORAL_PROXY_UPSTREAM_AUTH_MODE": AuthModeAPIKey,
	}, func() {
		_, err := Load()
		if err == nil {
			t.Fatalf("expected error when api-key mode is set without a key")
		}
	})
}

func TestLoad_DownstreamTLSRequiresCertAndKey(t *testing.T) {
	withEnv(t, map[string]string{
		"TEMPORAL_PROXY_STATIC_AUTH_FILE":    "/tmp/does-not-need-to-exist.json",
		"TEMPORAL_PROXY_DOWNSTREAM_TLS_MODE": TLSModeTLS,
	}, func() {
		_, err := Load()
		if err == nil {
			t.Fatalf("expected error when downstream tls mode is set without cert/key")
		}
	})
}

func TestLoad_InvalidAuthMode(t *testing.T) {
	withEnv(t, map[string]string{
		"TEMPORAL_PROXY_STATIC_AUTH_FILE":   "/tmp/does-not-need-to-exist.json",
		"TEMPORAL_PROXY_UPSTREAM_AUTH_MODE": "bogus",
	}, func() {
		_, err := Load()
		if err == nil {
			t.Fatalf("expected error for invalid auth mode")
		}
	})
}

func TestLoad_ValkeyTokenCacheRequiresAddrs(t *testing.T) {
	withEnv(t, map[string]string{
		"TEMPORAL_PROXY_STATIC_AUTH_FILE":    "/tmp/does-not-need-to-exist.json",
		"TEMPORAL_PROXY_TOKEN_CACHE_BACKEND": TokenCacheBackendValkey,
	}, func() {
		_, err := Load()
		if err == nil {
			t.Fatalf("expected error when valkey backend is set without addrs")
		}
	})
}

func TestLoad_ValkeyTokenCacheConfig(t *testing.T) {
	withEnv(t, map[string]string{
		"TEMPORAL_PROXY_STATIC_AUTH_FILE":         "/tmp/does-not-need-to-exist.json",
		"TEMPORAL_PROXY_TOKEN_CACHE_BACKEND":      TokenCacheBackendValkey,
		"TEMPORAL_PROXY_VALKEY_ADDRS":             "10.0.0.1:6379, 10.0.0.2:6379",
		"TEMPORAL_PROXY_VALKEY_PASSWORD":          "secret",
		"TEMPORAL_PROXY_VALKEY_TLS_MODE":          TLSModePlaintext,
		"TEMPORAL_PROXY_VALKEY_DIAL_TIMEOUT":      "3s",
		"TEMPORAL_PROXY_VALKEY_READ_TIMEOUT":      "4s",
		"TEMPORAL_PROXY_VALKEY_WRITE_TIMEOUT":     "5s",
		"TEMPORAL_PROXY_TOKEN_CACHE_TTL":          "30m",
		"TEMPORAL_PROXY_TOKEN_CACHE_MAX_SIZE":     "123",
		"TEMPORAL_PROXY_UPSTREAM_TLS_SKIP_VERIFY": "false",
		"TEMPORAL_PROXY_DOWNSTREAM_TLS_CERT_FILE": "",
		"TEMPORAL_PROXY_DOWNSTREAM_TLS_KEY_FILE":  "",
		"TEMPORAL_PROXY_UPSTREAM_TLS_SERVER_NAME": "",
		"TEMPORAL_PROXY_UPSTREAM_TLS_CA_FILE":     "",
		"TEMPORAL_PROXY_UPSTREAM_API_KEY":         "",
		"TEMPORAL_PROXY_JWT_AUDIENCE":             "",
		"TEMPORAL_PROXY_LISTEN_ADDR":              "127.0.0.1:7243",
		"TEMPORAL_PROXY_UPSTREAM_ADDR":            "127.0.0.1:7233",
		"TEMPORAL_PROXY_UPSTREAM_AUTH_MODE":       AuthModeNone,
		"TEMPORAL_PROXY_UPSTREAM_TLS_MODE":        TLSModePlaintext,
		"TEMPORAL_PROXY_DOWNSTREAM_TLS_MODE":      TLSModePlaintext,
		"TEMPORAL_PROXY_AUTH_MODE":                WorkerAuthModeStatic,
		"TEMPORAL_PROXY_LOG_LEVEL":                "info",
		"TEMPORAL_PROXY_VALKEY_TLS_CA_FILE":       "",
	}, func() {
		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if cfg.TokenCacheBackend != TokenCacheBackendValkey {
			t.Fatalf("unexpected backend: %s", cfg.TokenCacheBackend)
		}
		if len(cfg.Valkey.Addrs) != 2 || cfg.Valkey.Addrs[0] != "10.0.0.1:6379" || cfg.Valkey.Addrs[1] != "10.0.0.2:6379" {
			t.Fatalf("unexpected addrs: %#v", cfg.Valkey.Addrs)
		}
		if cfg.Valkey.Password != "secret" {
			t.Fatalf("unexpected password: %q", cfg.Valkey.Password)
		}
		if cfg.Valkey.TLSMode != TLSModePlaintext {
			t.Fatalf("unexpected valkey tls mode: %s", cfg.Valkey.TLSMode)
		}
	})
}

func TestLoad_InvalidTokenCacheBackend(t *testing.T) {
	withEnv(t, map[string]string{
		"TEMPORAL_PROXY_STATIC_AUTH_FILE":    "/tmp/does-not-need-to-exist.json",
		"TEMPORAL_PROXY_TOKEN_CACHE_BACKEND": "bogus",
	}, func() {
		_, err := Load()
		if err == nil {
			t.Fatalf("expected error for invalid token cache backend")
		}
	})
}

func TestLoad_InvalidValkeyTLSMode(t *testing.T) {
	withEnv(t, map[string]string{
		"TEMPORAL_PROXY_STATIC_AUTH_FILE": "/tmp/does-not-need-to-exist.json",
		"TEMPORAL_PROXY_VALKEY_TLS_MODE":  "bogus",
	}, func() {
		_, err := Load()
		if err == nil {
			t.Fatalf("expected error for invalid valkey tls mode")
		}
	})
}

func TestLoad_ToolVerifierDefaultsToNone(t *testing.T) {
	withEnv(t, map[string]string{
		"TEMPORAL_PROXY_STATIC_AUTH_FILE": "/tmp/does-not-need-to-exist.json",
	}, func() {
		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if cfg.ToolVerifier.Mode != ToolVerifierNone {
			t.Fatalf("unexpected default tool verifier: %s", cfg.ToolVerifier.Mode)
		}
	})
}

func TestLoad_NexusToolVerifier(t *testing.T) {
	withEnv(t, map[string]string{
		"TEMPORAL_PROXY_STATIC_AUTH_FILE":              "/tmp/does-not-need-to-exist.json",
		"TEMPORAL_PROXY_TOOL_VERIFIER":                 ToolVerifierNexus,
		"TEMPORAL_PROXY_TOOL_VERIFIER_NEXUS_NAMESPACE": "policy-ns",
		"TEMPORAL_PROXY_TOOL_VERIFIER_NEXUS_ENDPOINT":  "policy-endpoint",
		"TEMPORAL_PROXY_TOOL_VERIFIER_NEXUS_SERVICE":   "policy",
		"TEMPORAL_PROXY_TOOL_VERIFIER_TIMEOUT":         "2s",
	}, func() {
		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		v := cfg.ToolVerifier
		if v.NexusNamespace != "policy-ns" || v.NexusEndpoint != "policy-endpoint" || v.NexusService != "policy" {
			t.Fatalf("unexpected nexus verifier config: %+v", v)
		}
		if v.NexusOperation != "VerifyToolCalls" {
			t.Fatalf("unexpected default operation: %s", v.NexusOperation)
		}
		if v.Timeout.String() != "2s" {
			t.Fatalf("unexpected timeout: %s", v.Timeout)
		}
	})
}

func TestLoad_NexusToolVerifierRequiresTarget(t *testing.T) {
	withEnv(t, map[string]string{
		"TEMPORAL_PROXY_STATIC_AUTH_FILE": "/tmp/does-not-need-to-exist.json",
		"TEMPORAL_PROXY_TOOL_VERIFIER":    ToolVerifierNexus,
	}, func() {
		if _, err := Load(); err == nil {
			t.Fatalf("expected error when nexus verifier is set without namespace/endpoint/service")
		}
	})
}

func TestLoad_InvalidToolVerifier(t *testing.T) {
	withEnv(t, map[string]string{
		"TEMPORAL_PROXY_STATIC_AUTH_FILE":      "/tmp/does-not-need-to-exist.json",
		"TEMPORAL_PROXY_TOOL_VERIFIER":         "bogus",
		"TEMPORAL_PROXY_TOOL_VERIFIER_TIMEOUT": "-1s",
	}, func() {
		if _, err := Load(); err == nil {
			t.Fatalf("expected error for invalid tool verifier")
		}
	})
}

func TestLoad_NexusToolVerifierInvalidTimeout(t *testing.T) {
	withEnv(t, map[string]string{
		"TEMPORAL_PROXY_STATIC_AUTH_FILE":              "/tmp/does-not-need-to-exist.json",
		"TEMPORAL_PROXY_TOOL_VERIFIER":                 ToolVerifierNexus,
		"TEMPORAL_PROXY_TOOL_VERIFIER_NEXUS_NAMESPACE": "policy-ns",
		"TEMPORAL_PROXY_TOOL_VERIFIER_NEXUS_ENDPOINT":  "policy-endpoint",
		"TEMPORAL_PROXY_TOOL_VERIFIER_NEXUS_SERVICE":   "policy",
		"TEMPORAL_PROXY_TOOL_VERIFIER_TIMEOUT":         "0s",
	}, func() {
		if _, err := Load(); err == nil {
			t.Fatalf("expected error for non-positive verifier timeout")
		}
	})
}
