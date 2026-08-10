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
