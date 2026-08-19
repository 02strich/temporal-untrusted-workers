package main

import (
	"os"
	"testing"

	"github.com/02strich/temporal-untrusted-workers/internal/config"
)

func TestValkeyTLSConfigPlaintext(t *testing.T) {
	tlsConfig, err := valkeyTLSConfig(config.ValkeyConfig{TLSMode: config.TLSModePlaintext})
	if err != nil {
		t.Fatalf("valkeyTLSConfig: %v", err)
	}
	if tlsConfig != nil {
		t.Fatalf("expected nil TLS config when valkey TLS mode is plaintext")
	}
}

func TestValkeyTLSConfigRejectsInvalidCAFile(t *testing.T) {
	path := t.TempDir() + "/ca.pem"
	if err := os.WriteFile(path, []byte("not a certificate"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, err := valkeyTLSConfig(config.ValkeyConfig{TLSMode: config.TLSModeTLS, TLSCAFile: path})
	if err == nil {
		t.Fatalf("expected invalid CA file to fail")
	}
}
