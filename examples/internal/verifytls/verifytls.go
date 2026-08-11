// Package verifytls builds Temporal SDK TLS connection options for the verify
// examples from VERIFY_TLS_* environment variables. The knobs mirror the TLS
// configuration the proxy itself exposes (see internal/config), so a worker
// pointed at a TLS proxy - or a client pointed at a TLS upstream such as
// Temporal Cloud - can be configured the same way.
package verifytls

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strconv"

	"go.temporal.io/sdk/client"
)

// ConnectionOptions reads the VERIFY_TLS_* environment variables and returns
// SDK connection options. When VERIFY_TLS_MODE is unset or "plaintext" it
// returns a zero-value ConnectionOptions, preserving the examples'
// plaintext-by-default local-dev behavior.
//
// Supported variables (all optional):
//
//	VERIFY_TLS_MODE         "plaintext" (default) | "tls"
//	VERIFY_TLS_CA_FILE      PEM file of CA certs to trust; empty = system roots
//	VERIFY_TLS_SERVER_NAME  overrides the SNI / cert hostname to verify against
//	VERIFY_TLS_SKIP_VERIFY  "true" to skip certificate verification (dev only)
func ConnectionOptions() (client.ConnectionOptions, error) {
	mode := os.Getenv("VERIFY_TLS_MODE")
	if mode == "" || mode == "plaintext" {
		return client.ConnectionOptions{}, nil
	}
	if mode != "tls" {
		return client.ConnectionOptions{}, fmt.Errorf("VERIFY_TLS_MODE: invalid value %q (want \"plaintext\" or \"tls\")", mode)
	}

	skipVerify := false
	if v := os.Getenv("VERIFY_TLS_SKIP_VERIFY"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return client.ConnectionOptions{}, fmt.Errorf("VERIFY_TLS_SKIP_VERIFY: invalid boolean value %q", v)
		}
		skipVerify = b
	}

	tlsConfig := &tls.Config{
		InsecureSkipVerify: skipVerify, //nolint:gosec // explicit opt-in via env for local/dev use
		ServerName:         os.Getenv("VERIFY_TLS_SERVER_NAME"),
	}

	if caFile := os.Getenv("VERIFY_TLS_CA_FILE"); caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return client.ConnectionOptions{}, fmt.Errorf("reading VERIFY_TLS_CA_FILE: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return client.ConnectionOptions{}, fmt.Errorf("no certificates found in VERIFY_TLS_CA_FILE %s", caFile)
		}
		tlsConfig.RootCAs = pool
	}
	// A nil RootCAs falls back to the system root CA pool, which is correct
	// for a publicly-trusted endpoint such as Temporal Cloud.

	return client.ConnectionOptions{TLS: tlsConfig}, nil
}
