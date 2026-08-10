// Package upstream dials the real Temporal server that the proxy forwards
// allowed calls to.
package upstream

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/02strich/temporal-untrusted-workers/internal/config"
)

// Dial connects to the upstream Temporal server described by cfg and
// returns a ready-to-use WorkflowServiceClient along with the underlying
// connection (for graceful shutdown via Close).
func Dial(cfg config.UpstreamConfig) (workflowservice.WorkflowServiceClient, *grpc.ClientConn, error) {
	transportCreds, err := transportCredentials(cfg)
	if err != nil {
		return nil, nil, err
	}

	opts := []grpc.DialOption{grpc.WithTransportCredentials(transportCreds)}

	if cfg.AuthMode == config.AuthModeAPIKey {
		opts = append(opts, grpc.WithChainUnaryInterceptor(apiKeyInterceptor(cfg.APIKey)))
	}

	conn, err := grpc.NewClient(cfg.Addr, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("upstream: dialing %s: %w", cfg.Addr, err)
	}

	return workflowservice.NewWorkflowServiceClient(conn), conn, nil
}

func transportCredentials(cfg config.UpstreamConfig) (credentials.TransportCredentials, error) {
	if cfg.TLSMode == config.TLSModePlaintext {
		return insecure.NewCredentials(), nil
	}

	tlsConfig := &tls.Config{
		InsecureSkipVerify: cfg.TLSSkipVerify, //nolint:gosec // explicit opt-in via config for local/dev use
		ServerName:         cfg.TLSServerName,
	}

	if cfg.TLSCAFile != "" {
		pem, err := os.ReadFile(cfg.TLSCAFile)
		if err != nil {
			return nil, fmt.Errorf("upstream: reading TLS CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("upstream: no certificates found in TLS CA file %s", cfg.TLSCAFile)
		}
		tlsConfig.RootCAs = pool
	}
	// A nil RootCAs falls back to the system root CA pool, which is correct
	// for a publicly-trusted endpoint such as Temporal Cloud.

	return credentials.NewTLS(tlsConfig), nil
}

// apiKeyInterceptor attaches the upstream API key to every outgoing call,
// mirroring the mechanism the real Temporal SDK uses for its own API-key
// credential (metadata.AppendToOutgoingContext with an "authorization:
// Bearer <key>" entry). The key is read once, at dial time, since it is
// static for the proxy's lifetime.
func apiKeyInterceptor(apiKey string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+apiKey)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}
