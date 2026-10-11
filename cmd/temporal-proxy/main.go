// Command temporal-proxy runs a gRPC proxy that sits between untrusted
// Temporal workers and a real Temporal server, allowing through only the
// worker task-processing RPCs and pinning each authenticated identity to a
// single namespace and one or more task queues.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/02strich/temporal-untrusted-workers/internal/auth"
	"github.com/02strich/temporal-untrusted-workers/internal/config"
	"github.com/02strich/temporal-untrusted-workers/internal/inputevents"
	"github.com/02strich/temporal-untrusted-workers/internal/kvstore"
	"github.com/02strich/temporal-untrusted-workers/internal/proxy"
	"github.com/02strich/temporal-untrusted-workers/internal/tokencache"
	"github.com/02strich/temporal-untrusted-workers/internal/toolpolicy"
	"github.com/02strich/temporal-untrusted-workers/internal/upstream"
)

func main() {
	if err := run(); err != nil {
		slog.Error("temporal-proxy exiting", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}
	configureLogging(cfg.LogLevel)

	var authenticator auth.Authenticator
	switch cfg.WorkerAuthMode {
	case config.WorkerAuthModeJWT:
		authenticator, err = auth.NewJWTAuthenticatorFromFile(context.Background(), cfg.StaticAuthFile, cfg.JWTAudience)
	default:
		authenticator, err = auth.NewStaticAuthenticatorFromFile(cfg.StaticAuthFile)
	}
	if err != nil {
		return fmt.Errorf("building authenticator: %w", err)
	}

	upstreamClient, upstreamConn, err := upstream.Dial(cfg.Upstream)
	if err != nil {
		return fmt.Errorf("dialing upstream: %w", err)
	}
	defer upstreamConn.Close()

	// With the Valkey backend, one client serves both the token cache and the
	// input-event state store. run owns it; the stores built on it don't close
	// it. Deferred closes run in reverse, so the stores close before it.
	var kv kvstore.Client
	if cfg.TokenCacheBackend == config.TokenCacheBackendValkey {
		if kv, err = dialValkey(context.Background(), cfg.Valkey); err != nil {
			return fmt.Errorf("connecting to valkey: %w", err)
		}
		defer closeLogged("valkey client", kv)
	}

	cache := buildTokenCache(cfg, kv)
	defer closeLogged("token cache", cache)

	inputState := buildInputStateStore(cfg, kv)
	if inputState != nil {
		defer closeLogged("input state store", inputState)
	}

	verifier, inputs, err := buildToolVerifier(cfg, upstreamClient, inputState)
	if err != nil {
		return fmt.Errorf("building tool verifier: %w", err)
	}

	serverOpts := []grpc.ServerOption{grpc.UnaryInterceptor(proxy.NewInterceptor(authenticator, cache, verifier, inputs))}
	if cfg.Downstream.TLSMode == config.TLSModeTLS {
		creds, err := credentials.NewServerTLSFromFile(cfg.Downstream.CertFile, cfg.Downstream.KeyFile)
		if err != nil {
			return fmt.Errorf("loading downstream TLS credentials: %w", err)
		}
		serverOpts = append(serverOpts, grpc.Creds(creds))
	}

	grpcServer := grpc.NewServer(serverOpts...)
	workflowservice.RegisterWorkflowServiceServer(grpcServer, &proxy.Server{Upstream: upstreamClient})

	listener, err := net.Listen("tcp", cfg.Downstream.ListenAddr)
	if err != nil {
		return fmt.Errorf("listening on %s: %w", cfg.Downstream.ListenAddr, err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	serveErr := make(chan error, 1)
	go func() {
		slog.Info("temporal-proxy listening", "addr", cfg.Downstream.ListenAddr, "upstream", cfg.Upstream.Addr, "token_cache_backend", cfg.TokenCacheBackend, "tool_verifier", cfg.ToolVerifier.Mode)
		serveErr <- grpcServer.Serve(listener)
	}()

	select {
	case <-ctx.Done():
		slog.Info("shutting down")
		grpcServer.GracefulStop()
		return nil
	case err := <-serveErr:
		return err
	}
}

func configureLogging(level string) {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		lvl = slog.LevelInfo
	}
	slog.SetLogLoggerLevel(lvl)
}

func dialValkey(ctx context.Context, cfg config.ValkeyConfig) (kvstore.Client, error) {
	tlsConfig, err := valkeyTLSConfig(cfg)
	if err != nil {
		return nil, err
	}
	pingCtx, cancel := context.WithTimeout(ctx, maxDuration(cfg.DialTimeout, cfg.ReadTimeout))
	defer cancel()
	return kvstore.NewValkeyClient(pingCtx, kvstore.ValkeyOptions{
		Addrs:        cfg.Addrs,
		Password:     cfg.Password,
		TLSConfig:    tlsConfig,
		DialTimeout:  cfg.DialTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	})
}

// buildTokenCache returns the token cache: Valkey-backed when kv is set,
// local otherwise.
func buildTokenCache(cfg config.Config, kv kvstore.Client) tokencache.Store {
	if kv != nil {
		return tokencache.NewValkeyStore(kv, cfg.TokenCacheTTL)
	}
	return tokencache.New(cfg.TokenCacheTTL, cfg.TokenCacheMaxSize)
}

// buildInputStateStore returns the input-event state store, using the token
// cache's backend: Valkey-backed when kv is set, local otherwise. It returns
// nil unless the Nexus tool verifier (the only user of input events) is
// configured.
func buildInputStateStore(cfg config.Config, kv kvstore.Client) inputevents.StateStore {
	if cfg.ToolVerifier.Mode != config.ToolVerifierNexus {
		return nil
	}
	if kv != nil {
		return inputevents.NewValkeyStore(kv, cfg.TokenCacheTTL)
	}
	return inputevents.NewLocalStore(cfg.TokenCacheTTL, cfg.ToolVerifier.InputMaxRuns)
}

// closeLogged closes c, logging (rather than returning) a failure, for
// deferred cleanup on shutdown.
func closeLogged(name string, c io.Closer) {
	if err := c.Close(); err != nil {
		slog.Warn("closing "+name, "error", err)
	}
}

// buildToolVerifier returns the configured toolpolicy.Verifier and, for the
// Nexus verifier, the input-event recording setup using inputState (nil
// otherwise). The Nexus verifier runs its standalone Nexus operations over the
// upstream connection, so it reuses the proxy's upstream credentials.
func buildToolVerifier(cfg config.Config, upstreamClient workflowservice.WorkflowServiceClient, inputState inputevents.StateStore) (toolpolicy.Verifier, *proxy.Inputs, error) {
	tv := cfg.ToolVerifier
	if tv.Mode != config.ToolVerifierNexus {
		return toolpolicy.Default{}, nil, nil
	}
	verifier := &toolpolicy.NexusVerifier{
		Client:          upstreamClient,
		Namespace:       tv.NexusNamespace,
		Endpoint:        tv.NexusEndpoint,
		Service:         tv.NexusService,
		VerifyOperation: tv.NexusVerifyOperation,
		RecordOperation: tv.NexusRecordOperation,
		Timeout:         tv.Timeout,
	}
	filter, err := inputevents.ParseFilter(tv.InputInclude, tv.InputExclude)
	if err != nil {
		return nil, nil, err
	}
	return verifier, &proxy.Inputs{Store: inputState, Filter: filter, Recorder: verifier}, nil
}

func valkeyTLSConfig(cfg config.ValkeyConfig) (*tls.Config, error) {
	if cfg.TLSMode == config.TLSModePlaintext {
		return nil, nil
	}

	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if cfg.TLSCAFile == "" {
		return tlsConfig, nil
	}

	pem, err := os.ReadFile(cfg.TLSCAFile)
	if err != nil {
		return nil, fmt.Errorf("reading valkey TLS CA file: %w", err)
	}
	pool, err := x509.SystemCertPool()
	if err != nil {
		pool = x509.NewCertPool()
	}
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("reading valkey TLS CA file: no certificates found in %s", cfg.TLSCAFile)
	}
	tlsConfig.RootCAs = pool
	return tlsConfig, nil
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}
