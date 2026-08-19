// Command temporal-proxy runs a gRPC proxy that sits between untrusted
// Temporal workers and a real Temporal server, allowing through only the
// worker task-processing RPCs and pinning each authenticated identity to a
// single namespace + task queue.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
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
	"github.com/02strich/temporal-untrusted-workers/internal/proxy"
	"github.com/02strich/temporal-untrusted-workers/internal/tokencache"
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

	cache, err := buildTokenCache(context.Background(), cfg)
	if err != nil {
		return fmt.Errorf("building token cache: %w", err)
	}
	defer func() {
		if err := cache.Close(); err != nil {
			slog.Warn("closing token cache", "error", err)
		}
	}()

	serverOpts := []grpc.ServerOption{grpc.UnaryInterceptor(proxy.NewInterceptor(authenticator, cache))}
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
		slog.Info("temporal-proxy listening", "addr", cfg.Downstream.ListenAddr, "upstream", cfg.Upstream.Addr, "token_cache_backend", cfg.TokenCacheBackend)
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

func buildTokenCache(ctx context.Context, cfg config.Config) (tokencache.Store, error) {
	switch cfg.TokenCacheBackend {
	case config.TokenCacheBackendValkey:
		tlsConfig, err := valkeyTLSConfig(cfg.Valkey)
		if err != nil {
			return nil, err
		}
		pingTimeout := maxDuration(cfg.Valkey.DialTimeout, cfg.Valkey.ReadTimeout)
		pingCtx, cancel := context.WithTimeout(ctx, pingTimeout)
		defer cancel()
		return tokencache.NewValkeyStore(pingCtx, tokencache.ValkeyOptions{
			Addrs:        cfg.Valkey.Addrs,
			Password:     cfg.Valkey.Password,
			TLSConfig:    tlsConfig,
			DialTimeout:  cfg.Valkey.DialTimeout,
			ReadTimeout:  cfg.Valkey.ReadTimeout,
			WriteTimeout: cfg.Valkey.WriteTimeout,
			TTL:          cfg.TokenCacheTTL,
		})
	default:
		return tokencache.New(cfg.TokenCacheTTL, cfg.TokenCacheMaxSize), nil
	}
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
