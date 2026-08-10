// Command temporal-proxy runs a gRPC proxy that sits between untrusted
// Temporal workers and a real Temporal server, allowing through only the
// worker task-processing RPCs and pinning each authenticated identity to a
// single namespace + task queue.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

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

	authenticator, err := auth.NewStaticAuthenticatorFromFile(cfg.StaticAuthFile)
	if err != nil {
		return fmt.Errorf("building authenticator: %w", err)
	}

	upstreamClient, upstreamConn, err := upstream.Dial(cfg.Upstream)
	if err != nil {
		return fmt.Errorf("dialing upstream: %w", err)
	}
	defer upstreamConn.Close()

	cache := tokencache.New(cfg.TokenCacheTTL, cfg.TokenCacheMaxSize)
	defer cache.Close()

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
		slog.Info("temporal-proxy listening", "addr", cfg.Downstream.ListenAddr, "upstream", cfg.Upstream.Addr)
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
