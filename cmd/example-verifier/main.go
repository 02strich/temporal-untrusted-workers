// Command example-verifier is a reference implementation of the Nexus
// service temporal-proxy calls when TEMPORAL_PROXY_COMMAND_VERIFIER=nexus.
//
// It runs a Temporal worker hosting one Nexus service with a synchronous
// VerifyCommands operation that receives a commandpolicypb.VerifyCommandsRequest
// and returns a commandpolicypb.VerifyCommandsResponse (see
// proto/commandpolicy/v1/commandpolicy.proto). As its policy it applies the proxy's
// built-in namespace/task-queue scoping (commandpolicy.BuiltIn); replace
// verify with your own rules.
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	commandpolicypb "github.com/02strich/temporal-untrusted-workers/gen/commandpolicy/v1"
	"github.com/02strich/temporal-untrusted-workers/internal/auth"
	"github.com/02strich/temporal-untrusted-workers/internal/commandpolicy"
)

func main() {
	if err := run(); err != nil {
		slog.Error("example-verifier exiting", "error", err)
		os.Exit(1)
	}
}

func run() error {
	address := getEnv("TEMPORAL_ADDRESS", "127.0.0.1:7233")
	namespace := getEnv("TEMPORAL_NAMESPACE", "default")
	taskQueue := getEnv("VERIFIER_TASK_QUEUE", "command-verifier")
	serviceName := getEnv("VERIFIER_SERVICE", "command-policy")
	operationName := getEnv("VERIFIER_OPERATION", commandpolicy.DefaultNexusOperation)

	opts := client.Options{HostPort: address, Namespace: namespace}
	if apiKey := os.Getenv("TEMPORAL_API_KEY"); apiKey != "" {
		opts.Credentials = client.NewAPIKeyStaticCredentials(apiKey)
		opts.ConnectionOptions.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	c, err := client.Dial(opts)
	if err != nil {
		return fmt.Errorf("dialing temporal: %w", err)
	}
	defer c.Close()

	service := nexus.NewService(serviceName)
	if err := service.Register(nexus.NewSyncOperation(operationName, verify)); err != nil {
		return fmt.Errorf("registering operation: %w", err)
	}

	w := worker.New(c, taskQueue, worker.Options{})
	w.RegisterNexusService(service)

	slog.Info("example-verifier running", "address", address, "namespace", namespace, "task_queue", taskQueue, "service", serviceName, "operation", operationName)
	return w.Run(worker.InterruptCh())
}

// verify is the VerifyCommands operation handler. Returning an error fails
// the operation, which the proxy treats as "verifier unavailable" and fails
// closed; a policy rejection is reported as Allowed=false instead.
func verify(ctx context.Context, req *commandpolicypb.VerifyCommandsRequest, _ nexus.StartOperationOptions) (*commandpolicypb.VerifyCommandsResponse, error) {
	err := commandpolicy.BuiltIn{}.Verify(ctx, commandpolicy.Request{
		Identity:  auth.Identity{Valid: true, Namespace: req.GetNamespace(), TaskQueues: req.GetTaskQueues(), Subject: req.GetSubject()},
		TaskQueue: req.GetTaskQueue(),
		Commands:  req.GetCommands(),
	})
	var denied commandpolicy.DeniedError
	switch {
	case err == nil:
		return &commandpolicypb.VerifyCommandsResponse{Allowed: true}, nil
	case errors.As(err, &denied):
		slog.Info("commands denied", "namespace", req.GetNamespace(), "task_queue", req.GetTaskQueue(), "subject", req.GetSubject(), "reason", denied.Reason)
		return &commandpolicypb.VerifyCommandsResponse{Allowed: false, Reason: denied.Reason}, nil
	default:
		return nil, err
	}
}

func getEnv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}
