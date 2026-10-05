// Command example-verifier is a reference implementation of the Nexus
// service temporal-proxy calls when TEMPORAL_PROXY_TOOL_VERIFIER=nexus.
//
// It runs a Temporal worker hosting one Nexus service with a synchronous
// VerifyToolCalls operation that receives a toolpolicypb.VerifyToolCallsRequest
// and returns a toolpolicypb.VerifyToolCallsResponse (see
// proto/toolpolicy/v1/toolpolicy.proto). As its policy it allows the tool
// names listed in VERIFIER_ALLOWED_TOOLS; replace verify with your own rules.
//
// The request arrives as json/protobuf, which the SDK decodes with the
// protobuf JSON decoder: every parameter's "@type" must resolve to a type
// linked into this binary. The well-known types and Temporal's API types the
// proxy produces are; for your own json/protobuf tool inputs, import their
// generated Go packages. A request with an unknown parameter type fails the
// operation, which the proxy treats as "verifier unavailable" (fail closed).
package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	toolpolicypb "github.com/02strich/temporal-untrusted-workers/gen/toolpolicy/v1"
	"github.com/02strich/temporal-untrusted-workers/internal/toolpolicy"
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
	taskQueue := getEnv("VERIFIER_TASK_QUEUE", "tool-verifier")
	serviceName := getEnv("VERIFIER_SERVICE", "tool-policy")
	operationName := getEnv("VERIFIER_OPERATION", toolpolicy.DefaultNexusOperation)

	allowed := parseAllowedTools(os.Getenv("VERIFIER_ALLOWED_TOOLS"))
	if allowed == nil {
		slog.Warn("VERIFIER_ALLOWED_TOOLS is unset: allowing every tool call")
	}

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
	if err := service.Register(nexus.NewSyncOperation(operationName, verifier(allowed))); err != nil {
		return fmt.Errorf("registering operation: %w", err)
	}

	w := worker.New(c, taskQueue, worker.Options{})
	w.RegisterNexusService(service)

	slog.Info("example-verifier running", "address", address, "namespace", namespace, "task_queue", taskQueue, "service", serviceName, "operation", operationName)
	return w.Run(worker.InterruptCh())
}

// parseAllowedTools parses a comma-separated list of tool names; nil means
// "allow everything".
func parseAllowedTools(list string) map[string]bool {
	if strings.TrimSpace(list) == "" {
		return nil
	}
	allowed := map[string]bool{}
	for _, name := range strings.Split(list, ",") {
		if name = strings.TrimSpace(name); name != "" {
			allowed[name] = true
		}
	}
	return allowed
}

// verifier returns the VerifyToolCalls operation handler. Returning an error
// fails the operation, which the proxy treats as "verifier unavailable" and
// fails closed; a policy rejection is reported as Allowed=false instead.
func verifier(allowed map[string]bool) func(context.Context, *toolpolicypb.VerifyToolCallsRequest, nexus.StartOperationOptions) (*toolpolicypb.VerifyToolCallsResponse, error) {
	return func(_ context.Context, req *toolpolicypb.VerifyToolCallsRequest, _ nexus.StartOperationOptions) (*toolpolicypb.VerifyToolCallsResponse, error) {
		caller := req.GetCaller()
		for _, call := range req.GetToolCalls() {
			if allowed != nil && !allowed[call.GetName()] {
				reason := fmt.Sprintf("tool %q is not allowed", call.GetName())
				slog.Info("tool call denied", "namespace", caller.GetNamespace(), "task_queue", caller.GetTaskQueue(), "workflow_id", caller.GetWorkflowId(), "subject", caller.GetSubject(), "tool", call.GetName())
				return &toolpolicypb.VerifyToolCallsResponse{Allowed: false, Reason: reason}, nil
			}
		}
		return &toolpolicypb.VerifyToolCallsResponse{Allowed: true}, nil
	}
}

func getEnv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}
