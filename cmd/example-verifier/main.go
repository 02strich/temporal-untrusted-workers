// Command example-verifier is a reference implementation of the Nexus
// service temporal-proxy calls when TEMPORAL_PROXY_TOOL_VERIFIER=nexus.
//
// It runs a Temporal worker hosting one Nexus service with a synchronous
// VerifyToolCalls operation that receives a toolpolicypb.VerifyToolCallsRequest
// and returns a toolpolicypb.VerifyToolCallsResponse (see
// proto/toolpolicy/v1/toolpolicy.proto). As its policy it allows the tool
// names listed in VERIFIER_ALLOWED_TOOLS, escalates those listed in
// VERIFIER_ESCALATE_TOOLS to human review, and denies the rest; replace
// verifier with your own rules.
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

	allowed := parseToolList(os.Getenv("VERIFIER_ALLOWED_TOOLS"))
	if allowed == nil {
		slog.Warn("VERIFIER_ALLOWED_TOOLS is unset: allowing every tool call not listed in VERIFIER_ESCALATE_TOOLS")
	}
	escalate := parseToolList(os.Getenv("VERIFIER_ESCALATE_TOOLS"))

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
	if err := service.Register(nexus.NewSyncOperation(operationName, verifier(allowed, escalate))); err != nil {
		return fmt.Errorf("registering operation: %w", err)
	}

	w := worker.New(c, taskQueue, worker.Options{})
	w.RegisterNexusService(service)

	slog.Info("example-verifier running", "address", address, "namespace", namespace, "task_queue", taskQueue, "service", serviceName, "operation", operationName)
	return w.Run(worker.InterruptCh())
}

// parseToolList parses a comma-separated list of tool names; nil means the
// list is unset.
func parseToolList(list string) map[string]bool {
	if strings.TrimSpace(list) == "" {
		return nil
	}
	tools := map[string]bool{}
	for _, name := range strings.Split(list, ",") {
		if name = strings.TrimSpace(name); name != "" {
			tools[name] = true
		}
	}
	return tools
}

// verifier returns the VerifyToolCalls operation handler: a tool listed in
// escalate needs human review, a tool not in allowed (when set) is denied, and
// a denial outweighs an escalation. Returning an error fails the operation,
// which the proxy treats as "verifier unavailable" and fails closed; policy
// outcomes are reported as decisions instead.
func verifier(allowed, escalate map[string]bool) func(context.Context, *toolpolicypb.VerifyToolCallsRequest, nexus.StartOperationOptions) (*toolpolicypb.VerifyToolCallsResponse, error) {
	return func(_ context.Context, req *toolpolicypb.VerifyToolCallsRequest, _ nexus.StartOperationOptions) (*toolpolicypb.VerifyToolCallsResponse, error) {
		resp := decide(req, allowed, escalate)
		if resp.GetDecision() != toolpolicypb.Decision_DECISION_APPROVED {
			caller := req.GetCaller()
			slog.Info("tool calls not approved", "decision", resp.GetDecision(), "reason", resp.GetReason(), "namespace", caller.GetNamespace(), "task_queue", caller.GetTaskQueue(), "workflow_id", caller.GetWorkflowId(), "subject", caller.GetSubject())
		}
		return resp, nil
	}
}

func decide(req *toolpolicypb.VerifyToolCallsRequest, allowed, escalate map[string]bool) *toolpolicypb.VerifyToolCallsResponse {
	var escalated *toolpolicypb.VerifyToolCallsResponse
	for _, call := range req.GetToolCalls() {
		name := call.GetName()
		switch {
		case escalate[name]:
			if escalated == nil {
				escalated = &toolpolicypb.VerifyToolCallsResponse{
					Decision: toolpolicypb.Decision_DECISION_ESCALATE,
					Reason:   fmt.Sprintf("tool %q needs human approval", name),
				}
			}
		case allowed != nil && !allowed[name]:
			return &toolpolicypb.VerifyToolCallsResponse{
				Decision: toolpolicypb.Decision_DECISION_DENIED,
				Reason:   fmt.Sprintf("tool %q is not allowed", name),
			}
		}
	}
	if escalated != nil {
		return escalated
	}
	return &toolpolicypb.VerifyToolCallsResponse{Decision: toolpolicypb.Decision_DECISION_APPROVED}
}

func getEnv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}
