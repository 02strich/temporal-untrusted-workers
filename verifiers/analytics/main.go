// Command analytics-verifier is the tool-call verifier of the
// dogwood-analytics demo, written as plain Go instead of Dogwood policies.
// It is a Nexus service temporal-proxy calls when
// TEMPORAL_PROXY_TOOL_VERIFIER=nexus (see proto/toolpolicy/v1/toolpolicy.proto),
// hosted by a Temporal worker; the rules are in policy.go.
//
// It logs every evaluation as an "analytics_request: {json}" line, which the
// demo shows under each case.
package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"os"
	"strconv"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	toolpolicypb "github.com/02strich/temporal-untrusted-workers/gen/toolpolicy/v1"
)

// The operation names temporal-proxy calls by default.
const (
	defaultVerifyOperation = "VerifyToolCalls"
	defaultRecordOperation = "RecordInputEvents"
)

func main() {
	if err := run(); err != nil {
		slog.Error("analytics-verifier exiting", "error", err)
		os.Exit(1)
	}
}

func run() error {
	address := getEnv("TEMPORAL_ADDRESS", "127.0.0.1:7233")
	namespace := getEnv("TEMPORAL_NAMESPACE", "default")
	taskQueue := getEnv("VERIFIER_TASK_QUEUE", "tool-verifier")
	serviceName := getEnv("VERIFIER_SERVICE", "tool-policy")
	verifyOperation := getEnv("VERIFIER_VERIFY_OPERATION", defaultVerifyOperation)
	recordOperation := getEnv("VERIFIER_RECORD_OPERATION", defaultRecordOperation)
	maxRuns, err := strconv.Atoi(getEnv("VERIFIER_MAX_RUNS", "10000"))
	if err != nil {
		return fmt.Errorf("VERIFIER_MAX_RUNS: %w", err)
	}

	verifier := NewVerifier(maxRuns)
	evaluations := log.New(os.Stderr, "", log.LstdFlags)
	verifier.log = func(e evaluation) {
		line, err := json.Marshal(e)
		if err == nil {
			evaluations.Printf("analytics_request: %s", line)
		}
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
	if err := service.Register(
		nexus.NewSyncOperation(verifyOperation, func(_ context.Context, req *toolpolicypb.VerifyToolCallsRequest, _ nexus.StartOperationOptions) (*toolpolicypb.VerifyToolCallsResponse, error) {
			resp := verifier.Decide(req)
			if resp.GetDecision() != toolpolicypb.Decision_DECISION_APPROVED {
				slog.Info("tool calls not approved", "reason", resp.GetReason(), "workflow_id", req.GetCaller().GetWorkflowId())
			}
			return resp, nil
		}),
		nexus.NewSyncOperation(recordOperation, recordInputEvents),
	); err != nil {
		return fmt.Errorf("registering operations: %w", err)
	}

	w := worker.New(c, taskQueue, worker.Options{})
	w.RegisterNexusService(service)
	slog.Info("analytics-verifier running", "address", address, "namespace", namespace, "task_queue", taskQueue, "service", serviceName, "max_runs", maxRuns)
	return w.Run(worker.InterruptCh())
}

// recordInputEvents acknowledges the input events the proxy records: this
// policy only looks at the run's own queries, not at what it received.
func recordInputEvents(_ context.Context, _ *toolpolicypb.RecordInputEventsRequest, _ nexus.StartOperationOptions) (*toolpolicypb.RecordInputEventsResponse, error) {
	return &toolpolicypb.RecordInputEventsResponse{}, nil
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
