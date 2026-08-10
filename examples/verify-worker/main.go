// Command verify-worker is a minimal Temporal SDK worker used to manually
// verify the proxy end-to-end. It connects through the proxy (not directly
// to the upstream Temporal server) using an API-key credential, exactly as
// a real untrusted worker would.
//
// The go.temporal.io/sdk dependency is deliberately pinned to v1.34.0 (see
// go.mod) rather than latest: newer SDK releases add an experimental
// worker-fleet heartbeating feature that polls a dynamically-named
// per-worker "control" task queue and calls RecordWorkerHeartbeat - a
// different, fleet-observability trust model than the task-processing RPCs
// this proxy allows, and outside the confirmed allowlist scope. This only
// affects which SDK version an example worker uses; the proxy itself only
// depends on go.temporal.io/api and works with any client SDK version that
// speaks the same gRPC surface.
package main

import (
	"context"
	"log"
	"os"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// EchoWorkflow runs EchoActivity and returns its result unchanged. It
// exists purely to exercise the proxy's Poll/Respond path for both
// workflow and activity task queues.
func EchoWorkflow(ctx workflow.Context, msg string) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
	})
	var result string
	err := workflow.ExecuteActivity(ctx, EchoActivity, msg).Get(ctx, &result)
	return result, err
}

func EchoActivity(_ context.Context, msg string) (string, error) {
	return msg, nil
}

// CrossQueueWorkflow schedules EchoActivity on "forbidden-queue" - a task
// queue that belongs to no configured identity in testdata/static-auth.json.
// The proxy must deny the RespondWorkflowTaskCompleted call carrying this
// command (see internal/scope.ValidateCommands), so this workflow never
// actually completes; it exists purely to make that denial observable when
// running the example.
func CrossQueueWorkflow(ctx workflow.Context, msg string) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           "forbidden-queue",
		StartToCloseTimeout: time.Minute,
	})
	var result string
	err := workflow.ExecuteActivity(ctx, EchoActivity, msg).Get(ctx, &result)
	return result, err
}

func main() {
	proxyAddr := getEnv("VERIFY_PROXY_ADDR", "127.0.0.1:7243")
	namespace := getEnv("VERIFY_NAMESPACE", "default")
	taskQueue := getEnv("VERIFY_TASK_QUEUE", "proxy-test-queue")
	apiKey := os.Getenv("VERIFY_API_KEY")
	if apiKey == "" {
		log.Fatal("VERIFY_API_KEY is required")
	}

	c, err := client.Dial(client.Options{
		HostPort:    proxyAddr,
		Namespace:   namespace,
		Credentials: client.NewAPIKeyStaticCredentials(apiKey),
		// No TLS configured: the API-key credential does not force TLS on
		// this SDK version, and the proxy defaults to plaintext for local
		// dev anyway.
	})
	if err != nil {
		log.Fatalf("creating client: %v", err)
	}
	defer c.Close()

	w := worker.New(c, taskQueue, worker.Options{})
	w.RegisterWorkflow(EchoWorkflow)
	w.RegisterWorkflow(CrossQueueWorkflow)
	w.RegisterActivity(EchoActivity)

	log.Printf("verify-worker starting: proxy=%s namespace=%s taskQueue=%s", proxyAddr, namespace, taskQueue)
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalf("worker run failed: %v", err)
	}
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
