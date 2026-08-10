// Command verify-client is a direct (non-proxied) Temporal client used to
// manually verify the proxy end-to-end. It connects straight to the
// upstream Temporal server to start the sample workflow - StartWorkflowExecution
// is not in the proxy's allowed RPC set, so it must never be sent through
// the proxy - and then awaits the result, which is only produced once the
// proxied verify-worker has processed the workflow and activity tasks.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"go.temporal.io/sdk/client"
)

func main() {
	upstreamAddr := getEnv("VERIFY_UPSTREAM_ADDR", "127.0.0.1:7233")
	namespace := getEnv("VERIFY_NAMESPACE", "default")
	taskQueue := getEnv("VERIFY_TASK_QUEUE", "proxy-test-queue")

	c, err := client.Dial(client.Options{
		HostPort:  upstreamAddr,
		Namespace: namespace,
	})
	if err != nil {
		log.Fatalf("creating client: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        fmt.Sprintf("verify-%d", time.Now().UnixNano()),
		TaskQueue: taskQueue,
	}, "EchoWorkflow", "hello from verify-client")
	if err != nil {
		log.Fatalf("starting workflow: %v", err)
	}

	var result string
	if err := run.Get(ctx, &result); err != nil {
		log.Fatalf("workflow failed: %v", err)
	}
	log.Printf("workflow result: %q", result)
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
