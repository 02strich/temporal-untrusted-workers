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

	enumsv1 "go.temporal.io/api/enums/v1"
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

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
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

	verifyCrossQueueBlocked(ctx, c, taskQueue)
}

// verifyCrossQueueBlocked starts CrossQueueWorkflow, whose activity targets
// "forbidden-queue" - a task queue no configured identity is authorized
// for. The proxy denies the RespondWorkflowTaskCompleted call carrying that
// command, so the workflow can never actually complete (its workflow task
// will keep timing out and being rescheduled). This checks that the
// activity was indeed never scheduled, then terminates the workflow so it
// doesn't retry forever against the dev server.
func verifyCrossQueueBlocked(ctx context.Context, c client.Client, taskQueue string) {
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        fmt.Sprintf("verify-cross-queue-%d", time.Now().UnixNano()),
		TaskQueue: taskQueue,
	}, "CrossQueueWorkflow", "hello from verify-client")
	if err != nil {
		log.Fatalf("starting CrossQueueWorkflow: %v", err)
	}

	log.Println("started CrossQueueWorkflow (its activity targets \"forbidden-queue\", which the proxy should block)")
	time.Sleep(5 * time.Second)

	scheduledActivity := false
	iter := c.GetWorkflowHistory(ctx, run.GetID(), run.GetRunID(), false, enumsv1.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			log.Fatalf("reading CrossQueueWorkflow history: %v", err)
		}
		if event.GetEventType() == enumsv1.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED {
			scheduledActivity = true
		}
	}

	if err := c.TerminateWorkflow(ctx, run.GetID(), run.GetRunID(), "verify-client: cross-queue command correctly blocked, cleaning up"); err != nil {
		log.Printf("warning: failed to terminate CrossQueueWorkflow: %v", err)
	}

	if scheduledActivity {
		log.Fatal("CrossQueueWorkflow: activity was scheduled on forbidden-queue - the proxy should have blocked this")
	}
	log.Println("confirmed: CrossQueueWorkflow's forbidden-queue activity command was blocked by the proxy (no ActivityTaskScheduled event)")
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
