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
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/02strich/temporal-untrusted-workers/examples/internal/verifytls"
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
	authMode := getEnv("VERIFY_AUTH_MODE", authModeStatic)

	// The proxy accepts the credential the same way in either mode (an
	// "authorization: Bearer <cred>" header via NewAPIKeyStaticCredentials);
	// VERIFY_AUTH_MODE only changes what that credential is, mirroring the
	// proxy's own TEMPORAL_PROXY_AUTH_MODE.
	credential := resolveCredential(authMode, proxyAddr)

	// Plaintext by default (matching the proxy's local-dev default); set
	// VERIFY_TLS_MODE=tls to connect to a proxy whose downstream listener
	// terminates TLS.
	connOpts, err := verifytls.ConnectionOptions()
	if err != nil {
		log.Fatalf("configuring TLS: %v", err)
	}

	c, err := client.Dial(client.Options{
		HostPort:          proxyAddr,
		Namespace:         namespace,
		Credentials:       client.NewAPIKeyStaticCredentials(credential),
		ConnectionOptions: connOpts,
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

// Worker auth modes, mirroring the proxy's TEMPORAL_PROXY_AUTH_MODE.
const (
	authModeStatic = "static"
	authModeJWT    = "jwt"
)

// resolveCredential returns the credential the worker presents to the proxy,
// per VERIFY_AUTH_MODE. In "static" mode it is VERIFY_API_KEY (a shared API
// key); in "jwt" mode it is this instance's Google Cloud Run identity token,
// which the proxy validates when it runs with TEMPORAL_PROXY_AUTH_MODE=jwt.
func resolveCredential(authMode string, proxyAddr string) string {
	switch authMode {
	case authModeStatic:
		apiKey := os.Getenv("VERIFY_API_KEY")
		if apiKey == "" {
			log.Fatal("VERIFY_API_KEY is required when VERIFY_AUTH_MODE=static")
		}
		return apiKey
	case authModeJWT:
		audience := getEnv("VERIFY_CLOUDRUN_TOKEN_AUDIENCE", proxyAddr)
		cloudRunToken := getCloudRunToken(audience)

		if cloudRunToken == nil {
			log.Fatalf("VERIFY_AUTH_MODE=jwt requires a Cloud Run identity token, but none is available (audience=%q) - this mode must run on Cloud Run/GCP", audience)
		}
		return *cloudRunToken
	default:
		log.Fatalf("VERIFY_AUTH_MODE: invalid value %q (want %q or %q)", authMode, authModeStatic, authModeJWT)
		return "" // unreachable: log.Fatalf exits.
	}
}

// getCloudRunToken fetches this instance's service-account identity token
// from the GCP metadata server and returns it, when running on Cloud Run (or any
// GCP compute that exposes a metadata server).
func getCloudRunToken(audience string) *string {
	const metadataURL = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity"

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, metadataURL+"?audience="+url.QueryEscape(audience), nil)
	if err != nil {
		return nil
	}
	req.Header.Set("Metadata-Flavor", "Google")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		// Metadata server unreachable - not running on Cloud Run. Nothing to
		// log; this is the expected path for local dev.
		return nil
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Cloud Run identity token: reading metadata response: %v", err)
		return nil
	}
	if resp.StatusCode != http.StatusOK {
		log.Printf("Cloud Run identity token unavailable: metadata server returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
		return nil
	}

	token := strings.TrimSpace(string(body))
	return &token
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
