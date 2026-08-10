package upstream

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/02strich/temporal-untrusted-workers/internal/config"
)

func TestApiKeyInterceptor_AttachesAuthorizationHeader(t *testing.T) {
	interceptor := apiKeyInterceptor("upstream-secret")

	var seenMD metadata.MD
	invoker := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		seenMD, _ = metadata.FromOutgoingContext(ctx)
		return nil
	}

	err := interceptor(context.Background(), "/temporal.api.workflowservice.v1.WorkflowService/PollWorkflowTaskQueue", nil, nil, nil, invoker)
	if err != nil {
		t.Fatalf("interceptor returned error: %v", err)
	}

	got := seenMD.Get("authorization")
	if len(got) != 1 || got[0] != "Bearer upstream-secret" {
		t.Fatalf("expected authorization header to be set to bearer token, got: %v", got)
	}
}

func TestApiKeyInterceptor_DoesNotLeakDownstreamMetadata(t *testing.T) {
	// Simulate a downstream call context that carries the *worker's* own
	// authorization header (incoming metadata). The upstream interceptor
	// must set the *upstream* key on the outgoing context, not forward
	// whatever the caller happened to have lying around.
	incoming := metadata.Pairs("authorization", "Bearer downstream-worker-key")
	ctx := metadata.NewIncomingContext(context.Background(), incoming)

	interceptor := apiKeyInterceptor("upstream-secret")

	var seenMD metadata.MD
	invoker := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		seenMD, _ = metadata.FromOutgoingContext(ctx)
		return nil
	}

	if err := interceptor(ctx, "SomeMethod", nil, nil, nil, invoker); err != nil {
		t.Fatalf("interceptor returned error: %v", err)
	}

	got := seenMD.Get("authorization")
	if len(got) != 1 || got[0] != "Bearer upstream-secret" {
		t.Fatalf("expected only the upstream key on the outgoing context, got: %v", got)
	}
}

func TestTransportCredentials_Plaintext(t *testing.T) {
	creds, err := transportCredentials(config.UpstreamConfig{TLSMode: config.TLSModePlaintext})
	if err != nil {
		t.Fatalf("transportCredentials: %v", err)
	}
	if creds.Info().SecurityProtocol != insecure.NewCredentials().Info().SecurityProtocol {
		t.Fatalf("expected insecure credentials for plaintext mode")
	}
}

func TestTransportCredentials_TLSMissingCAFileErrors(t *testing.T) {
	_, err := transportCredentials(config.UpstreamConfig{
		TLSMode:   config.TLSModeTLS,
		TLSCAFile: "/does/not/exist.pem",
	})
	if err == nil {
		t.Fatalf("expected error for missing CA file")
	}
}

func TestTransportCredentials_TLSWithoutCAFileUsesSystemRoots(t *testing.T) {
	creds, err := transportCredentials(config.UpstreamConfig{TLSMode: config.TLSModeTLS})
	if err != nil {
		t.Fatalf("transportCredentials: %v", err)
	}
	if creds == nil {
		t.Fatalf("expected non-nil credentials")
	}
}
