package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	commandpb "go.temporal.io/api/command/v1"
	enums "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/02strich/temporal-untrusted-workers/internal/actions"
	"github.com/02strich/temporal-untrusted-workers/internal/auth"
	"github.com/02strich/temporal-untrusted-workers/internal/tokencache"
)

type fakeAuthenticator struct {
	identities map[string]auth.Identity
}

func (f *fakeAuthenticator) Authenticate(_ context.Context, apiKey string) (auth.Identity, error) {
	if id, ok := f.identities[apiKey]; ok {
		return id, nil
	}
	return auth.Identity{}, nil
}

func ctxWithBearer(key string) context.Context {
	md := metadata.Pairs("authorization", "Bearer "+key)
	return metadata.NewIncomingContext(context.Background(), md)
}

func callInterceptor(t *testing.T, interceptor grpc.UnaryServerInterceptor, ctx context.Context, fullMethod string, req any, resp any, handlerErr error) (any, error, bool) {
	t.Helper()
	handlerCalled := false
	handler := func(ctx context.Context, req any) (any, error) {
		handlerCalled = true
		return resp, handlerErr
	}
	got, err := interceptor(ctx, req, &grpc.UnaryServerInfo{FullMethod: fullMethod}, handler)
	return got, err, handlerCalled
}

func TestInterceptor_DeniesUnknownRPC(t *testing.T) {
	authr := &fakeAuthenticator{identities: map[string]auth.Identity{
		"key-a": {Valid: true, Namespace: "ns", TaskQueue: "queue-a"},
	}}
	cache := tokencache.New(time.Hour, 1000)
	defer cache.Close()
	interceptor := NewInterceptor(authr, cache)

	ctx := ctxWithBearer("key-a")
	_, err, called := callInterceptor(t, interceptor, ctx,
		"/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution",
		&workflowservice.StartWorkflowExecutionRequest{Namespace: "ns", TaskQueue: &taskqueuepb.TaskQueue{Name: "queue-a"}},
		&workflowservice.StartWorkflowExecutionResponse{}, nil)

	if called {
		t.Fatalf("handler must not run for a denied RPC")
	}
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %v", err)
	}
}

func TestInterceptor_DeniesMissingCredentials(t *testing.T) {
	authr := &fakeAuthenticator{identities: map[string]auth.Identity{
		"key-a": {Valid: true, Namespace: "ns", TaskQueue: "queue-a"},
	}}
	cache := tokencache.New(time.Hour, 1000)
	defer cache.Close()
	interceptor := NewInterceptor(authr, cache)

	req := &workflowservice.PollWorkflowTaskQueueRequest{Namespace: "ns", TaskQueue: &taskqueuepb.TaskQueue{Name: "queue-a"}}
	_, err, called := callInterceptor(t, interceptor, context.Background(),
		"/temporal.api.workflowservice.v1.WorkflowService/PollWorkflowTaskQueue",
		req, &workflowservice.PollWorkflowTaskQueueResponse{}, nil)

	if called {
		t.Fatalf("handler must not run without credentials")
	}
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %v", err)
	}
}

func TestInterceptor_DeniesInvalidAPIKey(t *testing.T) {
	authr := &fakeAuthenticator{identities: map[string]auth.Identity{}}
	cache := tokencache.New(time.Hour, 1000)
	defer cache.Close()
	interceptor := NewInterceptor(authr, cache)

	req := &workflowservice.PollWorkflowTaskQueueRequest{Namespace: "ns", TaskQueue: &taskqueuepb.TaskQueue{Name: "queue-a"}}
	_, err, called := callInterceptor(t, interceptor, ctxWithBearer("nonexistent-key"),
		"/temporal.api.workflowservice.v1.WorkflowService/PollWorkflowTaskQueue",
		req, &workflowservice.PollWorkflowTaskQueueResponse{}, nil)

	if called {
		t.Fatalf("handler must not run with an invalid key")
	}
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %v", err)
	}
}

func TestInterceptor_PollAllowsMatchingQueue(t *testing.T) {
	authr := &fakeAuthenticator{identities: map[string]auth.Identity{
		"key-a": {Valid: true, Namespace: "ns", TaskQueue: "queue-a"},
	}}
	cache := tokencache.New(time.Hour, 1000)
	defer cache.Close()
	interceptor := NewInterceptor(authr, cache)

	req := &workflowservice.PollWorkflowTaskQueueRequest{Namespace: "ns", TaskQueue: &taskqueuepb.TaskQueue{Name: "queue-a"}}
	resp, err, called := callInterceptor(t, interceptor, ctxWithBearer("key-a"),
		"/temporal.api.workflowservice.v1.WorkflowService/PollWorkflowTaskQueue",
		req, &workflowservice.PollWorkflowTaskQueueResponse{TaskToken: []byte("tok-1")}, nil)

	if !called {
		t.Fatalf("handler should have run for an authorized poll")
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.(*workflowservice.PollWorkflowTaskQueueResponse).GetTaskToken() == nil {
		t.Fatalf("expected response to pass through")
	}

	// The returned task token should now be registered under this identity.
	entry, ok := cache.Get([]byte("tok-1"))
	if !ok || entry.Namespace != "ns" || entry.TaskQueue != "queue-a" {
		t.Fatalf("expected token to be cached for the polling identity, got %+v (found=%v)", entry, ok)
	}
}

const shutdownWorkerMethod = "/temporal.api.workflowservice.v1.WorkflowService/ShutdownWorker"

func shutdownInterceptor(t *testing.T) grpc.UnaryServerInterceptor {
	t.Helper()
	authr := &fakeAuthenticator{identities: map[string]auth.Identity{
		"key-a": {Valid: true, Namespace: "ns", TaskQueue: "queue-a"},
	}}
	cache := tokencache.New(time.Hour, 1000)
	t.Cleanup(cache.Close)
	return NewInterceptor(authr, cache)
}

func TestInterceptor_ShutdownWorkerAllowsMatchingQueue(t *testing.T) {
	req := &workflowservice.ShutdownWorkerRequest{Namespace: "ns", TaskQueue: "queue-a", StickyTaskQueue: "host:random-uuid"}
	_, err, called := callInterceptor(t, shutdownInterceptor(t), ctxWithBearer("key-a"),
		shutdownWorkerMethod, req, &workflowservice.ShutdownWorkerResponse{}, nil)

	if !called {
		t.Fatalf("handler should have run for an authorized ShutdownWorker")
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestInterceptor_ShutdownWorkerAllowsEmptyQueue(t *testing.T) {
	// The normal task queue is optional; only the sticky queue is populated.
	req := &workflowservice.ShutdownWorkerRequest{Namespace: "ns", StickyTaskQueue: "host:random-uuid"}
	_, err, called := callInterceptor(t, shutdownInterceptor(t), ctxWithBearer("key-a"),
		shutdownWorkerMethod, req, &workflowservice.ShutdownWorkerResponse{}, nil)

	if !called {
		t.Fatalf("handler should have run for a ShutdownWorker with no normal queue")
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestInterceptor_ShutdownWorkerDeniesOtherQueue(t *testing.T) {
	// A worker must not be able to cancel another queue's outstanding polls.
	req := &workflowservice.ShutdownWorkerRequest{Namespace: "ns", TaskQueue: "victim-queue"}
	_, err, called := callInterceptor(t, shutdownInterceptor(t), ctxWithBearer("key-a"),
		shutdownWorkerMethod, req, &workflowservice.ShutdownWorkerResponse{}, nil)

	if called {
		t.Fatalf("handler must not run for a ShutdownWorker targeting another queue")
	}
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %v", err)
	}
}

func TestInterceptor_ShutdownWorkerDeniesOtherNamespace(t *testing.T) {
	req := &workflowservice.ShutdownWorkerRequest{Namespace: "other-ns", TaskQueue: "queue-a"}
	_, err, called := callInterceptor(t, shutdownInterceptor(t), ctxWithBearer("key-a"),
		shutdownWorkerMethod, req, &workflowservice.ShutdownWorkerResponse{}, nil)

	if called {
		t.Fatalf("handler must not run for a ShutdownWorker in another namespace")
	}
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %v", err)
	}
}

func TestInterceptor_PollDeniesWrongQueue(t *testing.T) {
	authr := &fakeAuthenticator{identities: map[string]auth.Identity{
		"key-a": {Valid: true, Namespace: "ns", TaskQueue: "queue-a"},
	}}
	cache := tokencache.New(time.Hour, 1000)
	defer cache.Close()
	interceptor := NewInterceptor(authr, cache)

	req := &workflowservice.PollWorkflowTaskQueueRequest{Namespace: "ns", TaskQueue: &taskqueuepb.TaskQueue{Name: "queue-b"}}
	_, err, called := callInterceptor(t, interceptor, ctxWithBearer("key-a"),
		"/temporal.api.workflowservice.v1.WorkflowService/PollWorkflowTaskQueue",
		req, &workflowservice.PollWorkflowTaskQueueResponse{}, nil)

	if called {
		t.Fatalf("handler must not run for a cross-queue poll")
	}
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %v", err)
	}
}

func TestInterceptor_PollDeniesWrongNamespace(t *testing.T) {
	authr := &fakeAuthenticator{identities: map[string]auth.Identity{
		"key-a": {Valid: true, Namespace: "ns-a", TaskQueue: "queue-a"},
	}}
	cache := tokencache.New(time.Hour, 1000)
	defer cache.Close()
	interceptor := NewInterceptor(authr, cache)

	req := &workflowservice.PollWorkflowTaskQueueRequest{Namespace: "ns-b", TaskQueue: &taskqueuepb.TaskQueue{Name: "queue-a"}}
	_, err, called := callInterceptor(t, interceptor, ctxWithBearer("key-a"),
		"/temporal.api.workflowservice.v1.WorkflowService/PollWorkflowTaskQueue",
		req, &workflowservice.PollWorkflowTaskQueueResponse{}, nil)

	if called {
		t.Fatalf("handler must not run for a cross-namespace poll")
	}
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %v", err)
	}
}

func TestInterceptor_TokenScoping(t *testing.T) {
	authr := &fakeAuthenticator{identities: map[string]auth.Identity{
		"key-a": {Valid: true, Namespace: "ns", TaskQueue: "queue-a"},
		"key-b": {Valid: true, Namespace: "ns", TaskQueue: "queue-b"},
	}}
	cache := tokencache.New(time.Hour, 1000)
	defer cache.Close()
	interceptor := NewInterceptor(authr, cache)

	// A token registered for queue-a's identity...
	cache.Put([]byte("tok-a"), tokencache.Entry{Namespace: "ns", TaskQueue: "queue-a"})

	// ...must be rejected when presented by queue-b's identity, even though
	// queue-b's own key is otherwise perfectly valid.
	req := &workflowservice.RespondActivityTaskCompletedRequest{Namespace: "ns", TaskToken: []byte("tok-a")}
	_, err, called := callInterceptor(t, interceptor, ctxWithBearer("key-b"),
		"/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskCompleted",
		req, &workflowservice.RespondActivityTaskCompletedResponse{}, nil)
	if called {
		t.Fatalf("handler must not run for a token bound to a different identity")
	}
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %v", err)
	}

	// A token never issued at all must also be rejected.
	req2 := &workflowservice.RespondActivityTaskCompletedRequest{Namespace: "ns", TaskToken: []byte("never-issued")}
	_, err, called = callInterceptor(t, interceptor, ctxWithBearer("key-a"),
		"/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskCompleted",
		req2, &workflowservice.RespondActivityTaskCompletedResponse{}, nil)
	if called {
		t.Fatalf("handler must not run for an unknown token")
	}
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %v", err)
	}

	// The rightful owner presenting its own token must succeed.
	req3 := &workflowservice.RespondActivityTaskCompletedRequest{Namespace: "ns", TaskToken: []byte("tok-a")}
	_, err, called = callInterceptor(t, interceptor, ctxWithBearer("key-a"),
		"/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskCompleted",
		req3, &workflowservice.RespondActivityTaskCompletedResponse{}, nil)
	if !called {
		t.Fatalf("handler should have run for the token's rightful owner")
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestInterceptor_TerminalRPCEvictsToken(t *testing.T) {
	authr := &fakeAuthenticator{identities: map[string]auth.Identity{
		"key-a": {Valid: true, Namespace: "ns", TaskQueue: "queue-a"},
	}}
	cache := tokencache.New(time.Hour, 1000)
	defer cache.Close()
	interceptor := NewInterceptor(authr, cache)

	cache.Put([]byte("tok-a"), tokencache.Entry{Namespace: "ns", TaskQueue: "queue-a"})

	req := &workflowservice.RespondActivityTaskCompletedRequest{Namespace: "ns", TaskToken: []byte("tok-a")}
	_, err, called := callInterceptor(t, interceptor, ctxWithBearer("key-a"),
		"/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskCompleted",
		req, &workflowservice.RespondActivityTaskCompletedResponse{}, nil)
	if !called || err != nil {
		t.Fatalf("expected successful call, called=%v err=%v", called, err)
	}

	if _, ok := cache.Get([]byte("tok-a")); ok {
		t.Fatalf("expected token to be evicted after a terminal Respond call")
	}
}

func TestInterceptor_HeartbeatDoesNotEvictToken(t *testing.T) {
	authr := &fakeAuthenticator{identities: map[string]auth.Identity{
		"key-a": {Valid: true, Namespace: "ns", TaskQueue: "queue-a"},
	}}
	cache := tokencache.New(time.Hour, 1000)
	defer cache.Close()
	interceptor := NewInterceptor(authr, cache)

	cache.Put([]byte("tok-a"), tokencache.Entry{Namespace: "ns", TaskQueue: "queue-a"})

	req := &workflowservice.RecordActivityTaskHeartbeatRequest{Namespace: "ns", TaskToken: []byte("tok-a")}
	_, err, called := callInterceptor(t, interceptor, ctxWithBearer("key-a"),
		"/temporal.api.workflowservice.v1.WorkflowService/RecordActivityTaskHeartbeat",
		req, &workflowservice.RecordActivityTaskHeartbeatResponse{}, nil)
	if !called || err != nil {
		t.Fatalf("expected successful call, called=%v err=%v", called, err)
	}

	if _, ok := cache.Get([]byte("tok-a")); !ok {
		t.Fatalf("expected token to remain live after a heartbeat")
	}
}

func TestInterceptor_HandlerErrorDoesNotMutateCache(t *testing.T) {
	authr := &fakeAuthenticator{identities: map[string]auth.Identity{
		"key-a": {Valid: true, Namespace: "ns", TaskQueue: "queue-a"},
	}}
	cache := tokencache.New(time.Hour, 1000)
	defer cache.Close()
	interceptor := NewInterceptor(authr, cache)

	cache.Put([]byte("tok-a"), tokencache.Entry{Namespace: "ns", TaskQueue: "queue-a"})

	req := &workflowservice.RespondActivityTaskCompletedRequest{Namespace: "ns", TaskToken: []byte("tok-a")}
	_, err, called := callInterceptor(t, interceptor, ctxWithBearer("key-a"),
		"/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskCompleted",
		req, nil, errors.New("upstream unavailable"))
	if !called {
		t.Fatalf("expected handler to run")
	}
	if err == nil {
		t.Fatalf("expected the handler error to propagate")
	}

	if _, ok := cache.Get([]byte("tok-a")); !ok {
		t.Fatalf("expected token to remain cached when the upstream call fails")
	}
}

func TestInterceptor_EagerDispatchTokensAreRegistered(t *testing.T) {
	authr := &fakeAuthenticator{identities: map[string]auth.Identity{
		"key-a": {Valid: true, Namespace: "ns", TaskQueue: "queue-a"},
	}}
	cache := tokencache.New(time.Hour, 1000)
	defer cache.Close()
	interceptor := NewInterceptor(authr, cache)

	cache.Put([]byte("wt-tok"), tokencache.Entry{Namespace: "ns", TaskQueue: "queue-a"})

	resp := &workflowservice.RespondWorkflowTaskCompletedResponse{
		WorkflowTask: &workflowservice.PollWorkflowTaskQueueResponse{TaskToken: []byte("new-wt-tok")},
		ActivityTasks: []*workflowservice.PollActivityTaskQueueResponse{
			{TaskToken: []byte("eager-activity-tok")},
		},
	}

	req := &workflowservice.RespondWorkflowTaskCompletedRequest{Namespace: "ns", TaskToken: []byte("wt-tok")}
	_, err, called := callInterceptor(t, interceptor, ctxWithBearer("key-a"),
		"/temporal.api.workflowservice.v1.WorkflowService/RespondWorkflowTaskCompleted",
		req, resp, nil)
	if !called || err != nil {
		t.Fatalf("expected successful call, called=%v err=%v", called, err)
	}

	for _, tok := range [][]byte{[]byte("new-wt-tok"), []byte("eager-activity-tok")} {
		entry, ok := cache.Get(tok)
		if !ok || entry.Namespace != "ns" || entry.TaskQueue != "queue-a" {
			t.Fatalf("expected eager-dispatch token %s to be registered, got %+v (found=%v)", tok, entry, ok)
		}
	}
}

func TestInterceptor_DeniesCommandTargetingDifferentQueue(t *testing.T) {
	authr := &fakeAuthenticator{identities: map[string]auth.Identity{
		"key-a": {Valid: true, Namespace: "ns", TaskQueue: "queue-a"},
	}}
	cache := tokencache.New(time.Hour, 1000)
	defer cache.Close()
	interceptor := NewInterceptor(authr, cache)

	// The task token itself is legitimately bound to queue-a, but the
	// emitted command tries to schedule an activity on queue-b.
	cache.Put([]byte("wt-tok"), tokencache.Entry{Namespace: "ns", TaskQueue: "queue-a"})

	req := &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: "ns",
		TaskToken: []byte("wt-tok"),
		Commands: []*commandpb.Command{
			{
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
					ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
						TaskQueue: &taskqueuepb.TaskQueue{Name: "queue-b"},
					},
				},
			},
		},
	}

	_, err, called := callInterceptor(t, interceptor, ctxWithBearer("key-a"),
		"/temporal.api.workflowservice.v1.WorkflowService/RespondWorkflowTaskCompleted",
		req, &workflowservice.RespondWorkflowTaskCompletedResponse{}, nil)

	if called {
		t.Fatalf("handler must not run when a command targets a different task queue")
	}
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %v", err)
	}

	// The valid token must still be usable afterwards - a denied call must
	// not have consumed it.
	if _, ok := cache.Get([]byte("wt-tok")); !ok {
		t.Fatalf("expected token to remain cached after a command-validation denial")
	}
}

func TestInterceptor_AllowsCommandTargetingOwnQueue(t *testing.T) {
	authr := &fakeAuthenticator{identities: map[string]auth.Identity{
		"key-a": {Valid: true, Namespace: "ns", TaskQueue: "queue-a"},
	}}
	cache := tokencache.New(time.Hour, 1000)
	defer cache.Close()
	interceptor := NewInterceptor(authr, cache)

	cache.Put([]byte("wt-tok"), tokencache.Entry{Namespace: "ns", TaskQueue: "queue-a"})

	req := &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: "ns",
		TaskToken: []byte("wt-tok"),
		Commands: []*commandpb.Command{
			{
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
					ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
						TaskQueue: &taskqueuepb.TaskQueue{Name: "queue-a"},
					},
				},
			},
		},
	}

	_, err, called := callInterceptor(t, interceptor, ctxWithBearer("key-a"),
		"/temporal.api.workflowservice.v1.WorkflowService/RespondWorkflowTaskCompleted",
		req, &workflowservice.RespondWorkflowTaskCompletedResponse{}, nil)

	if !called || err != nil {
		t.Fatalf("expected successful call, called=%v err=%v", called, err)
	}
}

func TestInterceptor_GetSystemInfoNeedsOnlyValidIdentity(t *testing.T) {
	authr := &fakeAuthenticator{identities: map[string]auth.Identity{
		"key-a": {Valid: true, Namespace: "ns", TaskQueue: "queue-a"},
	}}
	cache := tokencache.New(time.Hour, 1000)
	defer cache.Close()
	interceptor := NewInterceptor(authr, cache)

	_, err, called := callInterceptor(t, interceptor, ctxWithBearer("key-a"),
		"/temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo",
		&workflowservice.GetSystemInfoRequest{}, &workflowservice.GetSystemInfoResponse{}, nil)

	if !called || err != nil {
		t.Fatalf("expected successful call, called=%v err=%v", called, err)
	}
}

// captureLogs swaps the default slog logger for a JSON handler writing to buf
// for the duration of the test, restoring the original afterward.
func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(buf, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })
	return buf
}

// logLinesWithMsg parses buf as newline-delimited JSON log records and returns
// those whose "msg" equals msg.
func logLinesWithMsg(t *testing.T, buf *bytes.Buffer, msg string) []map[string]any {
	t.Helper()
	var matched []map[string]any
	for _, line := range bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var entry map[string]any
		if err := json.Unmarshal(line, &entry); err != nil {
			t.Fatalf("parsing log line %q: %v", line, err)
		}
		if entry["msg"] == msg {
			matched = append(matched, entry)
		}
	}
	return matched
}

func TestInterceptor_LogsCloudActionPerBillableCommand(t *testing.T) {
	authr := &fakeAuthenticator{identities: map[string]auth.Identity{
		"key-a": {Valid: true, Namespace: "ns", TaskQueue: "queue-a", Subject: "worker-a"},
	}}
	cache := tokencache.New(time.Hour, 1000)
	defer cache.Close()
	interceptor := NewInterceptor(authr, cache)
	cache.Put([]byte("wt-tok"), tokencache.Entry{Namespace: "ns", TaskQueue: "queue-a"})

	buf := captureLogs(t)

	req := &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: "ns",
		TaskToken: []byte("wt-tok"),
		Commands: []*commandpb.Command{
			{Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
				ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					TaskQueue: &taskqueuepb.TaskQueue{Name: "queue-a"},
				},
			}},
			// Not billable - must not produce a line.
			{Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{}},
		},
	}

	_, err, called := callInterceptor(t, interceptor, ctxWithBearer("key-a"),
		"/temporal.api.workflowservice.v1.WorkflowService/RespondWorkflowTaskCompleted",
		req, &workflowservice.RespondWorkflowTaskCompletedResponse{}, nil)
	if !called || err != nil {
		t.Fatalf("expected successful call, called=%v err=%v", called, err)
	}

	lines := logLinesWithMsg(t, buf, "cloud action consumed")
	if len(lines) != 1 {
		t.Fatalf("expected 1 cloud action line, got %d: %s", len(lines), buf.String())
	}
	got := lines[0]
	if got["action"] != actions.ScheduleActivity {
		t.Fatalf("action = %v, want %q", got["action"], actions.ScheduleActivity)
	}
	// JSON numbers decode as float64.
	if got["actions"] != float64(1) {
		t.Fatalf("actions = %v, want 1", got["actions"])
	}
	if got["namespace"] != "ns" || got["task_queue"] != "queue-a" || got["subject"] != "worker-a" {
		t.Fatalf("identity fields wrong: %+v", got)
	}
}

func TestInterceptor_PollEmitsNoCloudAction(t *testing.T) {
	authr := &fakeAuthenticator{identities: map[string]auth.Identity{
		"key-a": {Valid: true, Namespace: "ns", TaskQueue: "queue-a"},
	}}
	cache := tokencache.New(time.Hour, 1000)
	defer cache.Close()
	interceptor := NewInterceptor(authr, cache)

	buf := captureLogs(t)

	req := &workflowservice.PollWorkflowTaskQueueRequest{Namespace: "ns", TaskQueue: &taskqueuepb.TaskQueue{Name: "queue-a"}}
	_, err, called := callInterceptor(t, interceptor, ctxWithBearer("key-a"),
		"/temporal.api.workflowservice.v1.WorkflowService/PollWorkflowTaskQueue",
		req, &workflowservice.PollWorkflowTaskQueueResponse{}, nil)
	if !called || err != nil {
		t.Fatalf("expected successful poll, called=%v err=%v", called, err)
	}

	if lines := logLinesWithMsg(t, buf, "cloud action consumed"); len(lines) != 0 {
		t.Fatalf("expected no cloud action lines for a poll, got %d: %s", len(lines), buf.String())
	}
}

func TestInterceptor_PollAllowsOwnStickyQueue(t *testing.T) {
	authr := &fakeAuthenticator{identities: map[string]auth.Identity{
		"key-a": {Valid: true, Namespace: "ns", TaskQueue: "queue-a"},
	}}
	cache := tokencache.New(time.Hour, 1000)
	defer cache.Close()
	interceptor := NewInterceptor(authr, cache)

	req := &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: "ns",
		TaskQueue: &taskqueuepb.TaskQueue{
			Name:       "some-host:some-random-worker-uuid",
			Kind:       enums.TASK_QUEUE_KIND_STICKY,
			NormalName: "queue-a",
		},
	}
	_, err, called := callInterceptor(t, interceptor, ctxWithBearer("key-a"),
		"/temporal.api.workflowservice.v1.WorkflowService/PollWorkflowTaskQueue",
		req, &workflowservice.PollWorkflowTaskQueueResponse{}, nil)

	if !called || err != nil {
		t.Fatalf("expected sticky poll for own normal queue to be allowed, called=%v err=%v", called, err)
	}
}

func TestInterceptor_PollDeniesStickyQueueForDifferentNormalQueue(t *testing.T) {
	authr := &fakeAuthenticator{identities: map[string]auth.Identity{
		"key-a": {Valid: true, Namespace: "ns", TaskQueue: "queue-a"},
	}}
	cache := tokencache.New(time.Hour, 1000)
	defer cache.Close()
	interceptor := NewInterceptor(authr, cache)

	// Even if the caller fabricates a NormalName equal to a *different*
	// queue than its own, it must still be denied.
	req := &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: "ns",
		TaskQueue: &taskqueuepb.TaskQueue{
			Name:       "victim-host:victim-worker-uuid",
			Kind:       enums.TASK_QUEUE_KIND_STICKY,
			NormalName: "queue-b",
		},
	}
	_, err, called := callInterceptor(t, interceptor, ctxWithBearer("key-a"),
		"/temporal.api.workflowservice.v1.WorkflowService/PollWorkflowTaskQueue",
		req, &workflowservice.PollWorkflowTaskQueueResponse{}, nil)

	if called {
		t.Fatalf("handler must not run for a sticky poll declaring a different normal queue")
	}
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %v", err)
	}
}

func TestInterceptor_DescribeNamespaceScopedToOwnNamespace(t *testing.T) {
	authr := &fakeAuthenticator{identities: map[string]auth.Identity{
		"key-a": {Valid: true, Namespace: "ns-a", TaskQueue: "queue-a"},
	}}
	cache := tokencache.New(time.Hour, 1000)
	defer cache.Close()
	interceptor := NewInterceptor(authr, cache)

	_, err, called := callInterceptor(t, interceptor, ctxWithBearer("key-a"),
		"/temporal.api.workflowservice.v1.WorkflowService/DescribeNamespace",
		&workflowservice.DescribeNamespaceRequest{Namespace: "ns-a"}, &workflowservice.DescribeNamespaceResponse{}, nil)
	if !called || err != nil {
		t.Fatalf("expected successful call for own namespace, called=%v err=%v", called, err)
	}

	_, err, called = callInterceptor(t, interceptor, ctxWithBearer("key-a"),
		"/temporal.api.workflowservice.v1.WorkflowService/DescribeNamespace",
		&workflowservice.DescribeNamespaceRequest{Namespace: "ns-b"}, &workflowservice.DescribeNamespaceResponse{}, nil)
	if called {
		t.Fatalf("handler must not run for a cross-namespace DescribeNamespace call")
	}
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %v", err)
	}
}

// TestInterceptor_DoesNotLeakIdentityIntoOutgoingMetadata guards the
// property that the interceptor only ever attaches the authenticated
// Identity via context.WithValue (consumed via IdentityFromContext for
// logging) - it must never place credentials or identity data into gRPC
// outgoing metadata itself, since only the upstream client interceptor
// (internal/upstream) should ever set outbound auth headers.
func TestInterceptor_DoesNotLeakIdentityIntoOutgoingMetadata(t *testing.T) {
	authr := &fakeAuthenticator{identities: map[string]auth.Identity{
		"key-a": {Valid: true, Namespace: "ns", TaskQueue: "queue-a", Subject: "worker-fleet-a"},
	}}
	cache := tokencache.New(time.Hour, 1000)
	defer cache.Close()
	interceptor := NewInterceptor(authr, cache)

	var sawOutgoing bool
	handler := func(ctx context.Context, req any) (any, error) {
		_, sawOutgoing = metadata.FromOutgoingContext(ctx)
		id, ok := IdentityFromContext(ctx)
		if !ok || id.Subject != "worker-fleet-a" {
			t.Fatalf("expected identity to be attached to context, got %+v (ok=%v)", id, ok)
		}
		return &workflowservice.GetSystemInfoResponse{}, nil
	}

	_, err := interceptor(ctxWithBearer("key-a"), &workflowservice.GetSystemInfoRequest{}, &grpc.UnaryServerInfo{
		FullMethod: "/temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo",
	}, handler)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sawOutgoing {
		t.Fatalf("interceptor must not attach outgoing gRPC metadata itself")
	}
}
