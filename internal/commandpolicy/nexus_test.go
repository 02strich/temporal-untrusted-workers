package commandpolicy

import (
	"context"
	"errors"
	"testing"
	"time"

	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	commandpolicypb "github.com/02strich/temporal-untrusted-workers/gen/commandpolicy/v1"
	"github.com/02strich/temporal-untrusted-workers/internal/auth"
)

// fakeNexusClient implements the two standalone-Nexus RPCs NexusVerifier
// uses; any other method panics via the nil embedded interface.
type fakeNexusClient struct {
	workflowservice.WorkflowServiceClient

	startErr  error
	starts    []*workflowservice.StartNexusOperationExecutionRequest
	polls     []*workflowservice.PollNexusOperationExecutionRequest
	responses []*workflowservice.PollNexusOperationExecutionResponse
	// block makes every poll wait for ctx to expire.
	block bool
}

func (f *fakeNexusClient) StartNexusOperationExecution(_ context.Context, req *workflowservice.StartNexusOperationExecutionRequest, _ ...grpc.CallOption) (*workflowservice.StartNexusOperationExecutionResponse, error) {
	f.starts = append(f.starts, req)
	if f.startErr != nil {
		return nil, f.startErr
	}
	return &workflowservice.StartNexusOperationExecutionResponse{RunId: "run-1", Started: true}, nil
}

func (f *fakeNexusClient) PollNexusOperationExecution(ctx context.Context, req *workflowservice.PollNexusOperationExecutionRequest, _ ...grpc.CallOption) (*workflowservice.PollNexusOperationExecutionResponse, error) {
	f.polls = append(f.polls, req)
	if f.block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	resp := f.responses[0]
	if len(f.responses) > 1 {
		f.responses = f.responses[1:]
	}
	return resp, nil
}

func closedWithPayload(encoding string, data []byte) *workflowservice.PollNexusOperationExecutionResponse {
	return &workflowservice.PollNexusOperationExecutionResponse{
		RunId:     "run-1",
		WaitStage: enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED,
		Outcome: &workflowservice.PollNexusOperationExecutionResponse_Result{Result: &commonpb.Payload{
			Metadata: map[string][]byte{"encoding": []byte(encoding)},
			Data:     data,
		}},
	}
}

// closedWithResult returns a closed poll response whose result is data, a
// JSON document, encoded as "json/protobuf" like the Temporal SDKs do.
func closedWithResult(data string) *workflowservice.PollNexusOperationExecutionResponse {
	return closedWithPayload("json/protobuf", []byte(data))
}

func newTestVerifier(client *fakeNexusClient) *NexusVerifier {
	return &NexusVerifier{
		Client:    client,
		Namespace: "verifier-ns",
		Endpoint:  "verifier-endpoint",
		Service:   "policy",
		Operation: DefaultNexusOperation,
		Timeout:   time.Second,
	}
}

func testRequest() Request {
	return Request{
		Identity:  auth.Identity{Valid: true, Namespace: "ns-a", TaskQueues: []string{"queue-a", "queue-b"}, Subject: "fleet-a"},
		TaskQueue: "queue-a",
		Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
			Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
				ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId: "act-1",
					TaskQueue:  &taskqueuepb.TaskQueue{Name: "queue-a"},
				},
			},
		}},
	}
}

func TestNexusVerifier_Allowed(t *testing.T) {
	client := &fakeNexusClient{responses: []*workflowservice.PollNexusOperationExecutionResponse{closedWithResult(`{"allowed":true}`)}}
	if err := newTestVerifier(client).Verify(context.Background(), testRequest()); err != nil {
		t.Fatalf("expected allow, got %v", err)
	}

	if len(client.starts) != 1 {
		t.Fatalf("expected one start, got %d", len(client.starts))
	}
	start := client.starts[0]
	if start.GetNamespace() != "verifier-ns" || start.GetEndpoint() != "verifier-endpoint" || start.GetService() != "policy" || start.GetOperation() != DefaultNexusOperation {
		t.Fatalf("unexpected start request: %+v", start)
	}
	if start.GetOperationId() == "" || start.GetRequestId() == "" {
		t.Fatalf("expected operation and request ids to be set")
	}
	if got := start.GetScheduleToCloseTimeout().AsDuration(); got != time.Second {
		t.Fatalf("expected schedule-to-close timeout of 1s, got %v", got)
	}

	poll := client.polls[0]
	if poll.GetOperationId() != start.GetOperationId() || poll.GetRunId() != "run-1" || poll.GetWaitStage() != enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED {
		t.Fatalf("unexpected poll request: %+v", poll)
	}
}

func TestNexusVerifier_InputPayload(t *testing.T) {
	client := &fakeNexusClient{responses: []*workflowservice.PollNexusOperationExecutionResponse{closedWithResult(`{"allowed":true}`)}}
	if err := newTestVerifier(client).Verify(context.Background(), testRequest()); err != nil {
		t.Fatalf("Verify: %v", err)
	}

	input := client.starts[0].GetInput()
	if enc := string(input.GetMetadata()["encoding"]); enc != "json/protobuf" {
		t.Fatalf("expected json/protobuf encoding, got %q", enc)
	}
	if mt := string(input.GetMetadata()["messageType"]); mt != "temporal_untrusted_workers.commandpolicy.v1.VerifyCommandsRequest" {
		t.Fatalf("unexpected messageType %q", mt)
	}
	var got commandpolicypb.VerifyCommandsRequest
	if err := protojson.Unmarshal(input.GetData(), &got); err != nil {
		t.Fatalf("decoding input: %v", err)
	}
	if got.GetNamespace() != "ns-a" || got.GetTaskQueue() != "queue-a" || got.GetSubject() != "fleet-a" || len(got.GetTaskQueues()) != 2 {
		t.Fatalf("unexpected input: %v", &got)
	}
	if cmds := got.GetCommands(); len(cmds) != 1 || cmds[0].GetScheduleActivityTaskCommandAttributes().GetActivityId() != "act-1" {
		t.Fatalf("commands did not round-trip: %v", cmds)
	}
}

func TestNexusVerifier_Denied(t *testing.T) {
	client := &fakeNexusClient{responses: []*workflowservice.PollNexusOperationExecutionResponse{closedWithResult(`{"allowed":false,"reason":"no activities"}`)}}
	err := newTestVerifier(client).Verify(context.Background(), testRequest())
	var denied DeniedError
	if !errors.As(err, &denied) || denied.Reason != "no activities" {
		t.Fatalf("expected DeniedError with reason, got %T: %v", err, err)
	}
}

func TestNexusVerifier_BinaryProtoResult(t *testing.T) {
	data, err := proto.Marshal(&commandpolicypb.VerifyCommandsResponse{Allowed: false, Reason: "binary says no"})
	if err != nil {
		t.Fatal(err)
	}
	client := &fakeNexusClient{responses: []*workflowservice.PollNexusOperationExecutionResponse{closedWithPayload("binary/protobuf", data)}}
	err = newTestVerifier(client).Verify(context.Background(), testRequest())
	var denied DeniedError
	if !errors.As(err, &denied) || denied.Reason != "binary says no" {
		t.Fatalf("expected DeniedError from binary result, got %T: %v", err, err)
	}
}

func TestNexusVerifier_DeniedWithoutReason(t *testing.T) {
	// protojson omits false fields, so a denial from an SDK can be "{}".
	client := &fakeNexusClient{responses: []*workflowservice.PollNexusOperationExecutionResponse{closedWithResult(`{}`)}}
	err := newTestVerifier(client).Verify(context.Background(), testRequest())
	var denied DeniedError
	if !errors.As(err, &denied) || denied.Reason == "" {
		t.Fatalf("expected DeniedError with a default reason, got %T: %v", err, err)
	}
}

func TestNexusVerifier_PollsUntilClosed(t *testing.T) {
	client := &fakeNexusClient{responses: []*workflowservice.PollNexusOperationExecutionResponse{
		{RunId: "run-1", WaitStage: enumspb.NEXUS_OPERATION_WAIT_STAGE_STARTED},
		closedWithResult(`{"allowed":true}`),
	}}
	if err := newTestVerifier(client).Verify(context.Background(), testRequest()); err != nil {
		t.Fatalf("expected allow, got %v", err)
	}
	if len(client.polls) != 2 {
		t.Fatalf("expected two polls, got %d", len(client.polls))
	}
}

func assertUnavailable(t *testing.T, err error) {
	t.Helper()
	var unavailable UnavailableError
	if !errors.As(err, &unavailable) {
		t.Fatalf("expected UnavailableError, got %T: %v", err, err)
	}
}

func TestNexusVerifier_OperationFailure(t *testing.T) {
	client := &fakeNexusClient{responses: []*workflowservice.PollNexusOperationExecutionResponse{{
		RunId:     "run-1",
		WaitStage: enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED,
		Outcome:   &workflowservice.PollNexusOperationExecutionResponse_Failure{Failure: &failurepb.Failure{Message: "handler crashed"}},
	}}}
	assertUnavailable(t, newTestVerifier(client).Verify(context.Background(), testRequest()))
}

func TestNexusVerifier_StartError(t *testing.T) {
	client := &fakeNexusClient{startErr: errors.New("endpoint not found")}
	assertUnavailable(t, newTestVerifier(client).Verify(context.Background(), testRequest()))
}

func TestNexusVerifier_MalformedResult(t *testing.T) {
	client := &fakeNexusClient{responses: []*workflowservice.PollNexusOperationExecutionResponse{closedWithResult(`not json`)}}
	assertUnavailable(t, newTestVerifier(client).Verify(context.Background(), testRequest()))
}

func TestNexusVerifier_MissingResult(t *testing.T) {
	client := &fakeNexusClient{responses: []*workflowservice.PollNexusOperationExecutionResponse{{
		RunId:     "run-1",
		WaitStage: enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED,
	}}}
	assertUnavailable(t, newTestVerifier(client).Verify(context.Background(), testRequest()))
}

func TestNexusVerifier_Timeout(t *testing.T) {
	client := &fakeNexusClient{block: true}
	v := newTestVerifier(client)
	v.Timeout = 20 * time.Millisecond
	assertUnavailable(t, v.Verify(context.Background(), testRequest()))
}
