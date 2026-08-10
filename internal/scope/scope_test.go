package scope

import (
	"testing"

	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/proto"
)

func TestRequestNamespace(t *testing.T) {
	if ns, ok := RequestNamespace(&workflowservice.PollWorkflowTaskQueueRequest{Namespace: "ns-a"}); !ok || ns != "ns-a" {
		t.Fatalf("PollWorkflowTaskQueueRequest: got (%q, %v)", ns, ok)
	}
	if ns, ok := RequestNamespace(&workflowservice.PollActivityTaskQueueRequest{Namespace: "ns-a"}); !ok || ns != "ns-a" {
		t.Fatalf("PollActivityTaskQueueRequest: got (%q, %v)", ns, ok)
	}
	if ns, ok := RequestNamespace(&workflowservice.RespondWorkflowTaskCompletedRequest{Namespace: "ns-a"}); !ok || ns != "ns-a" {
		t.Fatalf("RespondWorkflowTaskCompletedRequest: got (%q, %v)", ns, ok)
	}
	if ns, ok := RequestNamespace(&workflowservice.RespondWorkflowTaskFailedRequest{Namespace: "ns-a"}); !ok || ns != "ns-a" {
		t.Fatalf("RespondWorkflowTaskFailedRequest: got (%q, %v)", ns, ok)
	}
	if ns, ok := RequestNamespace(&workflowservice.RecordActivityTaskHeartbeatRequest{Namespace: "ns-a"}); !ok || ns != "ns-a" {
		t.Fatalf("RecordActivityTaskHeartbeatRequest: got (%q, %v)", ns, ok)
	}
	if ns, ok := RequestNamespace(&workflowservice.RespondActivityTaskCompletedRequest{Namespace: "ns-a"}); !ok || ns != "ns-a" {
		t.Fatalf("RespondActivityTaskCompletedRequest: got (%q, %v)", ns, ok)
	}
	if ns, ok := RequestNamespace(&workflowservice.RespondActivityTaskFailedRequest{Namespace: "ns-a"}); !ok || ns != "ns-a" {
		t.Fatalf("RespondActivityTaskFailedRequest: got (%q, %v)", ns, ok)
	}
	if ns, ok := RequestNamespace(&workflowservice.RespondActivityTaskCanceledRequest{Namespace: "ns-a"}); !ok || ns != "ns-a" {
		t.Fatalf("RespondActivityTaskCanceledRequest: got (%q, %v)", ns, ok)
	}
	if ns, ok := RequestNamespace(&workflowservice.RespondQueryTaskCompletedRequest{Namespace: "ns-a"}); !ok || ns != "ns-a" {
		t.Fatalf("RespondQueryTaskCompletedRequest: got (%q, %v)", ns, ok)
	}
	if ns, ok := RequestNamespace(&workflowservice.DescribeNamespaceRequest{Namespace: "ns-a"}); !ok || ns != "ns-a" {
		t.Fatalf("DescribeNamespaceRequest: got (%q, %v)", ns, ok)
	}

	if _, ok := RequestNamespace(&workflowservice.GetSystemInfoRequest{}); ok {
		t.Fatalf("GetSystemInfoRequest should not have a namespace")
	}
	if _, ok := RequestNamespace(&workflowservice.StartWorkflowExecutionRequest{}); ok {
		t.Fatalf("unscoped RPC type should not match RequestNamespace")
	}
}

func TestRequestTaskQueueName(t *testing.T) {
	req := &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: "ns-a",
		TaskQueue: &taskqueuepb.TaskQueue{Name: "queue-a"},
	}
	if tq, ok := RequestTaskQueueName(req); !ok || tq != "queue-a" {
		t.Fatalf("got (%q, %v)", tq, ok)
	}

	act := &workflowservice.PollActivityTaskQueueRequest{
		Namespace: "ns-a",
		TaskQueue: &taskqueuepb.TaskQueue{Name: "queue-a"},
	}
	if tq, ok := RequestTaskQueueName(act); !ok || tq != "queue-a" {
		t.Fatalf("got (%q, %v)", tq, ok)
	}

	if _, ok := RequestTaskQueueName(&workflowservice.RespondActivityTaskCompletedRequest{}); ok {
		t.Fatalf("token-scoped RPC should not match RequestTaskQueueName")
	}
}

func TestRequestTaskQueue_StickyKindAndNormalName(t *testing.T) {
	req := &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: "ns-a",
		TaskQueue: &taskqueuepb.TaskQueue{
			Name:       "host:worker-uuid",
			Kind:       enumspb.TASK_QUEUE_KIND_STICKY,
			NormalName: "queue-a",
		},
	}
	tq, ok := RequestTaskQueue(req)
	if !ok {
		t.Fatalf("expected RequestTaskQueue to match PollWorkflowTaskQueueRequest")
	}
	if tq.GetKind() != enumspb.TASK_QUEUE_KIND_STICKY || tq.GetNormalName() != "queue-a" {
		t.Fatalf("unexpected task queue: %+v", tq)
	}

	if _, ok := RequestTaskQueue(&workflowservice.RespondActivityTaskCompletedRequest{}); ok {
		t.Fatalf("token-scoped RPC should not match RequestTaskQueue")
	}
}

func TestRequestTaskToken(t *testing.T) {
	token := []byte("tok-1")

	cases := []proxyReq{
		{name: "RespondWorkflowTaskCompleted", req: &workflowservice.RespondWorkflowTaskCompletedRequest{TaskToken: token}},
		{name: "RespondWorkflowTaskFailed", req: &workflowservice.RespondWorkflowTaskFailedRequest{TaskToken: token}},
		{name: "RecordActivityTaskHeartbeat", req: &workflowservice.RecordActivityTaskHeartbeatRequest{TaskToken: token}},
		{name: "RespondActivityTaskCompleted", req: &workflowservice.RespondActivityTaskCompletedRequest{TaskToken: token}},
		{name: "RespondActivityTaskFailed", req: &workflowservice.RespondActivityTaskFailedRequest{TaskToken: token}},
		{name: "RespondActivityTaskCanceled", req: &workflowservice.RespondActivityTaskCanceledRequest{TaskToken: token}},
		{name: "RespondQueryTaskCompleted", req: &workflowservice.RespondQueryTaskCompletedRequest{TaskToken: token}},
	}

	for _, c := range cases {
		got, ok := RequestTaskToken(c.req)
		if !ok || string(got) != string(token) {
			t.Fatalf("%s: got (%v, %v)", c.name, got, ok)
		}
	}

	if _, ok := RequestTaskToken(&workflowservice.PollWorkflowTaskQueueRequest{}); ok {
		t.Fatalf("poll RPC should not match RequestTaskToken")
	}
}

// proxyReq pairs a name with a request for table-driven tests.
type proxyReq struct {
	name string
	req  proto.Message
}

func TestCollectResponseTaskTokens_Poll(t *testing.T) {
	tok := []byte("poll-tok")

	if got := CollectResponseTaskTokens(&workflowservice.PollWorkflowTaskQueueResponse{TaskToken: tok}); len(got) != 1 || string(got[0]) != string(tok) {
		t.Fatalf("unexpected tokens: %v", got)
	}
	if got := CollectResponseTaskTokens(&workflowservice.PollActivityTaskQueueResponse{TaskToken: tok}); len(got) != 1 || string(got[0]) != string(tok) {
		t.Fatalf("unexpected tokens: %v", got)
	}

	// An empty poll response (no task available - the common long-poll
	// timeout case) must not yield a token.
	if got := CollectResponseTaskTokens(&workflowservice.PollWorkflowTaskQueueResponse{}); len(got) != 0 {
		t.Fatalf("expected no tokens for empty poll response, got %v", got)
	}
}

func TestCollectResponseTaskTokens_EagerDispatch(t *testing.T) {
	tokA := []byte("workflow-task-tok")
	tokB := []byte("activity-task-tok-1")
	tokC := []byte("activity-task-tok-2")

	resp := &workflowservice.RespondWorkflowTaskCompletedResponse{
		WorkflowTask: &workflowservice.PollWorkflowTaskQueueResponse{TaskToken: tokA},
		ActivityTasks: []*workflowservice.PollActivityTaskQueueResponse{
			{TaskToken: tokB},
			{TaskToken: tokC},
		},
	}

	got := CollectResponseTaskTokens(resp)
	if len(got) != 3 {
		t.Fatalf("expected 3 tokens, got %d: %v", len(got), got)
	}
	want := map[string]bool{string(tokA): true, string(tokB): true, string(tokC): true}
	for _, g := range got {
		if !want[string(g)] {
			t.Fatalf("unexpected token %v in result", g)
		}
		delete(want, string(g))
	}
	if len(want) != 0 {
		t.Fatalf("missing expected tokens: %v", want)
	}
}

func TestCollectResponseTaskTokens_NoEagerDispatch(t *testing.T) {
	// The common case: workflow task completed, no new sticky task and no
	// eager activity dispatch.
	resp := &workflowservice.RespondWorkflowTaskCompletedResponse{}
	if got := CollectResponseTaskTokens(resp); len(got) != 0 {
		t.Fatalf("expected no tokens, got %v", got)
	}
}

func TestValidateCommands_ScheduleActivityTask(t *testing.T) {
	commands := []*commandpb.Command{
		{
			Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
				ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					TaskQueue: &taskqueuepb.TaskQueue{Name: "queue-a"},
				},
			},
		},
	}

	if err := ValidateCommands(commands, "ns-a", "queue-a"); err != nil {
		t.Fatalf("expected same-queue command to pass, got: %v", err)
	}
	if err := ValidateCommands(commands, "ns-a", "queue-b"); err == nil {
		t.Fatalf("expected cross-queue ScheduleActivityTask command to be rejected")
	}
}

func TestValidateCommands_StartChildWorkflowExecution(t *testing.T) {
	sameQueue := []*commandpb.Command{
		{
			Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{
				StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
					Namespace: "ns-a",
					TaskQueue: &taskqueuepb.TaskQueue{Name: "queue-a"},
				},
			},
		},
	}
	if err := ValidateCommands(sameQueue, "ns-a", "queue-a"); err != nil {
		t.Fatalf("expected matching namespace+queue to pass, got: %v", err)
	}

	crossQueue := []*commandpb.Command{
		{
			Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{
				StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
					Namespace: "ns-a",
					TaskQueue: &taskqueuepb.TaskQueue{Name: "queue-b"},
				},
			},
		},
	}
	if err := ValidateCommands(crossQueue, "ns-a", "queue-a"); err == nil {
		t.Fatalf("expected cross-queue StartChildWorkflowExecution command to be rejected")
	}

	crossNamespace := []*commandpb.Command{
		{
			Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{
				StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
					Namespace: "ns-b",
					TaskQueue: &taskqueuepb.TaskQueue{Name: "queue-a"},
				},
			},
		},
	}
	if err := ValidateCommands(crossNamespace, "ns-a", "queue-a"); err == nil {
		t.Fatalf("expected cross-namespace StartChildWorkflowExecution command to be rejected")
	}

	// Namespace unset means "same as parent" and must be allowed.
	implicitNamespace := []*commandpb.Command{
		{
			Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{
				StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
					TaskQueue: &taskqueuepb.TaskQueue{Name: "queue-a"},
				},
			},
		},
	}
	if err := ValidateCommands(implicitNamespace, "ns-a", "queue-a"); err != nil {
		t.Fatalf("expected unset namespace to be treated as same-namespace, got: %v", err)
	}
}

func TestValidateCommands_ContinueAsNewWorkflowExecution(t *testing.T) {
	// Unset TaskQueue means "same queue" and must be allowed.
	implicitQueue := []*commandpb.Command{
		{
			Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{
				ContinueAsNewWorkflowExecutionCommandAttributes: &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{},
			},
		},
	}
	if err := ValidateCommands(implicitQueue, "ns-a", "queue-a"); err != nil {
		t.Fatalf("expected unset task queue to be treated as same-queue, got: %v", err)
	}

	crossQueue := []*commandpb.Command{
		{
			Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{
				ContinueAsNewWorkflowExecutionCommandAttributes: &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
					TaskQueue: &taskqueuepb.TaskQueue{Name: "queue-b"},
				},
			},
		},
	}
	if err := ValidateCommands(crossQueue, "ns-a", "queue-a"); err == nil {
		t.Fatalf("expected cross-queue ContinueAsNewWorkflowExecution command to be rejected")
	}
}

func TestValidateCommands_UntargetedCommandsPassThrough(t *testing.T) {
	commands := []*commandpb.Command{
		{Attributes: &commandpb.Command_RecordMarkerCommandAttributes{RecordMarkerCommandAttributes: &commandpb.RecordMarkerCommandAttributes{MarkerName: "m"}}},
		{Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}}},
	}
	if err := ValidateCommands(commands, "ns-a", "queue-a"); err != nil {
		t.Fatalf("expected untargeted commands to pass through, got: %v", err)
	}
}
