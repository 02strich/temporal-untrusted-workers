package actions

import (
	"testing"

	commandpb "go.temporal.io/api/command/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/proto"
)

func respond(commands ...*commandpb.Command) *workflowservice.RespondWorkflowTaskCompletedRequest {
	return &workflowservice.RespondWorkflowTaskCompletedRequest{Commands: commands}
}

func TestFromRequest_BillableCommandMappings(t *testing.T) {
	tests := []struct {
		name      string
		command   *commandpb.Command
		wantType  string
		wantCount int
	}{
		{"schedule_activity", &commandpb.Command{Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{}}, ScheduleActivity, 1},
		{"start_timer", &commandpb.Command{Attributes: &commandpb.Command_StartTimerCommandAttributes{}}, StartTimer, 1},
		{"start_child_workflow", &commandpb.Command{Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{}}, StartChildWorkflow, 2},
		{"continue_as_new", &commandpb.Command{Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{}}, ContinueAsNewWorkflow, 1},
		{"signal_external", &commandpb.Command{Attributes: &commandpb.Command_SignalExternalWorkflowExecutionCommandAttributes{}}, SignalExternalWorkflow, 1},
		{"upsert_search_attributes", &commandpb.Command{Attributes: &commandpb.Command_UpsertWorkflowSearchAttributesCommandAttributes{}}, UpsertSearchAttributes, 1},
		{"schedule_nexus", &commandpb.Command{Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{}}, ScheduleNexusOperation, 1},
		{"cancel_nexus", &commandpb.Command{Attributes: &commandpb.Command_RequestCancelNexusOperationCommandAttributes{}}, RequestCancelNexusOperation, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FromRequest(respond(tt.command))
			if len(got) != 1 {
				t.Fatalf("expected 1 action, got %d (%+v)", len(got), got)
			}
			if got[0].Type != tt.wantType || got[0].Count != tt.wantCount {
				t.Fatalf("got %+v, want {Type:%q Count:%d}", got[0], tt.wantType, tt.wantCount)
			}
		})
	}
}

func TestFromRequest_NonBillableCommandsProduceNothing(t *testing.T) {
	req := respond(
		&commandpb.Command{Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{}},
		&commandpb.Command{Attributes: &commandpb.Command_FailWorkflowExecutionCommandAttributes{}},
		&commandpb.Command{Attributes: &commandpb.Command_CancelWorkflowExecutionCommandAttributes{}},
		&commandpb.Command{Attributes: &commandpb.Command_CancelTimerCommandAttributes{}},
		&commandpb.Command{Attributes: &commandpb.Command_RequestCancelActivityTaskCommandAttributes{}},
		&commandpb.Command{Attributes: &commandpb.Command_RequestCancelExternalWorkflowExecutionCommandAttributes{}},
		&commandpb.Command{Attributes: &commandpb.Command_RecordMarkerCommandAttributes{}},
		&commandpb.Command{Attributes: &commandpb.Command_ProtocolMessageCommandAttributes{}},
		&commandpb.Command{Attributes: &commandpb.Command_ModifyWorkflowPropertiesCommandAttributes{}},
	)
	if got := FromRequest(req); got != nil {
		t.Fatalf("expected no actions for non-billable commands, got %+v", got)
	}
}

func TestFromRequest_MixedCommandsYieldOnlyBillable(t *testing.T) {
	req := respond(
		&commandpb.Command{Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{}},
		&commandpb.Command{Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{}}, // non-billable
		&commandpb.Command{Attributes: &commandpb.Command_StartTimerCommandAttributes{}},
	)
	got := FromRequest(req)
	if len(got) != 2 {
		t.Fatalf("expected 2 actions, got %d (%+v)", len(got), got)
	}
	if got[0].Type != ScheduleActivity || got[1].Type != StartTimer {
		t.Fatalf("unexpected actions, got %+v", got)
	}
}

func TestFromRequest_ActivityHeartbeat(t *testing.T) {
	got := FromRequest(&workflowservice.RecordActivityTaskHeartbeatRequest{})
	if len(got) != 1 || got[0].Type != RecordActivityHeartbeat || got[0].Count != 1 {
		t.Fatalf("expected one record_activity_heartbeat action, got %+v", got)
	}
}

func TestFromRequest_NonActionRequests(t *testing.T) {
	nonActions := []proto.Message{
		&workflowservice.PollWorkflowTaskQueueRequest{},
		&workflowservice.PollActivityTaskQueueRequest{},
		&workflowservice.PollNexusTaskQueueRequest{},
		&workflowservice.RespondActivityTaskCompletedRequest{},
		&workflowservice.RespondWorkflowTaskFailedRequest{},
		&workflowservice.RespondNexusTaskCompletedRequest{},
		&workflowservice.RespondNexusTaskFailedRequest{},
		&workflowservice.DescribeNamespaceRequest{},
		&workflowservice.ShutdownWorkerRequest{},
		&workflowservice.RecordWorkerHeartbeatRequest{},
	}
	for _, req := range nonActions {
		if got := FromRequest(req); got != nil {
			t.Fatalf("%T: expected nil, got %+v", req, got)
		}
	}
}

func TestFromRequest_EmptyRespondHasNoActions(t *testing.T) {
	if got := FromRequest(&workflowservice.RespondWorkflowTaskCompletedRequest{}); got != nil {
		t.Fatalf("expected nil for a completion with no commands, got %+v", got)
	}
}
