// Package actions maps the worker RPCs the proxy forwards to the billable
// Temporal Cloud "Actions" they represent, so the proxy can log action volume
// per identity. See https://docs.temporal.io/cloud/actions.
//
// Only worker-attributable actions are observable here: the commands a
// workflow emits in RespondWorkflowTaskCompleted, and activity heartbeats.
// Client-initiated actions (start_workflow, signal_workflow, query_workflow,
// client-initiated updates, schedules, reset_workflow, history export) travel
// on client RPCs that never traverse this proxy (they are denied by the
// allowlist), so they cannot be counted. The result is a worker-attributable
// subset - a lower bound - of a namespace's billed actions, not the full bill.
package actions

import (
	commandpb "go.temporal.io/api/command/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/proto"
)

// Billable action codes, matching the identifiers used in the Temporal Cloud
// actions documentation. Only the subset observable in worker traffic is
// defined here.
const (
	ScheduleActivity            = "schedule_activity"
	StartTimer                  = "start_timer"
	StartChildWorkflow          = "start_child_workflow"
	ContinueAsNewWorkflow       = "continue_as_new_workflow"
	SignalExternalWorkflow      = "signal_external_workflow"
	UpsertSearchAttributes      = "upsert_workflow_search_attributes"
	ScheduleNexusOperation      = "schedule_nexus_operation"
	RequestCancelNexusOperation = "request_nexus_operation_cancellation"
	RecordActivityHeartbeat     = "record_activity_heartbeat"
)

// Action is one billable event observed in a forwarded worker request. Count
// is how many billed actions the single source event represents - almost
// always 1, but a child-workflow start is billed as two actions.
type Action struct {
	Type  string
	Count int
}

// FromRequest returns the billable actions observable in a worker request that
// the proxy is about to forward (or has just successfully forwarded). It
// returns nil for requests that carry no worker-attributable action.
func FromRequest(req proto.Message) []Action {
	switch r := req.(type) {
	case *workflowservice.RespondWorkflowTaskCompletedRequest:
		var out []Action
		for _, cmd := range r.GetCommands() {
			if a, ok := fromCommand(cmd); ok {
				out = append(out, a)
			}
		}
		return out
	case *workflowservice.RecordActivityTaskHeartbeatRequest:
		// A heartbeat is billable only if it reaches the server; the SDK
		// throttles them client-side, so any heartbeat the proxy forwards is
		// one that counts.
		return []Action{{Type: RecordActivityHeartbeat, Count: 1}}
	default:
		return nil
	}
}

// fromCommand maps a single workflow command to its billable action, if any.
// Command types that carry no billing (workflow completion/failure/cancel,
// timer/activity/external-workflow cancellation, markers, protocol messages,
// and property modifications) return ok=false. RecordMarker is deliberately
// excluded: markers cover side effects, local activities, and versioning,
// which map to actions ambiguously.
func fromCommand(cmd *commandpb.Command) (Action, bool) {
	switch cmd.GetAttributes().(type) {
	case *commandpb.Command_ScheduleActivityTaskCommandAttributes:
		return Action{Type: ScheduleActivity, Count: 1}, true
	case *commandpb.Command_StartTimerCommandAttributes:
		return Action{Type: StartTimer, Count: 1}, true
	case *commandpb.Command_StartChildWorkflowExecutionCommandAttributes:
		// Temporal Cloud bills a child-workflow start as two actions (the
		// durable intent plus the start attempt).
		return Action{Type: StartChildWorkflow, Count: 2}, true
	case *commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes:
		return Action{Type: ContinueAsNewWorkflow, Count: 1}, true
	case *commandpb.Command_SignalExternalWorkflowExecutionCommandAttributes:
		return Action{Type: SignalExternalWorkflow, Count: 1}, true
	case *commandpb.Command_UpsertWorkflowSearchAttributesCommandAttributes:
		return Action{Type: UpsertSearchAttributes, Count: 1}, true
	case *commandpb.Command_ScheduleNexusOperationCommandAttributes:
		return Action{Type: ScheduleNexusOperation, Count: 1}, true
	case *commandpb.Command_RequestCancelNexusOperationCommandAttributes:
		return Action{Type: RequestCancelNexusOperation, Count: 1}, true
	default:
		return Action{}, false
	}
}
