package commandpolicy

import (
	"context"
	"errors"
	"testing"

	commandpb "go.temporal.io/api/command/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"

	"github.com/02strich/temporal-untrusted-workers/internal/auth"
)

// validateBuiltIn runs BuiltIn for an identity pinned to namespace, with the
// workflow task token issued by taskQueue, and checks that any rejection is
// reported as a DeniedError.
func validateBuiltIn(t *testing.T, commands []*commandpb.Command, namespace, taskQueue string) error {
	t.Helper()
	err := BuiltIn{}.Verify(context.Background(), Request{
		Identity:  auth.Identity{Valid: true, Namespace: namespace, TaskQueues: []string{taskQueue}},
		TaskQueue: taskQueue,
		Commands:  commands,
	})
	var denied DeniedError
	if err != nil && !errors.As(err, &denied) {
		t.Fatalf("expected DeniedError, got %T: %v", err, err)
	}
	return err
}

func TestBuiltIn_ScheduleActivityTask(t *testing.T) {
	commands := []*commandpb.Command{
		{
			Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
				ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					TaskQueue: &taskqueuepb.TaskQueue{Name: "queue-a"},
				},
			},
		},
	}

	if err := validateBuiltIn(t, commands, "ns-a", "queue-a"); err != nil {
		t.Fatalf("expected same-queue command to pass, got: %v", err)
	}
	if err := validateBuiltIn(t, commands, "ns-a", "queue-b"); err == nil {
		t.Fatalf("expected cross-queue ScheduleActivityTask command to be rejected")
	}
}

func TestBuiltIn_StartChildWorkflowExecution(t *testing.T) {
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
	if err := validateBuiltIn(t, sameQueue, "ns-a", "queue-a"); err != nil {
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
	if err := validateBuiltIn(t, crossQueue, "ns-a", "queue-a"); err == nil {
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
	if err := validateBuiltIn(t, crossNamespace, "ns-a", "queue-a"); err == nil {
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
	if err := validateBuiltIn(t, implicitNamespace, "ns-a", "queue-a"); err != nil {
		t.Fatalf("expected unset namespace to be treated as same-namespace, got: %v", err)
	}
}

func TestBuiltIn_ContinueAsNewWorkflowExecution(t *testing.T) {
	// Unset TaskQueue means "same queue" and must be allowed.
	implicitQueue := []*commandpb.Command{
		{
			Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{
				ContinueAsNewWorkflowExecutionCommandAttributes: &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{},
			},
		},
	}
	if err := validateBuiltIn(t, implicitQueue, "ns-a", "queue-a"); err != nil {
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
	if err := validateBuiltIn(t, crossQueue, "ns-a", "queue-a"); err == nil {
		t.Fatalf("expected cross-queue ContinueAsNewWorkflowExecution command to be rejected")
	}
}

func TestBuiltIn_UntargetedCommandsPassThrough(t *testing.T) {
	commands := []*commandpb.Command{
		{Attributes: &commandpb.Command_RecordMarkerCommandAttributes{RecordMarkerCommandAttributes: &commandpb.RecordMarkerCommandAttributes{MarkerName: "m"}}},
		{Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}}},
	}
	if err := validateBuiltIn(t, commands, "ns-a", "queue-a"); err != nil {
		t.Fatalf("expected untargeted commands to pass through, got: %v", err)
	}
}
