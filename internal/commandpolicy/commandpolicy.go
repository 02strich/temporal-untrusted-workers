// Package commandpolicy decides whether the commands emitted by a
// RespondWorkflowTaskCompleted call may be forwarded upstream.
//
// The decision is pluggable via the Verifier interface: BuiltIn enforces the
// proxy's default namespace/task-queue scoping, and NexusVerifier delegates
// the decision to an operator-provided Temporal Nexus service.
package commandpolicy

import (
	"context"
	"fmt"

	commandpb "go.temporal.io/api/command/v1"

	"github.com/02strich/temporal-untrusted-workers/internal/auth"
)

// Request describes one RespondWorkflowTaskCompleted call to be verified.
type Request struct {
	// Identity is the authenticated caller.
	Identity auth.Identity
	// TaskQueue is the task queue that issued the workflow task token being
	// completed - always one of Identity.TaskQueues.
	TaskQueue string
	// Commands are the commands the worker emitted.
	Commands []*commandpb.Command
}

// Verifier decides whether a set of workflow commands may be forwarded.
//
// Verify returns nil to allow the call, a DeniedError when the commands are
// not permitted, or an UnavailableError when no decision could be made (the
// proxy fails closed in that case). Any other error is treated as a denial.
type Verifier interface {
	Verify(ctx context.Context, req Request) error
}

// DeniedError reports that a Verifier rejected the commands.
type DeniedError struct {
	Reason string
}

func (e DeniedError) Error() string {
	return e.Reason
}

// UnavailableError reports that a Verifier could not reach a decision.
type UnavailableError struct {
	Err error
}

func (e UnavailableError) Error() string {
	return fmt.Sprintf("command verifier unavailable: %v", e.Err)
}

func (e UnavailableError) Unwrap() error {
	return e.Err
}

// BuiltIn is the default Verifier: it checks every command against the
// caller's authorized namespace and the task queue that issued the workflow
// task token. Only command types that can direct work elsewhere are checked:
//
//   - ScheduleActivityTaskCommandAttributes: TaskQueue.Name must equal
//     the token's task queue. Activities are always scheduled in the
//     workflow's own namespace (there is no namespace override on this
//     command).
//   - StartChildWorkflowExecutionCommandAttributes: TaskQueue.Name must equal
//     the token's task queue; Namespace, if set, must equal the identity's
//     namespace (an empty Namespace means "same as parent").
//   - ContinueAsNewWorkflowExecutionCommandAttributes: TaskQueue.Name, if
//     set, must equal the token's task queue (an empty TaskQueue means "same
//     queue").
//
// All other command types (StartTimer, CompleteWorkflowExecution,
// FailWorkflowExecution, RequestCancelActivityTask, CancelTimer,
// CancelWorkflowExecution, RequestCancelExternalWorkflowExecution,
// RecordMarker, SignalExternalWorkflowExecution,
// UpsertWorkflowSearchAttributes, ProtocolMessage,
// ModifyWorkflowProperties, Nexus operation commands, ...) carry no
// task-queue/namespace targeting and are not checked.
type BuiltIn struct{}

func (BuiltIn) Verify(_ context.Context, req Request) error {
	namespace, taskQueue := req.Identity.Namespace, req.TaskQueue
	for _, cmd := range req.Commands {
		switch attr := cmd.GetAttributes().(type) {
		case *commandpb.Command_ScheduleActivityTaskCommandAttributes:
			a := attr.ScheduleActivityTaskCommandAttributes
			if tq := a.GetTaskQueue().GetName(); tq != taskQueue {
				return DeniedError{Reason: fmt.Sprintf("ScheduleActivityTask command targets task queue %q, not authorized queue %q", tq, taskQueue)}
			}
		case *commandpb.Command_StartChildWorkflowExecutionCommandAttributes:
			a := attr.StartChildWorkflowExecutionCommandAttributes
			if tq := a.GetTaskQueue().GetName(); tq != taskQueue {
				return DeniedError{Reason: fmt.Sprintf("StartChildWorkflowExecution command targets task queue %q, not authorized queue %q", tq, taskQueue)}
			}
			if ns := a.GetNamespace(); ns != "" && ns != namespace {
				return DeniedError{Reason: fmt.Sprintf("StartChildWorkflowExecution command targets namespace %q, not authorized namespace %q", ns, namespace)}
			}
		case *commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes:
			a := attr.ContinueAsNewWorkflowExecutionCommandAttributes
			if tq := a.GetTaskQueue().GetName(); tq != "" && tq != taskQueue {
				return DeniedError{Reason: fmt.Sprintf("ContinueAsNewWorkflowExecution command targets task queue %q, not authorized queue %q", tq, taskQueue)}
			}
		}
	}
	return nil
}
