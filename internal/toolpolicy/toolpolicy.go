// Package toolpolicy decides whether the work a RespondWorkflowTaskCompleted
// call sends outside the worker's own task queue may be forwarded.
//
// The proxy enforces the local rules itself (scope.ValidateCommands). Commands
// that leave the worker's task queue - Nexus operations, and activities on
// other task queues - are translated into tool calls (FromCommands) and judged
// by a Verifier: Default when no verifier is configured, or NexusVerifier,
// which delegates to an operator-provided Temporal Nexus service.
package toolpolicy

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/02strich/temporal-untrusted-workers/internal/auth"
	"github.com/02strich/temporal-untrusted-workers/internal/inputevents"
)

// Kind is the kind of command a tool call was translated from.
type Kind int

const (
	// KindNexusOperation is a ScheduleNexusOperation command.
	KindNexusOperation Kind = iota
	// KindActivity is a ScheduleActivityTask command targeting another task
	// queue.
	KindActivity
)

// Call is one tool call.
type Call struct {
	// Kind is not sent to the verifier; it lets Default keep the proxy's
	// historic behavior for each kind.
	Kind Kind
	// Name is "<endpoint>/<service>/<operation>" for Nexus operations and
	// "<taskQueue>/<activityType>" for activities.
	Name string
	// Parameters are the call's inputs in the protobuf JSON form of
	// google.protobuf.Any, one per input payload.
	Parameters []json.RawMessage
	// TargetTaskQueue is the task queue an activity call targets.
	TargetTaskQueue string
	// Err is set when an input could not be rendered as a parameter (e.g.
	// binary/protobuf of a type the proxy does not know). Verifiers that need
	// parameters must reject such calls.
	Err error
}

// Caller identifies the workflow run a workflow task belongs to and the
// worker identity processing it.
type Caller struct {
	// Identity is the authenticated worker.
	Identity auth.Identity
	// TaskQueue is the task queue that issued the workflow task token -
	// always one of Identity.TaskQueues.
	TaskQueue string
	// WorkflowID and RunID identify the workflow run.
	WorkflowID string
	RunID      string
}

// Request describes the tool calls of one RespondWorkflowTaskCompleted call.
type Request struct {
	Caller
	// Calls are the tool calls, in command order.
	Calls []Call
}

// InputRecorder records the context a workflow run receives (input events)
// with the tool verifier, before the worker sees it.
type InputRecorder interface {
	RecordInputEvents(ctx context.Context, caller Caller, batch inputevents.Batch) error
}

// Verifier decides whether a set of tool calls may be forwarded.
//
// Verify returns nil to allow the call, a DeniedError when the tool calls are
// not permitted, or an UnavailableError when no decision could be made (the
// proxy fails closed in that case). Any other error is treated as a denial.
type Verifier interface {
	Verify(ctx context.Context, req Request) error
}

// DeniedError reports that a Verifier rejected the tool calls.
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
	return fmt.Sprintf("tool verifier unavailable: %v", e.Err)
}

func (e UnavailableError) Unwrap() error {
	return e.Err
}

// Default is the Verifier used when no tool verifier is configured. It keeps
// the proxy's behavior from before tool calls existed: Nexus operations are
// allowed, and activities on other task queues are denied.
type Default struct{}

func (Default) Verify(_ context.Context, req Request) error {
	for _, call := range req.Calls {
		if call.Kind == KindActivity {
			return DeniedError{Reason: fmt.Sprintf("activity %q targets task queue %q, not authorized queue %q", call.Name, call.TargetTaskQueue, req.TaskQueue)}
		}
	}
	return nil
}
