package proxy

import (
	"context"
	"fmt"
	"log/slog"

	"go.temporal.io/api/workflowservice/v1"

	"github.com/02strich/temporal-untrusted-workers/internal/auth"
	"github.com/02strich/temporal-untrusted-workers/internal/inputevents"
	"github.com/02strich/temporal-untrusted-workers/internal/toolpolicy"
)

// Inputs configures recording a workflow's input events (its input, activity
// and Nexus operation results, signals and updates) with the tool verifier.
type Inputs struct {
	// Store keeps each run's progress between polls.
	Store inputevents.StateStore
	// Filter selects the events that are sent.
	Filter inputevents.Filter
	// Recorder delivers the events.
	Recorder toolpolicy.InputRecorder
}

// record records the input events a workflow task adds, before the task is
// handed to the worker. It returns an error if the run's state can't be read
// or the verifier doesn't acknowledge the events.
func (in *Inputs) record(ctx context.Context, identity auth.Identity, taskQueue string, task *workflowservice.PollWorkflowTaskQueueResponse) error {
	exec := task.GetWorkflowExecution()
	key := inputevents.RunKey{Namespace: identity.Namespace, WorkflowID: exec.GetWorkflowId(), RunID: exec.GetRunId()}

	state, _, err := in.Store.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("reading input state: %w", err)
	}
	batch, next := inputevents.Collect(state, task, in.Filter)
	if !batch.Empty() {
		caller := toolpolicy.Caller{Identity: identity, TaskQueue: taskQueue, WorkflowID: key.WorkflowID, RunID: key.RunID}
		if err := in.Recorder.RecordInputEvents(ctx, caller, batch); err != nil {
			return err
		}
	}
	// The events are recorded; failing to save the progress only means they
	// are sent again next time, which the verifier deduplicates.
	if err := in.Store.Put(ctx, key, next); err != nil {
		slog.Warn("saving input state failed; events will be re-sent", "workflow_id", key.WorkflowID, "run_id", key.RunID, "error", err)
	}
	return nil
}
