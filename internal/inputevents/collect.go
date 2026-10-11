// Package inputevents extracts the context a workflow run receives - its
// input, activity and Nexus operation results, signals and updates - from the
// workflow-task poll responses the proxy relays, so it can be recorded with
// the tool verifier (RecordInputEvents) before the worker sees it.
//
// With sticky execution a poll response carries only the history events since
// the worker's previous task, so a small per-run RunState (kept in a
// StateStore) remembers how far the run has been recorded and the names of
// scheduled activities and Nexus operations whose results are still pending.
package inputevents

import (
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
)

// Kind is the kind of an input event.
type Kind int

const (
	KindWorkflowInput Kind = iota + 1
	KindActivityResult
	KindNexusOperationResult
	KindSignal
	KindUpdate
)

// FilterName is the kind's name in filter patterns.
func (k Kind) FilterName() string {
	switch k {
	case KindWorkflowInput:
		return "workflow"
	case KindActivityResult:
		return "activity"
	case KindNexusOperationResult:
		return "nexus"
	case KindSignal:
		return "signal"
	case KindUpdate:
		return "update"
	}
	return "unknown"
}

// ProtoName is the kind's toolpolicy.v1.InputEventKind enum value name.
func (k Kind) ProtoName() string {
	switch k {
	case KindWorkflowInput:
		return "INPUT_EVENT_KIND_WORKFLOW_INPUT"
	case KindActivityResult:
		return "INPUT_EVENT_KIND_ACTIVITY_RESULT"
	case KindNexusOperationResult:
		return "INPUT_EVENT_KIND_NEXUS_OPERATION_RESULT"
	case KindSignal:
		return "INPUT_EVENT_KIND_SIGNAL"
	case KindUpdate:
		return "INPUT_EVENT_KIND_UPDATE"
	}
	return "INPUT_EVENT_KIND_UNSPECIFIED"
}

// Event is one piece of context a workflow run received.
type Event struct {
	Kind Kind
	// Name is the workflow type, "<taskQueue>/<activityType>",
	// "<endpoint>/<service>/<operation>", or the signal / update name.
	Name     string
	Payloads []*commonpb.Payload
	// EventID is the history event ID; 0 for an update not yet in history.
	EventID  int64
	UpdateID string
}

// Batch is the input events a poll response adds for a run.
type Batch struct {
	Events []Event
	// FromHistoryStart is set when the batch starts at the run's first
	// history event.
	FromHistoryStart bool
}

// Empty reports whether there is nothing to record.
func (b Batch) Empty() bool {
	return len(b.Events) == 0 && !b.FromHistoryStart
}

// RunState is what the proxy remembers about a run between polls.
type RunState struct {
	// LastEventID is the highest history event ID already processed.
	LastEventID int64 `json:"last_event_id"`
	// Scheduled maps the event ID of each scheduled activity / Nexus
	// operation whose result hasn't arrived yet to its name.
	Scheduled map[int64]string `json:"scheduled,omitempty"`
	// PendingUpdates are update IDs already sent from a poll's messages but
	// not yet accepted into history.
	PendingUpdates []string `json:"pending_updates,omitempty"`
}

// maxPendingUpdates bounds PendingUpdates; updates rejected by the workflow
// never reach history, so their IDs would otherwise accumulate.
const maxPendingUpdates = 100

// unknownName names a result whose scheduled event the proxy never saw.
const unknownName = "?/?"

// Collect returns the input events resp adds to a run whose state is state,
// and the state after them. It doesn't modify state; the caller stores the
// returned state only once the batch has been recorded, so a failed recording
// is retried with the same events on the next poll.
func Collect(state RunState, resp *workflowservice.PollWorkflowTaskQueueResponse, filter Filter) (Batch, RunState) {
	next := RunState{
		LastEventID:    state.LastEventID,
		Scheduled:      make(map[int64]string, len(state.Scheduled)),
		PendingUpdates: append([]string(nil), state.PendingUpdates...),
	}
	for id, name := range state.Scheduled {
		next.Scheduled[id] = name
	}

	var batch Batch
	add := func(ev Event) {
		if filter.Allows(ev.Kind, ev.Name) {
			batch.Events = append(batch.Events, ev)
		}
	}

	for _, ev := range resp.GetHistory().GetEvents() {
		id := ev.GetEventId()
		if id <= next.LastEventID {
			continue
		}
		next.LastEventID = id
		collectHistoryEvent(ev, &next, &batch, add)
	}

	for _, msg := range resp.GetMessages() {
		req := &updatepb.Request{}
		if !msg.GetBody().MessageIs(req) || msg.GetBody().UnmarshalTo(req) != nil {
			continue
		}
		updateID := req.GetMeta().GetUpdateId()
		if updateID == "" || contains(next.PendingUpdates, updateID) {
			continue
		}
		next.PendingUpdates = append(next.PendingUpdates, updateID)
		if len(next.PendingUpdates) > maxPendingUpdates {
			next.PendingUpdates = next.PendingUpdates[len(next.PendingUpdates)-maxPendingUpdates:]
		}
		add(Event{Kind: KindUpdate, Name: req.GetInput().GetName(), Payloads: req.GetInput().GetArgs().GetPayloads(), UpdateID: updateID})
	}

	if len(next.Scheduled) == 0 {
		next.Scheduled = nil
	}
	if len(next.PendingUpdates) == 0 {
		next.PendingUpdates = nil
	}
	return batch, next
}

func collectHistoryEvent(ev *historypb.HistoryEvent, next *RunState, batch *Batch, add func(Event)) {
	id := ev.GetEventId()
	switch ev.GetEventType() {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:
		a := ev.GetWorkflowExecutionStartedEventAttributes()
		if id == 1 {
			batch.FromHistoryStart = true
		}
		add(Event{Kind: KindWorkflowInput, Name: a.GetWorkflowType().GetName(), Payloads: a.GetInput().GetPayloads(), EventID: id})

	case enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
		a := ev.GetActivityTaskScheduledEventAttributes()
		next.Scheduled[id] = a.GetTaskQueue().GetName() + "/" + a.GetActivityType().GetName()
	case enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED:
		a := ev.GetNexusOperationScheduledEventAttributes()
		next.Scheduled[id] = a.GetEndpoint() + "/" + a.GetService() + "/" + a.GetOperation()

	case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
		a := ev.GetActivityTaskCompletedEventAttributes()
		add(Event{Kind: KindActivityResult, Name: takeScheduled(next, a.GetScheduledEventId()), Payloads: a.GetResult().GetPayloads(), EventID: id})
	case enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED:
		a := ev.GetNexusOperationCompletedEventAttributes()
		var payloads []*commonpb.Payload
		if a.GetResult() != nil {
			payloads = []*commonpb.Payload{a.GetResult()}
		}
		add(Event{Kind: KindNexusOperationResult, Name: takeScheduled(next, a.GetScheduledEventId()), Payloads: payloads, EventID: id})

	// Terminal outcomes without a result: forget the scheduled name.
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED:
		delete(next.Scheduled, ev.GetActivityTaskFailedEventAttributes().GetScheduledEventId())
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
		delete(next.Scheduled, ev.GetActivityTaskTimedOutEventAttributes().GetScheduledEventId())
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED:
		delete(next.Scheduled, ev.GetActivityTaskCanceledEventAttributes().GetScheduledEventId())
	case enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED:
		delete(next.Scheduled, ev.GetNexusOperationFailedEventAttributes().GetScheduledEventId())
	case enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT:
		delete(next.Scheduled, ev.GetNexusOperationTimedOutEventAttributes().GetScheduledEventId())
	case enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED:
		delete(next.Scheduled, ev.GetNexusOperationCanceledEventAttributes().GetScheduledEventId())

	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
		a := ev.GetWorkflowExecutionSignaledEventAttributes()
		add(Event{Kind: KindSignal, Name: a.GetSignalName(), Payloads: a.GetInput().GetPayloads(), EventID: id})

	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED:
		req := ev.GetWorkflowExecutionUpdateAcceptedEventAttributes().GetAcceptedRequest()
		updateID := req.GetMeta().GetUpdateId()
		if i := index(next.PendingUpdates, updateID); i >= 0 {
			// Already sent when the request arrived as a message.
			next.PendingUpdates = append(next.PendingUpdates[:i], next.PendingUpdates[i+1:]...)
			return
		}
		add(Event{Kind: KindUpdate, Name: req.GetInput().GetName(), Payloads: req.GetInput().GetArgs().GetPayloads(), EventID: id, UpdateID: updateID})
	}
}

func takeScheduled(state *RunState, scheduledEventID int64) string {
	name, ok := state.Scheduled[scheduledEventID]
	if !ok {
		return unknownName
	}
	delete(state.Scheduled, scheduledEventID)
	return name
}

func contains(list []string, s string) bool { return index(list, s) >= 0 }

func index(list []string, s string) int {
	for i, v := range list {
		if v == s {
			return i
		}
	}
	return -1
}
