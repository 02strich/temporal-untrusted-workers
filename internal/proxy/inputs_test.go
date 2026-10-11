package proxy

import (
	"context"
	"errors"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/02strich/temporal-untrusted-workers/internal/auth"
	"github.com/02strich/temporal-untrusted-workers/internal/inputevents"
	"github.com/02strich/temporal-untrusted-workers/internal/tokencache"
	"github.com/02strich/temporal-untrusted-workers/internal/toolpolicy"
)

type fakeRecorder struct {
	err     error
	callers []toolpolicy.Caller
	batches []inputevents.Batch
}

func (f *fakeRecorder) RecordInputEvents(_ context.Context, caller toolpolicy.Caller, batch inputevents.Batch) error {
	f.callers = append(f.callers, caller)
	f.batches = append(f.batches, batch)
	return f.err
}

type failingStateStore struct{ inputevents.StateStore }

func (failingStateStore) Get(context.Context, inputevents.RunKey) (inputevents.RunState, bool, error) {
	return inputevents.RunState{}, false, errors.New("valkey down")
}

func workflowTask(token string, events ...*historypb.HistoryEvent) *workflowservice.PollWorkflowTaskQueueResponse {
	return &workflowservice.PollWorkflowTaskQueueResponse{
		TaskToken:         []byte(token),
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "wf-1", RunId: "run-1"},
		History:           &historypb.History{Events: events},
	}
}

func startedEvent() *historypb.HistoryEvent {
	return &historypb.HistoryEvent{EventId: 1, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
			WorkflowType: &commonpb.WorkflowType{Name: "AgentWorkflow"},
		}}}
}

func signalEvent(id int64) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{EventId: id, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{SignalName: "msg"}}}
}

func inputsInterceptorSetup(t *testing.T, inputs *Inputs) (func(resp *workflowservice.PollWorkflowTaskQueueResponse) (any, error), func(resp *workflowservice.RespondWorkflowTaskCompletedResponse) (any, error), tokencache.Store) {
	t.Helper()
	authr := &fakeAuthenticator{identities: map[string]auth.Identity{
		"key-a": {Valid: true, Namespace: "ns", TaskQueues: []string{"queue-a"}, Subject: "fleet-a"},
	}}
	cache := tokencache.New(time.Hour, 1000)
	t.Cleanup(func() { cache.Close() })
	interceptor := NewInterceptor(authr, cache, nil, inputs)

	poll := func(resp *workflowservice.PollWorkflowTaskQueueResponse) (any, error) {
		got, err, _ := callInterceptor(t, interceptor, ctxWithBearer("key-a"),
			"/temporal.api.workflowservice.v1.WorkflowService/PollWorkflowTaskQueue",
			&workflowservice.PollWorkflowTaskQueueRequest{Namespace: "ns", TaskQueue: &taskqueuepb.TaskQueue{Name: "queue-a"}},
			resp, nil)
		return got, err
	}
	complete := func(resp *workflowservice.RespondWorkflowTaskCompletedResponse) (any, error) {
		got, err, _ := callInterceptor(t, interceptor, ctxWithBearer("key-a"),
			"/temporal.api.workflowservice.v1.WorkflowService/RespondWorkflowTaskCompleted",
			&workflowservice.RespondWorkflowTaskCompletedRequest{Namespace: "ns", TaskToken: []byte("wt-1")},
			resp, nil)
		return got, err
	}
	return poll, complete, cache
}

func newInputs(recorder *fakeRecorder) *Inputs {
	return &Inputs{Store: inputevents.NewLocalStore(time.Hour, 100), Filter: inputevents.AllowAll, Recorder: recorder}
}

func TestInputs_RecordedBeforeRelayingPoll(t *testing.T) {
	recorder := &fakeRecorder{}
	poll, _, cache := inputsInterceptorSetup(t, newInputs(recorder))

	if _, err := poll(workflowTask("wt-1", startedEvent(), signalEvent(3))); err != nil {
		t.Fatalf("poll: %v", err)
	}
	if len(recorder.batches) != 1 {
		t.Fatalf("expected one recording, got %d", len(recorder.batches))
	}
	b := recorder.batches[0]
	if !b.FromHistoryStart || len(b.Events) != 2 {
		t.Fatalf("unexpected batch %+v", b)
	}
	c := recorder.callers[0]
	if c.Identity.Subject != "fleet-a" || c.TaskQueue != "queue-a" || c.WorkflowID != "wf-1" || c.RunID != "run-1" {
		t.Fatalf("unexpected caller %+v", c)
	}
	entry, ok, _ := cache.Get(context.Background(), []byte("wt-1"))
	if !ok || entry.RunID != "run-1" {
		t.Fatalf("token must be registered with its run, got (%+v, %v)", entry, ok)
	}

	// A later sticky poll only sends the new events; one with none sends nothing.
	if _, err := poll(workflowTask("wt-2", signalEvent(3), signalEvent(6))); err != nil {
		t.Fatalf("poll: %v", err)
	}
	if len(recorder.batches) != 2 || len(recorder.batches[1].Events) != 1 || recorder.batches[1].Events[0].EventID != 6 {
		t.Fatalf("expected only the new signal, got %+v", recorder.batches)
	}
	if _, err := poll(workflowTask("wt-3", signalEvent(6))); err != nil {
		t.Fatalf("poll: %v", err)
	}
	if len(recorder.batches) != 2 {
		t.Fatalf("a poll without new events must not record")
	}
}

func TestInputs_RecordingFailureWithholdsTask(t *testing.T) {
	recorder := &fakeRecorder{err: toolpolicy.UnavailableError{Err: errors.New("verifier down")}}
	poll, _, cache := inputsInterceptorSetup(t, newInputs(recorder))

	_, err := poll(workflowTask("wt-1", startedEvent()))
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable, got %v", err)
	}
	if _, ok, _ := cache.Get(context.Background(), []byte("wt-1")); ok {
		t.Fatalf("a withheld task's token must not be registered")
	}

	// The state wasn't advanced, so the next attempt sends the same events.
	recorder.err = nil
	if _, err := poll(workflowTask("wt-2", startedEvent())); err != nil {
		t.Fatalf("poll: %v", err)
	}
	if last := recorder.batches[len(recorder.batches)-1]; !last.FromHistoryStart || len(last.Events) != 1 {
		t.Fatalf("expected the events to be re-sent, got %+v", last)
	}
}

func TestInputs_StateStoreFailureWithholdsTask(t *testing.T) {
	recorder := &fakeRecorder{}
	poll, _, _ := inputsInterceptorSetup(t, &Inputs{Store: failingStateStore{}, Filter: inputevents.AllowAll, Recorder: recorder})
	if _, err := poll(workflowTask("wt-1", startedEvent())); status.Code(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable, got %v", err)
	}
	if len(recorder.batches) != 0 {
		t.Fatalf("nothing must be recorded without state")
	}
}

func TestInputs_EagerTaskDroppedOnFailure(t *testing.T) {
	recorder := &fakeRecorder{}
	poll, complete, cache := inputsInterceptorSetup(t, newInputs(recorder))
	if _, err := poll(workflowTask("wt-1", startedEvent())); err != nil {
		t.Fatalf("poll: %v", err)
	}

	recorder.err = errors.New("verifier down")
	resp := &workflowservice.RespondWorkflowTaskCompletedResponse{WorkflowTask: workflowTask("wt-eager", signalEvent(5))}
	got, err := complete(resp)
	if err != nil {
		t.Fatalf("the completion itself must succeed, got %v", err)
	}
	if got.(*workflowservice.RespondWorkflowTaskCompletedResponse).GetWorkflowTask() != nil {
		t.Fatalf("the eager task must be dropped from the response")
	}
	if _, ok, _ := cache.Get(context.Background(), []byte("wt-eager")); ok {
		t.Fatalf("a dropped eager task's token must not be registered")
	}

	// Recorded successfully, the eager task is relayed. (The completion above
	// consumed wt-1, so poll it again first.)
	recorder.err = nil
	if _, err := poll(workflowTask("wt-1", startedEvent())); err != nil {
		t.Fatalf("poll: %v", err)
	}
	resp = &workflowservice.RespondWorkflowTaskCompletedResponse{WorkflowTask: workflowTask("wt-eager-2", signalEvent(5))}
	got, err = complete(resp)
	if err != nil || got.(*workflowservice.RespondWorkflowTaskCompletedResponse).GetWorkflowTask() == nil {
		t.Fatalf("expected the eager task to be relayed, err=%v", err)
	}
}

func TestInputs_DisabledWithoutConfig(t *testing.T) {
	poll, _, _ := inputsInterceptorSetup(t, nil)
	if _, err := poll(workflowTask("wt-1", startedEvent())); err != nil {
		t.Fatalf("poll: %v", err)
	}
}
