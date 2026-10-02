// Package scope extracts the namespace/task-queue/task-token scoping
// information from the WorkflowService RPCs the proxy allows. Validation of
// the commands a RespondWorkflowTaskCompleted request emits lives in package
// commandpolicy.
//
// Extraction is done via explicit per-RPC type switches over the concrete
// go.temporal.io/api generated structs rather than reflection: it is a
// little more code to maintain as RPCs are added to the allowlist, but a
// missing case is a compile-time-visible gap (the switch's default branch)
// rather than a silently-skipped field.
package scope

import (
	"fmt"

	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/proto"
)

// RequestNamespace returns the namespace carried by a request of one of the
// allowed RPCs, if that RPC's request carries one.
func RequestNamespace(req proto.Message) (string, bool) {
	switch r := req.(type) {
	case *workflowservice.PollWorkflowTaskQueueRequest:
		return r.GetNamespace(), true
	case *workflowservice.PollActivityTaskQueueRequest:
		return r.GetNamespace(), true
	case *workflowservice.PollNexusTaskQueueRequest:
		return r.GetNamespace(), true
	case *workflowservice.RespondWorkflowTaskCompletedRequest:
		return r.GetNamespace(), true
	case *workflowservice.RespondWorkflowTaskFailedRequest:
		return r.GetNamespace(), true
	case *workflowservice.RecordActivityTaskHeartbeatRequest:
		return r.GetNamespace(), true
	case *workflowservice.RespondActivityTaskCompletedRequest:
		return r.GetNamespace(), true
	case *workflowservice.RespondActivityTaskFailedRequest:
		return r.GetNamespace(), true
	case *workflowservice.RespondActivityTaskCanceledRequest:
		return r.GetNamespace(), true
	case *workflowservice.RespondQueryTaskCompletedRequest:
		return r.GetNamespace(), true
	case *workflowservice.RespondNexusTaskCompletedRequest:
		return r.GetNamespace(), true
	case *workflowservice.RespondNexusTaskFailedRequest:
		return r.GetNamespace(), true
	case *workflowservice.DescribeNamespaceRequest:
		return r.GetNamespace(), true
	case *workflowservice.ShutdownWorkerRequest:
		return r.GetNamespace(), true
	case *workflowservice.RecordWorkerHeartbeatRequest:
		return r.GetNamespace(), true
	default:
		return "", false
	}
}

// RequestTaskQueueName returns the task queue name carried by a request of
// the workflow/activity Poll RPCs or of ShutdownWorker. ShutdownWorker
// carries the normal queue name directly as a string (not a TaskQueue
// message), and that name may legitimately be empty - the second return value
// reports only that the RPC has such a field, not that it is set.
func RequestTaskQueueName(req proto.Message) (string, bool) {
	switch r := req.(type) {
	case *workflowservice.PollWorkflowTaskQueueRequest:
		return r.GetTaskQueue().GetName(), true
	case *workflowservice.PollActivityTaskQueueRequest:
		return r.GetTaskQueue().GetName(), true
	case *workflowservice.ShutdownWorkerRequest:
		return r.GetTaskQueue(), true
	default:
		return "", false
	}
}

// RequestTaskQueue returns the full TaskQueue message (name, kind, and - for
// sticky queues - the real "normal" queue name it belongs to) carried by a
// request of one of the Poll RPCs.
//
// A worker with sticky execution enabled (the SDK default) polls not only
// its configured task queue but also a per-process sticky queue, whose Name
// is a random, per-worker-instance identifier (e.g. "host:uuid") rather
// than the configured queue name - the server only ever routes a workflow
// task to that exact sticky name once that name was legitimately
// registered via a prior, properly-authorized RespondWorkflowTaskCompleted
// call for that specific workflow execution, so its unguessability is what
// actually keeps it safe to poll (the same trust assumption the proxy
// already relies on for opaque task tokens). Kind == STICKY requests
// therefore need to be authorized by their self-declared NormalName rather
// than by Name.
func RequestTaskQueue(req proto.Message) (*taskqueuepb.TaskQueue, bool) {
	switch r := req.(type) {
	case *workflowservice.PollWorkflowTaskQueueRequest:
		return r.GetTaskQueue(), true
	case *workflowservice.PollActivityTaskQueueRequest:
		return r.GetTaskQueue(), true
	case *workflowservice.PollNexusTaskQueueRequest:
		return r.GetTaskQueue(), true
	default:
		return nil, false
	}
}

// RequestTaskToken returns the opaque task token carried by a request of one
// of the token-scoped RPCs.
func RequestTaskToken(req proto.Message) ([]byte, bool) {
	switch r := req.(type) {
	case *workflowservice.RespondWorkflowTaskCompletedRequest:
		return r.GetTaskToken(), true
	case *workflowservice.RespondWorkflowTaskFailedRequest:
		return r.GetTaskToken(), true
	case *workflowservice.RecordActivityTaskHeartbeatRequest:
		return r.GetTaskToken(), true
	case *workflowservice.RespondActivityTaskCompletedRequest:
		return r.GetTaskToken(), true
	case *workflowservice.RespondActivityTaskFailedRequest:
		return r.GetTaskToken(), true
	case *workflowservice.RespondActivityTaskCanceledRequest:
		return r.GetTaskToken(), true
	case *workflowservice.RespondQueryTaskCompletedRequest:
		return r.GetTaskToken(), true
	case *workflowservice.RespondNexusTaskCompletedRequest:
		return r.GetTaskToken(), true
	case *workflowservice.RespondNexusTaskFailedRequest:
		return r.GetTaskToken(), true
	default:
		return nil, false
	}
}

// CollectResponseTaskTokens returns every task token embedded in a response
// of an allowed RPC: the direct token on Poll responses, and - for
// RespondWorkflowTaskCompletedResponse specifically - the new tokens Temporal
// embeds for eager/sticky dispatch (a new workflow task, and/or eagerly
// dispatched activity tasks). Every token returned here should be registered
// in the token cache under the calling identity, or a legitimate follow-up
// Respond/Heartbeat call presenting one of these tokens would be wrongly
// denied as unknown.
func CollectResponseTaskTokens(resp proto.Message) [][]byte {
	switch r := resp.(type) {
	case *workflowservice.PollWorkflowTaskQueueResponse:
		if tok := r.GetTaskToken(); len(tok) > 0 {
			return [][]byte{tok}
		}
	case *workflowservice.PollActivityTaskQueueResponse:
		if tok := r.GetTaskToken(); len(tok) > 0 {
			return [][]byte{tok}
		}
	case *workflowservice.PollNexusTaskQueueResponse:
		if tok := r.GetTaskToken(); len(tok) > 0 {
			return [][]byte{tok}
		}
	case *workflowservice.RespondWorkflowTaskCompletedResponse:
		var tokens [][]byte
		if wt := r.GetWorkflowTask(); wt != nil {
			if tok := wt.GetTaskToken(); len(tok) > 0 {
				tokens = append(tokens, tok)
			}
		}
		for _, at := range r.GetActivityTasks() {
			if tok := at.GetTaskToken(); len(tok) > 0 {
				tokens = append(tokens, tok)
			}
		}
		return tokens
	}
	return nil
}

// ValidateWorkerHeartbeatTaskQueues checks every heartbeat entry against the
// caller's authorized task queues.
func ValidateWorkerHeartbeatTaskQueues(heartbeats []*workerpb.WorkerHeartbeat, taskQueues []string) error {
	for i, hb := range heartbeats {
		tq := hb.GetTaskQueue()
		if tq == "" {
			return fmt.Errorf("worker_heartbeat[%d] missing task queue", i)
		}
		if !containsTaskQueue(taskQueues, tq) {
			return fmt.Errorf("worker_heartbeat[%d] targets task queue %q, not authorized", i, tq)
		}
	}
	return nil
}

func containsTaskQueue(taskQueues []string, taskQueue string) bool {
	for _, allowed := range taskQueues {
		if allowed == taskQueue {
			return true
		}
	}
	return false
}
