package proxy

import (
	"context"

	"go.temporal.io/api/workflowservice/v1"
)

// Server implements workflowservice.WorkflowServiceServer for exactly the
// RPCs listed in rpcpolicy.Allowed. Every method here is a thin forwarding
// call to the upstream client - all allowlisting, authentication, and
// namespace/task-queue/token scoping happens in the unary interceptor
// (interceptor.go) before any of these methods run. Every other RPC on
// WorkflowServiceServer is satisfied by the embedded
// UnimplementedWorkflowServiceServer, but the interceptor denies those with
// PermissionDenied before they would ever reach it.
type Server struct {
	workflowservice.UnimplementedWorkflowServiceServer

	Upstream workflowservice.WorkflowServiceClient
}

func (s *Server) PollWorkflowTaskQueue(ctx context.Context, req *workflowservice.PollWorkflowTaskQueueRequest) (*workflowservice.PollWorkflowTaskQueueResponse, error) {
	return s.Upstream.PollWorkflowTaskQueue(ctx, req)
}

func (s *Server) PollActivityTaskQueue(ctx context.Context, req *workflowservice.PollActivityTaskQueueRequest) (*workflowservice.PollActivityTaskQueueResponse, error) {
	return s.Upstream.PollActivityTaskQueue(ctx, req)
}

func (s *Server) RespondWorkflowTaskCompleted(ctx context.Context, req *workflowservice.RespondWorkflowTaskCompletedRequest) (*workflowservice.RespondWorkflowTaskCompletedResponse, error) {
	return s.Upstream.RespondWorkflowTaskCompleted(ctx, req)
}

func (s *Server) RespondWorkflowTaskFailed(ctx context.Context, req *workflowservice.RespondWorkflowTaskFailedRequest) (*workflowservice.RespondWorkflowTaskFailedResponse, error) {
	return s.Upstream.RespondWorkflowTaskFailed(ctx, req)
}

func (s *Server) RecordActivityTaskHeartbeat(ctx context.Context, req *workflowservice.RecordActivityTaskHeartbeatRequest) (*workflowservice.RecordActivityTaskHeartbeatResponse, error) {
	return s.Upstream.RecordActivityTaskHeartbeat(ctx, req)
}

func (s *Server) RespondActivityTaskCompleted(ctx context.Context, req *workflowservice.RespondActivityTaskCompletedRequest) (*workflowservice.RespondActivityTaskCompletedResponse, error) {
	return s.Upstream.RespondActivityTaskCompleted(ctx, req)
}

func (s *Server) RespondActivityTaskFailed(ctx context.Context, req *workflowservice.RespondActivityTaskFailedRequest) (*workflowservice.RespondActivityTaskFailedResponse, error) {
	return s.Upstream.RespondActivityTaskFailed(ctx, req)
}

func (s *Server) RespondActivityTaskCanceled(ctx context.Context, req *workflowservice.RespondActivityTaskCanceledRequest) (*workflowservice.RespondActivityTaskCanceledResponse, error) {
	return s.Upstream.RespondActivityTaskCanceled(ctx, req)
}

func (s *Server) RespondQueryTaskCompleted(ctx context.Context, req *workflowservice.RespondQueryTaskCompletedRequest) (*workflowservice.RespondQueryTaskCompletedResponse, error) {
	return s.Upstream.RespondQueryTaskCompleted(ctx, req)
}

func (s *Server) GetSystemInfo(ctx context.Context, req *workflowservice.GetSystemInfoRequest) (*workflowservice.GetSystemInfoResponse, error) {
	return s.Upstream.GetSystemInfo(ctx, req)
}

func (s *Server) DescribeNamespace(ctx context.Context, req *workflowservice.DescribeNamespaceRequest) (*workflowservice.DescribeNamespaceResponse, error) {
	return s.Upstream.DescribeNamespace(ctx, req)
}

func (s *Server) ShutdownWorker(ctx context.Context, req *workflowservice.ShutdownWorkerRequest) (*workflowservice.ShutdownWorkerResponse, error) {
	return s.Upstream.ShutdownWorker(ctx, req)
}
