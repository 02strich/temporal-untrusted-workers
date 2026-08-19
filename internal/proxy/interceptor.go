package proxy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	enums "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/02strich/temporal-untrusted-workers/internal/actions"
	"github.com/02strich/temporal-untrusted-workers/internal/auth"
	"github.com/02strich/temporal-untrusted-workers/internal/rpcpolicy"
	"github.com/02strich/temporal-untrusted-workers/internal/scope"
	"github.com/02strich/temporal-untrusted-workers/internal/tokencache"
)

var errMissingCredentials = errors.New("missing or malformed authorization metadata")

type identityContextKey struct{}

// IdentityFromContext returns the authenticated Identity attached by the
// interceptor to a request's context, for use in logging/audit within
// forwarding methods.
func IdentityFromContext(ctx context.Context) (auth.Identity, bool) {
	id, ok := ctx.Value(identityContextKey{}).(auth.Identity)
	return id, ok
}

// NewInterceptor builds the single unary server interceptor that enforces
// the entire access-control policy: RPC allowlisting, downstream API-key
// authentication, and namespace/task-queue/token scoping. It must run
// before any handler - registering it via grpc.UnaryInterceptor on the
// server that hosts proxy.Server guarantees that.
func NewInterceptor(authenticator auth.Authenticator, cache *tokencache.Cache) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		rpcName := rpcNameFromFullMethod(info.FullMethod)

		policy, allowed := rpcpolicy.Allowed[rpcName]
		if !allowed {
			slog.Warn("access denied: rpc not permitted through proxy", "rpc", rpcName)
			return nil, status.Errorf(codes.PermissionDenied, "access denied: rpc %q is not permitted through this proxy", rpcName)
		}

		apiKey, err := extractAPIKey(ctx)
		if err != nil {
			slog.Warn("access denied: missing or malformed credentials", "rpc", rpcName)
			return nil, status.Error(codes.PermissionDenied, "access denied: missing or malformed credentials")
		}

		identity, err := authenticator.Authenticate(ctx, apiKey)
		if err != nil || !identity.Valid {
			slog.Warn("access denied: invalid credentials", "rpc", rpcName)
			return nil, status.Error(codes.PermissionDenied, "access denied: invalid credentials")
		}

		protoReq, ok := req.(proto.Message)
		if !ok {
			return nil, status.Error(codes.Internal, "proxy: request does not implement proto.Message")
		}

		if err := authorizeRequest(protoReq, rpcName, policy, identity, cache); err != nil {
			slog.Warn("access denied", "rpc", rpcName, "subject", identity.Subject, "reason", err.Error())
			return nil, status.Errorf(codes.PermissionDenied, "access denied: %s", err.Error())
		}

		ctx = context.WithValue(ctx, identityContextKey{}, identity)

		resp, err := handler(ctx, req)
		if err != nil {
			return resp, err
		}

		// Bookkeeping only runs on success, so an upstream failure never
		// mutates token-cache state.
		if policy.Terminal {
			if token, ok := scope.RequestTaskToken(protoReq); ok {
				cache.Delete(token)
			}
		}
		if protoResp, ok := resp.(proto.Message); ok {
			for _, token := range scope.CollectResponseTaskTokens(protoResp) {
				cache.Put(token, tokencache.Entry{Namespace: identity.Namespace, TaskQueue: identity.TaskQueue})
			}
		}

		// Log the billable Temporal Cloud actions this call consumed, one line
		// per command, attributed to the calling identity. Runs only on
		// success, so a call the upstream rejected records no action.
		for _, a := range actions.FromRequest(protoReq) {
			slog.Info("cloud action consumed",
				"action", a.Type,
				"actions", a.Count,
				"rpc", rpcName,
				"namespace", identity.Namespace,
				"task_queue", identity.TaskQueue,
				"subject", identity.Subject)
		}

		return resp, nil
	}
}

func rpcNameFromFullMethod(fullMethod string) string {
	if idx := strings.LastIndex(fullMethod, "/"); idx != -1 {
		return fullMethod[idx+1:]
	}
	return fullMethod
}

// extractAPIKey reads the API key from the same "authorization: Bearer
// <key>" metadata convention the real Temporal SDK's API-key credential
// uses, so stock SDK workers can point at the proxy without custom client
// code.
func extractAPIKey(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", errMissingCredentials
	}

	values := md.Get("authorization")
	if len(values) != 1 {
		return "", errMissingCredentials
	}

	const prefix = "Bearer "
	if !strings.HasPrefix(values[0], prefix) {
		return "", errMissingCredentials
	}

	key := strings.TrimPrefix(values[0], prefix)
	if key == "" {
		return "", errMissingCredentials
	}
	return key, nil
}

// authorizeRequest checks req against identity's authorized namespace/task
// queue, per policy.Category, and - for RespondWorkflowTaskCompleted -
// additionally validates that none of the request's commands target
// anywhere else.
func authorizeRequest(req proto.Message, rpcName string, policy rpcpolicy.Policy, identity auth.Identity, cache *tokencache.Cache) error {
	switch policy.Category {
	case rpcpolicy.CategoryPoll:
		ns, _ := scope.RequestNamespace(req)
		if ns != identity.Namespace {
			return fmt.Errorf("namespace %q not authorized for this identity", ns)
		}

		tq, ok := scope.RequestTaskQueue(req)
		if !ok || tq.GetName() == "" {
			return errors.New("missing task queue")
		}

		if tq.GetKind() == enums.TASK_QUEUE_KIND_STICKY {
			// The sticky queue Name is a random per-worker identifier, not
			// the configured queue - authorize by the real queue it
			// declares itself bound to instead (see scope.RequestTaskQueue).
			if tq.GetNormalName() != identity.TaskQueue {
				return fmt.Errorf("sticky task queue for normal queue %q not authorized for this identity", tq.GetNormalName())
			}
		} else if tq.GetName() != identity.TaskQueue {
			return fmt.Errorf("task queue %q not authorized for this identity", tq.GetName())
		}
		if r, ok := req.(*workflowservice.PollNexusTaskQueueRequest); ok {
			if err := scope.ValidateWorkerHeartbeatTaskQueues(r.GetWorkerHeartbeat(), identity.TaskQueue); err != nil {
				return err
			}
		}

	case rpcpolicy.CategoryToken:
		if ns, ok := scope.RequestNamespace(req); ok && ns != identity.Namespace {
			return fmt.Errorf("namespace %q not authorized for this identity", ns)
		}

		token, ok := scope.RequestTaskToken(req)
		if !ok || len(token) == 0 {
			return errors.New("missing task token")
		}

		entry, found := cache.Get(token)
		if !found {
			return errors.New("unknown or expired task token")
		}
		if entry.Namespace != identity.Namespace || entry.TaskQueue != identity.TaskQueue {
			return errors.New("task token not authorized for this identity")
		}

	case rpcpolicy.CategoryUnscoped:
		// A valid, recognized identity is sufficient.

	case rpcpolicy.CategoryNamespaceOnly:
		ns, _ := scope.RequestNamespace(req)
		if ns != identity.Namespace {
			return fmt.Errorf("namespace %q not authorized for this identity", ns)
		}

	case rpcpolicy.CategoryWorker:
		ns, _ := scope.RequestNamespace(req)
		if ns != identity.Namespace {
			return fmt.Errorf("namespace %q not authorized for this identity", ns)
		}
		// The normal task-queue name is optional here; when present it must
		// match the caller's queue so a worker can't cancel another queue's
		// outstanding polls within the namespace. The unguessable sticky queue
		// name (if any) needs no check, mirroring the sticky Poll case.
		if tq, ok := scope.RequestTaskQueueName(req); ok && tq != "" && tq != identity.TaskQueue {
			return fmt.Errorf("task queue %q not authorized for this identity", tq)
		}

	case rpcpolicy.CategoryWorkerHeartbeat:
		ns, _ := scope.RequestNamespace(req)
		if ns != identity.Namespace {
			return fmt.Errorf("namespace %q not authorized for this identity", ns)
		}
		r, ok := req.(*workflowservice.RecordWorkerHeartbeatRequest)
		if !ok {
			return errors.New("invalid worker heartbeat request")
		}
		if err := scope.ValidateWorkerHeartbeatTaskQueues(r.GetWorkerHeartbeat(), identity.TaskQueue); err != nil {
			return err
		}
	}

	if rpcName == "RespondWorkflowTaskCompleted" {
		if r, ok := req.(*workflowservice.RespondWorkflowTaskCompletedRequest); ok {
			if err := scope.ValidateCommands(r.GetCommands(), identity.Namespace, identity.TaskQueue); err != nil {
				return err
			}
		}
	}

	return nil
}
