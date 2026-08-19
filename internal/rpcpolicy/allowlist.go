// Package rpcpolicy defines the set of WorkflowService RPCs that untrusted
// workers are permitted to call through the proxy, and how each RPC's
// scoping fields should be interpreted.
package rpcpolicy

// Category describes how an RPC's request carries the information needed to
// scope it to a single namespace/task queue.
type Category int

const (
	// CategoryPoll requests carry an explicit namespace and task_queue.name.
	CategoryPoll Category = iota
	// CategoryToken requests carry a namespace and an opaque task_token that
	// must be resolved against previously observed poll responses.
	CategoryToken
	// CategoryUnscoped requests carry no scoping fields; a valid identity is
	// sufficient.
	CategoryUnscoped
	// CategoryNamespaceOnly requests carry an explicit namespace but no task
	// queue or task token.
	CategoryNamespaceOnly
	// CategoryWorker requests carry an explicit namespace and the normal task
	// queue name as a plain string (which may be empty). When the name is set
	// it must match the caller's authorized queue; any sticky queue name they
	// also carry is a per-worker unguessable identifier and is not checked.
	CategoryWorker
	// CategoryWorkerHeartbeat requests carry an explicit namespace and a batch
	// of WorkerHeartbeat entries, each with its own normal task queue name.
	CategoryWorkerHeartbeat
)

// Policy describes how the proxy should treat one allowed RPC.
type Policy struct {
	Category Category
	// Terminal indicates a successful call ends the task token's lifecycle,
	// so the token should be evicted from the token cache.
	Terminal bool
}

// Allowed is the fixed set of WorkflowService RPCs (by unqualified method
// name) the proxy will forward to the upstream server. Any RPC not present
// here is rejected with PermissionDenied before a handler ever runs.
//
// RespondWorkflowTaskCompleted additionally has its emitted Commands
// validated against the caller's namespace/task queue (see
// internal/scope.ValidateCommands) - that check is RPC-specific rather than
// a generic Category and so is not represented in this table.
//
// The *ById activity-completion RPCs (RespondActivityTaskCompletedById,
// RespondActivityTaskFailedById, RespondActivityTaskCanceledById) and
// RecordActivityTaskHeartbeatById are intentionally excluded: they identify
// the target activity by namespace/workflow_id/run_id/activity_id instead of
// an opaque task token, which is a different trust model than the
// token-scoping this proxy relies on. They fall through to PermissionDenied.
var Allowed = map[string]Policy{
	"PollWorkflowTaskQueue":        {Category: CategoryPoll},
	"PollActivityTaskQueue":        {Category: CategoryPoll},
	"PollNexusTaskQueue":           {Category: CategoryPoll},
	"RespondWorkflowTaskCompleted": {Category: CategoryToken, Terminal: true},
	"RespondWorkflowTaskFailed":    {Category: CategoryToken, Terminal: true},
	"RecordActivityTaskHeartbeat":  {Category: CategoryToken, Terminal: false},
	"RespondActivityTaskCompleted": {Category: CategoryToken, Terminal: true},
	"RespondActivityTaskFailed":    {Category: CategoryToken, Terminal: true},
	"RespondActivityTaskCanceled":  {Category: CategoryToken, Terminal: true},
	"RespondNexusTaskCompleted":    {Category: CategoryToken, Terminal: true},
	"RespondNexusTaskFailed":       {Category: CategoryToken, Terminal: true},
	// RespondQueryTaskCompleted reuses the same task token issued in the
	// originating PollWorkflowTaskQueueResponse for the workflow task that
	// carried the legacy query, so it must not evict the token - a later
	// RespondWorkflowTaskCompleted for that same workflow task still needs it.
	"RespondQueryTaskCompleted": {Category: CategoryToken, Terminal: false},
	"GetSystemInfo":             {Category: CategoryUnscoped},
	// DescribeNamespace is called by the standard Temporal SDK client during
	// worker startup to fetch namespace capabilities (see
	// DescribeNamespaceRequest.weak_consistency's doc comment in
	// go.temporal.io/api) - without allowing it, a stock SDK worker cannot
	// even start. It is read-only and carries no task queue, so it is scoped
	// to the caller's namespace only.
	"DescribeNamespace": {Category: CategoryNamespaceOnly},
	// ShutdownWorker is sent by an SDK worker as it stops, so the server can
	// cancel that worker's outstanding poll RPCs and update its heartbeat
	// state instead of waiting for timeouts. It carries the caller's namespace
	// and (optionally) the normal task queue it polls; both are scoped to the
	// caller's identity so a worker can't cancel another queue's polls. It has
	// no task token, so it is not terminal.
	"ShutdownWorker": {Category: CategoryWorker},
	// RecordWorkerHeartbeat updates server-side worker liveness/telemetry. It
	// carries no task token, so every reported heartbeat entry is scoped by its
	// explicit task queue instead.
	"RecordWorkerHeartbeat": {Category: CategoryWorkerHeartbeat},
}
