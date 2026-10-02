// Equivalent of temporal-proxy's built-in command policy
// (commandpolicy.BuiltIn): commands may not direct work outside the task
// queue that issued the workflow task token, or outside the identity's
// namespace. Command types that carry no such targeting are allowed.

@id("allow-commands")
permit (principal, action, resource);

// Activities must be scheduled on the token's task queue. Activities always run
// in the workflow's own namespace, so there is nothing else to check.
@id("activities-stay-on-token-queue")
forbid (principal, action == Action::"ScheduleActivityTask", resource)
unless { context.targetTaskQueue == resource.name };

// Child workflows must run on the token's task queue, in the identity's
// namespace. An empty namespace means "same as the parent".
@id("child-workflows-stay-on-token-queue-and-namespace")
forbid (principal, action == Action::"StartChildWorkflowExecution", resource)
unless {
  context.targetTaskQueue == resource.name &&
  (context.targetNamespace == "" || context.targetNamespace == principal.namespace)
};

// Continue-as-new must stay on the token's task queue. An empty task queue
// means "same queue".
@id("continue-as-new-stays-on-token-queue")
forbid (principal, action == Action::"ContinueAsNewWorkflowExecution", resource)
unless { context.targetTaskQueue == "" || context.targetTaskQueue == resource.name };
