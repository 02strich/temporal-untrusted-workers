# cedar-verifier

A command verifier for temporal-proxy (`TEMPORAL_PROXY_COMMAND_VERIFIER=nexus`) whose rules are
[Cedar](https://www.cedarpolicy.com) policies. It hosts the `VerifyCommands` Nexus operation
described in [`proto/commandpolicy/v1/commandpolicy.proto`](../../proto/commandpolicy/v1/commandpolicy.proto),
so changing what untrusted workers may do means editing a policy file, not writing code.

[`policies/builtin.cedar`](policies/builtin.cedar) reproduces the proxy's built-in policy
(`commandpolicy.BuiltIn`), so it's a drop-in starting point.

## How commands map to Cedar

Every command in a `RespondWorkflowTaskCompleted` call becomes one authorization request. The call
is allowed only if every command is allowed, and a call with no commands is always allowed. The
model is declared in [`policies/schema.cedarschema`](policies/schema.cedarschema):

| Cedar | Value |
|-------|-------|
| `principal` | `Worker::"<subject>"`, the calling identity, with `namespace` and `taskQueues` (every queue it is authorized for) |
| `action` | The command type, for example `Action::"ScheduleActivityTask"` or `Action::"StartChildWorkflowExecution"` |
| `resource` | `TaskQueue::"<queue>"`, the queue that issued the workflow task token, with `name` and `namespace` |
| `context.targetTaskQueue` | The command's `taskQueue.name` (activities, child workflows, continue-as-new) |
| `context.targetNamespace` | The command's `namespace` (child workflows, external signals and cancels) |
| `context.activityType` | The command's `activityType.name` |
| `context.workflowType` | The command's `workflowType.name` |

Context fields are always present and are `""` when the command doesn't have them. That keeps
policies simple, and it's how "unset means same queue/namespace" rules are written.

The action comes from the command's attributes field (`scheduleActivityTaskCommandAttributes` →
`ScheduleActivityTask`), the same field the proxy's built-in policy switches on. The verifier falls
back to `commandType` only if the command has no attributes field.

Cedar denies by default, so a policy set needs at least one `permit`. `builtin.cedar` permits
everything and then adds `forbid` rules. A denied call is reported to the worker with the `@id` of
the policy that denied it, for example
`ScheduleActivityTask denied by Cedar policy activities-stay-on-token-queue`.

### Example: only allow listed activity types

```cedar
@id("allowed-activity-types")
forbid (principal, action == Action::"ScheduleActivityTask", resource)
unless { ["Echo", "SendEmail"].contains(context.activityType) };
```

### Adding context fields

Raw command attributes are deliberately not passed to Cedar. Protobuf JSON contains values Cedar
can't represent, such as doubles in retry policies. To expose another field:
1. Extract it in `CommandFacts::extract` and add it to `CommandFacts::context` in
   [`src/policy.rs`](src/policy.rs).
2. Declare it in `CommandContext` in the schema.

## Running

```bash
temporal operator nexus endpoint create --name command-verifier \
  --target-namespace default --target-task-queue command-verifier
CEDAR_SCHEMA_FILE=policies/schema.cedarschema cargo run --release
```

Then point the proxy at it with `TEMPORAL_PROXY_COMMAND_VERIFIER=nexus` and
`TEMPORAL_PROXY_COMMAND_VERIFIER_NEXUS_{NAMESPACE,ENDPOINT,SERVICE}`, as described in the
[root README](../../README.md#custom-command-verification).

| Variable | Default | Description |
|----------|---------|-------------|
| `TEMPORAL_ADDRESS` | `http://127.0.0.1:7233` | Temporal frontend URL (`https://...` for Temporal Cloud). |
| `TEMPORAL_NAMESPACE` | `default` | Namespace the verifier's Nexus endpoint targets. |
| `TEMPORAL_API_KEY` | — | Optional API key; enables TLS. |
| `VERIFIER_TASK_QUEUE` | `command-verifier` | Task queue the endpoint targets. |
| `VERIFIER_SERVICE` | `command-policy` | Nexus service name. |
| `VERIFIER_OPERATION` | `VerifyCommands` | Nexus operation name. |
| `CEDAR_POLICY_FILE` | `policies/builtin.cedar` | Policies to enforce. |
| `CEDAR_SCHEMA_FILE` | — | If set, the policies are validated against this schema at startup, and the verifier refuses to start on errors. Recommended. |
| `RUST_LOG` | `info` | Log filter. |

The verifier fails closed in the proxy's favor:
- Input it can't interpret is answered with a Nexus `BAD_REQUEST` handler error, which the proxy
  treats as "verifier unavailable".
- On Ctrl-C or SIGTERM it drains gracefully for up to 10 seconds and then exits. Any verification
  left unanswered is retried by the server.

## Implementation notes

The Temporal Rust SDK (1.0) can't host Nexus handlers yet; its `experimental` feature only adds
calling Nexus operations from workflows. This crate therefore runs a Nexus-only worker directly on
`temporalio-sdk-core`, which takes care of polling, worker heartbeats and shutdown. Temporal's
protos are compiled with the `vendored-protox` feature, so no system `protoc` is needed.

Run the tests with `cargo test` (or `make test-cedar-verifier` from the repo root). They check that
`builtin.cedar` matches the proxy's built-in policy case by case and validates against the schema.
