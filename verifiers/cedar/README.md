# cedar-verifier

A tool-call verifier for temporal-proxy (`TEMPORAL_PROXY_TOOL_VERIFIER=nexus`) whose rules are
[Cedar](https://www.cedarpolicy.com) policies. It hosts the `VerifyToolCalls` Nexus operation
described in [`proto/toolpolicy/v1/toolpolicy.proto`](../../proto/toolpolicy/v1/toolpolicy.proto).

The proxy keeps workflows on their own task queue by itself. What reaches this verifier is the work
that leaves it, as tool calls:
- Nexus operations, named `<endpoint>/<service>/<operation>`;
- activities on other task queues, named `<taskQueue>/<activityType>`.

Deciding which of those untrusted workflows may make, and with which parameters, means editing a
policy file, not writing code.

## How tool calls map to Cedar

Every tool call becomes one authorization request. A workflow task is approved only if all of its
tool calls are allowed.

| Cedar | Value |
|-------|-------|
| `principal` | `Worker::"<subject>"`, the worker identity, with `namespace` |
| `action` | The tool: `Action::"payments/billing/Charge"`, `Action::"trusted-tools/SendEmail"`, … |
| `resource` | `TaskQueue::"<queue>"`, the calling workflow's task queue, with `name` and `namespace` |
| `context.workflowId` | The calling workflow's ID |
| `context.parameterCount` | How many parameters the call has |
| `context.parameters.argN` | Parameter N (`arg0`, `arg1`, …) as `{ type, data }` |
| `context.workflowInput` | The workflow's start input: `{ name, parameters }` (see below) |
| `context.events.<type>["<name>"]` | The latest activity result, Nexus result, signal or update of that name: `{ type, name, count, parameters }` |
| `context.inputEventsComplete` | Whether the verifier has the run's input events from its start |

Each tool is its own action, so [`policies/schema.cedarschema`](policies/schema.cedarschema)
declares the shape of its parameters. Cedar denies by default, so a tool no policy permits,
including any tool the schema doesn't declare, is rejected. A denial is reported to the worker with
the `@id` of the deciding policy, for example
`tool payments/billing/Charge denied by Cedar policy charge-limit-10000`.

### Escalating to a human

Cedar only allows or denies. To ask for human review instead, annotate a `forbid` policy with
`@decision("escalate")`. When every policy that denied a call carries the annotation, the verifier
answers `DECISION_ESCALATE` rather than `DECISION_DENIED`, and the proxy rejects the task with a
`pending human review: …` reason. A plain `forbid` that also applies wins, and so does any other
call in the same task that is denied outright. Approving escalated calls, for example by remembering
an approval and answering `DECISION_APPROVED` when the worker retries, is up to you.

### Parameters

The proxy sends each parameter as a `google.protobuf.Any` in protobuf JSON form. `argN.type` is its
type name, for example `google.protobuf.Value` or `acme.billing.v1.ChargeRequest`. `argN.data` holds
its content:

| Parameter type | `data` |
|----------------|--------|
| `google.protobuf.Value`, which is how plain JSON inputs (the SDK default) arrive | the JSON object's fields; any other value is under `value` (`data.value`) |
| other well-known types (`Struct`, `Timestamp`, wrappers, …) | the same, from the Any's `value` |
| a message type (`json/protobuf` inputs, Temporal API types) | the message's fields in protobuf JSON |
| `temporal.api.common.v1.Payload` (inputs the proxy can't interpret, for example encrypted ones) | `{}` |

JSON values become Cedar values as follows:
- strings, booleans, integers and objects map directly;
- arrays become sets;
- floats and nulls are dropped, because Cedar has no equivalent.

In protobuf JSON, 64-bit integer fields of message types are strings (`"amount": "500"`), so declare
them as `String` or use 32-bit fields.

### Input events

The proxy records each run's input events (the workflow input, activity and Nexus results, signals,
updates) with the verifier's `RecordInputEvents` operation, before the worker sees them. The
verifier keeps them per run, deduplicated by event ID or update ID, and exposes them as context:
- `context.workflowInput` is the workflow's start input: `{ name, parameters }`, where `name` is the
  workflow type.
- `context.events` holds everything the run received after it started, by type: `activity`,
  `nexus`, `signal` and `update`, each a record keyed by name. An entry is
  `{ type, name, count, parameters }` for the **latest** event of that type and name, and `count`
  says how many there were.

Every entry's `parameters` has the same structure as the tool call's own parameters,
`{ arg0: { type, data }, … }`, with the payload's full content in `data`. So a policy can compare a
tool call's parameter with a specific field of a specific input:

```cedar
// Only email the customer the workflow was started for.
context.parameters.arg0.data.value == context.workflowInput.parameters.arg0.data.customerEmail

// Only refund what the latest `approval` signal named. Signals are sent from outside the
// workflow, so this is a human's approval, not something the workflow decided.
context.events.signal has approval &&
context.parameters.arg0.data.value == context.events.signal.approval.parameters.arg0.data.refundId

// Activity and Nexus names contain "/", so quote them.
context.events.activity has "crm/LookupCustomer" &&
context.parameters.arg0.data.value == context.events.activity["crm/LookupCustomer"].parameters.arg0.data.email
```

Cedar can only read declared fields, so declare the workflow input, and each event name your
policies use with its parameters, in the schema (`WorkflowInput` and `Events` in
[`schema.cedarschema`](policies/schema.cedarschema)). Mark the names and fields optional and check
them with `has`, so a missing event or field fails closed instead of erroring.

- `context.inputEventsComplete` is true once the verifier has received the run's events from its
  first history event. Policies that rely on input events should require it, so a verifier restart
  fails closed instead of trusting partial context.

The store is in memory and bounded (`VERIFIER_INPUT_MAX_RUNS`), so a restart loses it. Run a single
verifier instance: with several, each sees only the events delivered to it.

## The example policy

[`policies/example.cedar`](policies/example.cedar) allows:
- the Nexus operation `echo/echo/Echo`, for everyone;
- the Nexus operation `payments/billing/Charge`, in USD or EUR:
  - up to 1000 it goes through;
  - above 1000 it is escalated for review (`charge-over-1000-needs-review`, a `forbid` annotated
    `@decision("escalate")`);
  - above 10000, or without an amount, it is denied (`charge-limit-10000`);
- the activity `trusted-tools/IssueRefund`, only for a refund an `approval` signal named
  (`refund-needs-approval`);
- the activity `trusted-tools/SendEmail`, only to the customer the workflow was started for
  (`send-email-to-customer`: the recipient must equal `workflowInput`'s `customerEmail`, and the
  context must be complete).

## Declaring a tool

1. Add an action for the tool to the schema, with the shape of its parameters. Mark the fields
   optional (`?`), so policies have to check them with `has`. A parameter that's missing then fails
   closed instead of erroring.

   ```cedar
   type RefundArguments = { arg0?: { type: String, data: { orderId?: String, amount?: Long } } };

   action "payments/billing/Refund" appliesTo {
     principal: Worker,
     resource: TaskQueue,
     context: { workflowId: String, parameterCount: Long, parameters: RefundArguments },
   };
   ```

2. Permit it:

   ```cedar
   @id("refunds-from-order-workflows")
   permit (principal, action == Action::"payments/billing/Refund", resource)
   when {
     context.workflowId like "order-*" &&
     context.parameters has arg0 && context.parameters.arg0.data has amount &&
     context.parameters.arg0.data.amount <= 100
   };
   ```

3. Restart the verifier. With `CEDAR_SCHEMA_FILE` set, it refuses to start if a policy doesn't
   match the schema.

## Running

```bash
temporal operator nexus endpoint create --name tool-verifier \
  --target-namespace default --target-task-queue tool-verifier
CEDAR_SCHEMA_FILE=policies/schema.cedarschema cargo run --release -p cedar-verifier
```

Then point the proxy at it with `TEMPORAL_PROXY_TOOL_VERIFIER=nexus` and
`TEMPORAL_PROXY_TOOL_VERIFIER_NEXUS_{NAMESPACE,ENDPOINT,SERVICE}`, as described in the
[root README](../../README.md#tool-call-verification).

| Variable | Default | Description |
|----------|---------|-------------|
| `TEMPORAL_ADDRESS` | `http://127.0.0.1:7233` | Temporal frontend URL (`https://...` for Temporal Cloud). |
| `TEMPORAL_NAMESPACE` | `default` | Namespace the verifier's Nexus endpoint targets. |
| `TEMPORAL_API_KEY` | — | Optional API key; enables TLS. |
| `VERIFIER_TASK_QUEUE` | `tool-verifier` | Task queue the endpoint targets. |
| `VERIFIER_SERVICE` | `tool-policy` | Nexus service name. |
| `VERIFIER_VERIFY_OPERATION` | `VerifyToolCalls` | Nexus operation that decides tool calls. |
| `VERIFIER_RECORD_OPERATION` | `RecordInputEvents` | Nexus operation that receives input events. |
| `VERIFIER_INPUT_MAX_RUNS` | `10000` | Runs whose input events are kept in memory. |
| `CEDAR_POLICY_FILE` | `policies/example.cedar` | Policies to enforce. |
| `CEDAR_SCHEMA_FILE` | — | If set, policies are validated against this schema at startup, and the verifier refuses to start on errors. Recommended. |
| `RUST_LOG` | `info` | Log filter. |

To see what a policy was evaluated against, run with `RUST_LOG=info,cedar_request=debug`. The
verifier then logs one JSON line per tool call. Each line holds the principal, action, resource, the
exact `context`, the decision, and the `@id`s of the policies that decided it. The demo uses this to
show the evaluated request under each case.

The verifier fails closed in the proxy's favor:
- Input it can't interpret is answered with a Nexus `BAD_REQUEST` handler error, which the proxy
  treats as "verifier unavailable".
- On Ctrl-C or SIGTERM it drains gracefully for up to 10 seconds and then exits. Any verification
  left unanswered is retried by the server.

## Implementation notes

The Temporal Rust SDK (1.0) can't host Nexus handlers yet; its `experimental` feature only adds
calling Nexus operations from workflows. The Nexus worker therefore runs directly on
`temporalio-sdk-core`, which takes care of polling, worker heartbeats and shutdown. It lives in
[`../host`](../host) (`verifier-host`), shared with the [Dogwood verifier](../dogwood). Temporal's
protos are compiled with the `vendored-protox` feature, so no system `protoc` is needed.

`cedar-policy` is pinned to 4.11.0, the version Dogwood pins, so both verifiers share one Cargo
workspace and lockfile (`verifiers/`).

Run the tests with `cargo test -p cedar-verifier` in `verifiers/` (or `make test-cedar-verifier`
from the repo root).
