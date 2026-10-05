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

Every tool call becomes one authorization request. A workflow task is allowed only if all of its
tool calls are.

| Cedar | Value |
|-------|-------|
| `principal` | `Worker::"<subject>"`, the worker identity, with `namespace` |
| `action` | The tool: `Action::"payments/billing/Charge"`, `Action::"trusted-tools/SendEmail"`, … |
| `resource` | `TaskQueue::"<queue>"`, the calling workflow's task queue, with `name` and `namespace` |
| `context.workflowId` | The calling workflow's ID |
| `context.parameterCount` | How many parameters the call has |
| `context.parameters.argN` | Parameter N (`arg0`, `arg1`, …) as `{ type, data }` |

Each tool is its own action, so [`policies/schema.cedarschema`](policies/schema.cedarschema)
declares the shape of its parameters. Cedar denies by default, so a tool no policy permits,
including any tool the schema doesn't declare, is rejected. A denial is reported to the worker with
the `@id` of the deciding policy, for example
`tool payments/billing/Charge denied by Cedar policy charge-limit-1000`.

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

## The example policy

[`policies/example.cedar`](policies/example.cedar) allows:
- the Nexus operation `echo/echo/Echo`, for everyone;
- the Nexus operation `payments/billing/Charge`, in USD or EUR, and never above 1000 (the
  `charge-limit-1000` `forbid` overrides the `permit`);
- the activity `trusted-tools/SendEmail`, only to `@example.com` addresses.

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
CEDAR_SCHEMA_FILE=policies/schema.cedarschema cargo run --release
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
| `VERIFIER_OPERATION` | `VerifyToolCalls` | Nexus operation name. |
| `CEDAR_POLICY_FILE` | `policies/example.cedar` | Policies to enforce. |
| `CEDAR_SCHEMA_FILE` | — | If set, policies are validated against this schema at startup, and the verifier refuses to start on errors. Recommended. |
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

Run the tests with `cargo test` (or `make test-cedar-verifier` from the repo root).
