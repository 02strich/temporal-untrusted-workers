# dogwood-verifier

A tool-call verifier for temporal-proxy (`TEMPORAL_PROXY_TOOL_VERIFIER=nexus`) whose rules are
[Dogwood](https://github.com/dogwood-policy/dogwood) policies. It hosts the `VerifyToolCalls` and
`RecordInputEvents` Nexus operations described in
[`proto/toolpolicy/v1/toolpolicy.proto`](../../proto/toolpolicy/v1/toolpolicy.proto), and is built on
[`dogwood-local-engine`](https://github.com/dogwood-policy/dogwood-local-engine).

Dogwood's core language is Cedar, so every rule the [Cedar verifier](../cedar) can express works here
too. Dogwood adds **temporal conditions** over a history of events: `when temporal { formerly within
30d … }`. Here, that history is what each workflow run has received: its input, signals, updates, and
activity and Nexus results. A policy can therefore say "only refund what an `approval` signal named"
or "only email the address the workflow was started with" by looking back at those events. The engine
keeps the history in a durable store on disk, so it survives verifier restarts.

## How tool calls and inputs map to Dogwood

Dogwood decides **events**. The verifier turns everything the proxy sends it into events in one store:

| From the proxy | Dogwood event |
|----------------|---------------|
| A tool call, e.g. the Nexus operation `payments/billing/Charge` | `request` of `Temporal::Action::"payments/billing/Charge"`. It is a decision point, so Dogwood answers allow or deny. |
| The workflow input of a `ToolWorkflow` run | `received` of `Temporal::Action::"workflow/ToolWorkflow"` |
| A signal or update `X` | `received` of `Temporal::Action::"signal/X"` or `"update/X"` |
| An activity or Nexus result, e.g. `crm/LookupCustomer` | `received` of the tool's own action, `Temporal::Action::"crm/LookupCustomer"` |

`received` events are history: they get no verdict, but later decisions can look back on them. Tool
calls are named as in the Cedar verifier: Nexus operations `<endpoint>/<service>/<operation>`, and
activities on other task queues `<taskQueue>/<activityType>`. A workflow task is approved only if all
of its tool calls are allowed.

A `request` event carries:

| Dogwood | Value |
|---------|-------|
| `principal` | `Temporal::Worker::"<subject>"`, the worker identity |
| `action` | the tool, e.g. `Temporal::Action::"trusted-tools/SendEmail"` |
| `resource` | `Temporal::TaskQueue::"<queue>"`, the calling workflow's task queue |
| `context.input.parameterCount` | how many parameters the call has |
| `context.input.argN` | parameter N (`arg0`, `arg1`, …) |
| `context.caller` | the calling run: `{ namespace, workflowId, run, taskQueue }`, where `run` is `<namespace>/<workflowId>/<runId>` |

A `received` event carries the same `input` (its payloads as `argN`) and `caller`, and temporal
patterns match them as `input.argN…` and `caller.…`.

### Parameters

Parameters arrive as `google.protobuf.Any`s, as described in the Cedar verifier's
[Parameters](../cedar/README.md#parameters) section, and `argN` holds their content:
- a plain JSON value (`google.protobuf.Value`, the SDK default) is the value itself: `SendEmail("a@b")`
  gives `arg0 == "a@b"`, and an object gives a record of its fields;
- a message type gives a record of its fields in protobuf JSON;
- a payload the proxy couldn't interpret gives an empty record.

As in Cedar, arrays become sets, and floats and nulls are dropped. In protobuf JSON, 64-bit integer
fields of message types are strings.

### One history per workflow run

[`policies/events.dwschema`](policies/events.dwschema) declares the two event kinds and pins
`caller.run` on both:

```text
decision event <A>::request  { ...inputs(A), caller: { pin run: String = context.caller.run } }
event <A>::received          { ...inputs(A), caller: { pin run: String = context.caller.run } }
```

A pin on every kind makes Dogwood evaluate every temporal condition **per run**: a decision only sees
events of the same workflow run. Another run's input or approval can't satisfy it, and no policy can
look across runs. `max_window = 30d` caps how far back a condition may look.

### Decisions

Dogwood denies by default, so a tool that no policy permits is rejected, and so is a tool the schema
doesn't declare. A denial is reported to the worker with the `@id` of the deciding policies, e.g.
`tool payments/billing/Charge denied by Dogwood policy charge-limit-10000`.

To ask for human review instead, annotate a `forbid` policy with `@decision("escalate")`. This works
exactly as in the Cedar verifier ([Escalating to a human](../cedar/README.md#escalating-to-a-human)):
if every policy that denied a call carries the annotation, the answer is `DECISION_ESCALATE`, and any
call in the task that is denied outright wins.

Evaluation errors, such as reading a field that's missing, are logged and deny the call.

## The example policy

[`policies/example.dw`](policies/example.dw), written against
[`policies/schema.cedarschema`](policies/schema.cedarschema), has the same rules as the Cedar
verifier's example:
- the Nexus operation `echo/echo/Echo` is allowed for everyone;
- the Nexus operation `payments/billing/Charge` is allowed in USD or EUR:
  - up to 1000 it goes through;
  - above 1000 it is escalated for review (`charge-over-1000-needs-review`);
  - above 10000, or without an amount, it is denied (`charge-limit-10000`);
- the activity `trusted-tools/SendEmail` may only go to the customer the run was started for:

  ```text
  @id("send-email-to-customer")
  permit (principal, action == Temporal::Action::"trusted-tools/SendEmail", resource)
  when temporal {
    formerly within 30d Temporal::Action::"workflow/ToolWorkflow"::received{
      input.arg0.customerEmail: context.input.arg0
    }
  };
  ```

- the activity `trusted-tools/IssueRefund` is only allowed for a refund that an `approval` signal named
  (`refund-needs-approval`, the same pattern on `Temporal::Action::"signal/approval"`). Signals are
  sent from outside the workflow, so this is a human's approval, not something the workflow decided.

## Declaring a tool or an input

Every action a policy refers to, whether a tool or an input event, must be declared in the schema
with the shape of its `input`. Mark parameters optional (`?`) and check them with `has` in `when`
conditions, so a missing parameter fails closed instead of erroring:

```cedar
action "payments/billing/Refund" appliesTo {
  principal: [Worker], resource: [TaskQueue],
  context: {
    input: { parameterCount: Long, arg0?: { orderId?: String, amount?: Long } },
    caller: Caller,
  },
};
```

Events of undeclared actions are still recorded, but no policy can refer to them.

## Policy changes keep the history

On every start, the verifier brings the store's policies in line with `DOGWOOD_POLICY_FILE`. It
deletes the policies that are gone and adds the new ones, and keeps the unchanged ones. Dogwood keeps
the history each kept policy has accumulated. A new or edited policy starts with an empty history, so
it only sees events recorded after the change. The event schema is fixed when the store is created:
to change it, start with a new store.

A store with policies that don't validate against the schemas is refused at startup, and the verifier
doesn't start.

## Running

```bash
temporal operator nexus endpoint create --name tool-verifier \
  --target-namespace default --target-task-queue tool-verifier
cargo run --release -p dogwood-verifier   # in verifiers/dogwood
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
| `VERIFIER_INPUT_MAX_RUNS` | `10000` | Runs whose recorded event IDs are remembered in memory, to skip redelivered events. |
| `DOGWOOD_POLICY_FILE` | `policies/example.dw` | Policies to enforce. |
| `DOGWOOD_SCHEMA_FILE` | `policies/schema.cedarschema` | Action schema the policies are validated against. |
| `DOGWOOD_EVENT_SCHEMA_FILE` | `policies/events.dwschema` | Event schema; fixed once the store exists. |
| `DOGWOOD_STORE` | `dogwood.redb` | The durable store: the history and the installed policies. |
| `RUST_LOG` | `info` | Log filter. |

To see what a policy was evaluated against, run with `RUST_LOG=info,dogwood_request=debug`. The
verifier then logs one JSON line per tool call. Each line holds the principal, action, resource, the
exact `context`, the latest events the run received (`history`, up to 20), the decision, and the
`@id`s of the deciding policies. The demo uses this to show the evaluated request under each case.

## Operating notes

Dogwood's own [embedding notes](https://github.com/dogwood-policy/dogwood-local-engine#embedding-the-engine-safely)
apply. In particular:
- **Run a single verifier instance per store.** Every event has to reach the store that decides, and
  the store file can't be shared between processes.
- **Protect the store file.** It holds the installed policies and the history the decisions depend on.
  Whoever can write it can change what is allowed.
- **Keep your own audit log.** The store is checkpointed and pruned, and it doesn't record decisions.
- **Redelivered events.** A redelivered input event is skipped if its run is among the last
  `VERIFIER_INPUT_MAX_RUNS` seen since the verifier started. Otherwise it is recorded again, which
  doesn't affect `formerly` conditions but does affect `count` aggregations.
- **Fails closed.** Input the verifier can't interpret, or a store that can't record, is answered with
  a Nexus handler error, which the proxy treats as "verifier unavailable". An event is acknowledged
  only once it is durably recorded, so the proxy redelivers anything that failed. On Ctrl-C or SIGTERM
  the verifier drains for up to 10 seconds and then exits.

Run the tests with `cargo test -p dogwood-verifier` in `verifiers/` (or `make test-dogwood-verifier`
from the repo root).
