# dogwood-tool-calls

The same untrusted workflows and trusted "tools" as [cedar-tool-calls](../cedar-tool-calls), judged
by the [Dogwood verifier](../../verifiers/dogwood) instead. The proxy runs with
`TEMPORAL_PROXY_TOOL_VERIFIER=nexus`.

Dogwood policies are Cedar policies plus temporal conditions. The verifier records everything a
workflow run receives (its input, signals, updates, and activity and Nexus results) as history in a
durable store. Policies look back on that history with `formerly within …`, instead of reading a
snapshot of it from the context.

Every case shows the request Dogwood evaluated: the principal, the action, the resource, the full
`context`, the run's **history** the temporal conditions could look back on, and the `@id` of the
policy that decided it.

The walkthrough ([`demo.tape`](demo.tape), about two minutes):

1. Starts the stack:
   - a Temporal dev server;
   - trusted handlers for the Nexus operations `echo/Echo` and `billing/Charge`, and the activities
     `SendEmail` and `IssueRefund` on `trusted-tools`;
   - the Dogwood verifier with [`example.dw`](../../verifiers/dogwood/policies/example.dw), its history
     in `run/dogwood.redb`;
   - temporal-proxy;
   - an untrusted worker on `proxy-test-queue`, connected through the proxy.
2. Shows the policy.
3. **Allowed:** an email to `alice@example.com`. Each workflow starts with that address as the
   customer's input (`customerEmail`). The proxy records the input with the verifier, which adds it to
   the run's history as a `workflow/ToolWorkflow` event. `send-email-to-customer` permits the email
   because the history has an input whose `customerEmail` is the recipient
   (`input.arg0.customerEmail: context.input.arg0`).
4. **Denied:** an email to `mallory@evil.com`, as if a prompt injection had steered the workflow
   there. No input in the run's history names it, so no policy permits the email.
5. **Escalated:** a 5000 USD charge. `context.input.arg0.amount` is over 1000, so the `forbid`
   annotated `@decision("escalate")` sends it to human review.
6. **Approved by a signal:** a manager approves refund `R-1001` by sending the workflow an `approval`
   signal (`{refundId, approvedBy}`). The proxy records the signal, as a `signal/approval` event,
   before the workflow reacts to it. The workflow then calls `trusted-tools/IssueRefund("R-1001")`,
   and `refund-needs-approval` permits it because the history has an approval naming `R-1001`.
7. **Refund without approval:** the same refund without the signal is denied. `R-1001` is in the
   workflow's own input, but only an approval signal counts.
8. **Blocked by the proxy itself:** a child workflow on another task queue. Dogwood is never asked.
9. **Live policy edit:** the review threshold is raised to 10000 and the verifier is restarted. It
   swaps the edited policy and keeps the others, together with their history. The same charge is now
   allowed by `charge-in-usd-or-eur`.

Each workflow run only ever sees its own history: the event schema pins every event to its run
(`caller.run`), so the approval sent to one run can't approve a refund in another.

## Running it yourself

Requirements: Go, Rust and the Temporal CLI.

```bash
./up.sh
source env.sh
demo run echo charge-ok charge-big charge-huge email-ok email-bad refund-approved refund-unapproved child-cross
```

Edit `run/policy.dw` (a copy of the example policy) and run `./restart-verifier.sh` to try your own
rules. Run `./down.sh` to stop everything. `up.sh` starts with an empty history; `restart-verifier.sh`
keeps it.

To render the video to `out/dogwood-tool-calls.mp4`, run `vhs demo.tape`.

The demo uses ports 51233 (Temporal), 51250 (proxy), 52233 (Temporal UI) and 53090 (metrics).
