# cedar-tool-calls

Untrusted workflows calling trusted "tools", judged by the
[Cedar verifier](../../verifiers/cedar). The proxy runs with `TEMPORAL_PROXY_TOOL_VERIFIER=nexus`.

Every case shows the request Cedar evaluated: the principal, the action, the resource, the full
`context`, and the `@id` of the policy that decided it. Viewers can match each policy condition (for
example `context.workflowInput.parameters.arg0.data.customerEmail` or `arg0.data.amount <= 1000`)
to the values it was checked against.

The walkthrough ([`demo.tape`](demo.tape), about two minutes):

1. Starts the stack:
   - a Temporal dev server;
   - trusted handlers for the Nexus operations `echo/Echo` and `billing/Charge`, and the activity
     `SendEmail` and `IssueRefund` on `trusted-tools`;
   - the Cedar verifier with [`example.cedar`](../../verifiers/cedar/policies/example.cedar);
   - temporal-proxy;
   - an untrusted worker on `proxy-test-queue`, connected through the proxy.
2. Shows the policy.
3. **Allowed:** an email to `alice@example.com`. Each workflow starts with that address as the
   customer's input (`customerEmail`), and the proxy records it with the verifier. It appears in
   `context.workflowInput`, so `send-email-to-customer` permits the email.
4. **Denied:** an email to `mallory@evil.com`, as if a prompt injection had steered the workflow
   there. It isn't the workflow input's `customerEmail`, so no policy permits the email.
5. **Escalated:** a 5000 USD charge. `context.parameters.arg0.data.amount` is over 1000, so the
   `forbid` annotated `@decision("escalate")` sends it to human review.
6. **Approved by a signal:** a manager approves refund `R-1001` by sending the workflow an
   `approval` signal (`{refundId, approvedBy}`). The proxy records the signal before the workflow
   reacts to it. The workflow then calls `trusted-tools/IssueRefund("R-1001")`, and
   `refund-needs-approval` permits it because
   `context.events.signal["approval"].parameters.arg0.data.refundId` is `R-1001`.
7. **Refund without approval:** the same refund without the signal is denied. `R-1001` is in the
   workflow's own input, but only an approval signal counts.
8. **Blocked by the proxy itself:** a child workflow on another task queue. Cedar is never asked.
9. **Live policy edit:** the review threshold is raised to 10000 and the verifier is restarted. The
   same charge, with the same context, is now allowed by `charge-in-usd-or-eur`.

## Running it yourself

Requirements: Go, Rust and the Temporal CLI.

```bash
./up.sh
source env.sh
demo run echo charge-ok charge-big charge-huge email-ok email-bad refund-approved refund-unapproved child-cross
```

Edit `run/policy.cedar` (a copy of the example policy) and run `./restart-verifier.sh` to try your
own rules. Run `./down.sh` to stop everything.

To render the video to `out/cedar-tool-calls.mp4`, run `vhs demo.tape`.

The demo uses ports 47233 (Temporal), 47250 (proxy), 48233 (Temporal UI) and 49090 (metrics).
