# cedar-tool-calls

Untrusted workflows calling trusted "tools", judged by the
[Cedar verifier](../../verifiers/cedar). The proxy runs with `TEMPORAL_PROXY_TOOL_VERIFIER=nexus`.

The walkthrough ([`demo.tape`](demo.tape), about 70 seconds):

1. Starts the stack:
   - a Temporal dev server;
   - trusted handlers for the Nexus operations `echo/Echo` and `billing/Charge`, and the activity
     `SendEmail` on `trusted-tools`;
   - the Cedar verifier with [`example.cedar`](../../verifiers/cedar/policies/example.cedar);
   - temporal-proxy;
   - an untrusted worker on `proxy-test-queue`, connected through the proxy.
2. Shows the policy.
3. **Allowed:** an echo call, a 500 USD charge, and an email to `alice@example.com` all reach the
   trusted handlers.
4. **Blocked by Cedar:**
   - a 5000 USD charge is escalated for human review (`charge-over-1000-needs-review`, answered as
     `DECISION_ESCALATE`);
   - a 50000 USD charge is denied outright (`charge-limit-10000`);
   - an email to `mallory@evil.com` is denied because no policy permits it.
5. **Blocked by the proxy itself:** a child workflow on another task queue. The verifier is never
   asked.
6. **Live policy edit:** the review threshold is raised to 10000, the verifier is restarted, and the
   same 5000 charge now goes through.

## Running it yourself

Requirements: Go, Rust and the Temporal CLI.

```bash
./up.sh
source env.sh
demo run echo charge-ok charge-big charge-huge email-ok email-bad child-cross
```

Edit `run/policy.cedar` (a copy of the example policy) and run `./restart-verifier.sh` to try your
own rules. Run `./down.sh` to stop everything.

To render the video to `out/cedar-tool-calls.mp4`, run `vhs demo.tape`.

The demo uses ports 47233 (Temporal), 47250 (proxy), 48233 (Temporal UI) and 49090 (metrics).
