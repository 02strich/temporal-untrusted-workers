# Demo: untrusted Temporal workers making tool calls, judged by Cedar policies.
# Each case shows the request Cedar evaluated (principal, action, resource,
# context) and the policies that decided it, next to the policy file.
#
# Render with:   cd demos/cedar-tool-calls && vhs demo.tape
# (Requires the Temporal CLI, Go, Rust and VHS; the stack is started by up.sh.)

Output out/cedar-tool-calls.mp4

Set Shell "bash"
Set FontSize 15
Set Width 1600
Set Height 1200
Set Padding 24
Set Theme "Catppuccin Mocha"
Set WindowBar Colorful
Set TypingSpeed 30ms

# --- setup (not recorded) ---------------------------------------------------
Hide
Type "./down.sh; rm -rf run; source env.sh; export PS1='\[\e[36m\]$ \[\e[0m\]'" Enter
# Warm the build caches so up.sh doesn't compile on camera.
Type "(cd $REPO_DIR/verifiers/cedar && cargo build -q --release) && (cd $DEMOS_DIR && go build -o /dev/null ./cedar-tool-calls) && (cd $REPO_DIR && go build -o /dev/null ./cmd/temporal-proxy) && clear && echo ready" Enter
Wait+Screen@600s /^ready/
Type "clear" Enter
Sleep 1s
Show

# --- intro --------------------------------------------------------------------
Type "# temporal-proxy lets untrusted workers run workflows safely."
Enter
Sleep 1s
Type "# Calls that leave the worker's task queue are 'tool calls', judged by Cedar."
Enter
Sleep 2s

# --- bring up the stack ---------------------------------------------------------
Type "./up.sh"
Enter
Wait+Screen@120s /Untrusted worker/
Sleep 3s
Type "clear" Enter

# --- the policy -----------------------------------------------------------------
Type "# The Cedar policy (comments stripped):"
Enter
Type "grep -v '^ *//' run/policy.cedar | cat -s"
Enter
Sleep 10s
Type "clear" Enter

# --- allowed: the address is in the workflow's input ----------------------------
Type "# Each workflow starts with the customer's email (alice@example.com) as input,"
Enter
Type "# which the proxy records with the verifier: it shows up in context.workflowInput."
Enter
Type "# send-email-to-customer permits emails to that customer:"
Enter
Type "demo run email-ok"
Enter
Wait+Screen@60s /email sent to alice/
Sleep 7s
Type "clear" Enter

# --- denied: an address the workflow never received -----------------------------
Type "# Another address - as if a prompt injection steered the workflow there -"
Enter
Type "# isn't the workflow input's customerEmail, so no policy permits the email:"
Enter
Type "demo run email-bad"
Enter
Wait+Screen@60s /SendEmail not permitted/
Sleep 7s
Type "clear" Enter

# --- escalated: parameters drive the decision -----------------------------------
Type "# Parameters count too: arg0.data.amount is over 1000, so the forbid annotated"
Enter
Type `# @decision("escalate") sends this charge to human review:`
Enter
Type "demo run charge-big"
Enter
Wait+Screen@60s /pending human review/
Sleep 7s
Type "clear" Enter

# --- signals: an approval for a later action -------------------------------------
Type "# Signals are input events too. A manager approves refund R-1001 by signaling the"
Enter
Type "# workflow; refund-needs-approval checks context.events.signal.approval:"
Enter
Type "demo run refund-approved"
Enter
Wait+Screen@60s /refunded R-1001/
Sleep 8s
Type "clear" Enter

Type "# Without that signal the refund is denied - even though R-1001 is in the"
Enter
Type "# workflow's own input: only an approval signal counts:"
Enter
Type "demo run refund-unapproved"
Enter
Wait+Screen@60s /IssueRefund not permitted/
Sleep 8s
Type "clear" Enter

# --- local rules ----------------------------------------------------------------
Type "# Child workflows must stay on the worker's own task queue."
Enter
Type "# The proxy enforces that itself; Cedar is never asked:"
Enter
Type "demo run child-cross"
Enter
Wait+Screen@60s /not authorized queue/
Sleep 4s
Type "clear" Enter

# --- change the policy live -----------------------------------------------------
Type "# Policies, not code: raise the review threshold to 10000 ..."
Enter
Type "sed -i '' 's/<= 1000$/<= 10000/' run/policy.cedar && ./restart-verifier.sh"
Enter
Wait+Screen@30s /reloaded/
Sleep 1s
Type "# ... and the same context now passes the escalation policy:"
Enter
Type "demo run charge-big"
Enter
Wait+Screen@60s /charged 5000/
Sleep 7s

# --- teardown (not recorded) ------------------------------------------------------
Hide
Type "./down.sh" Enter
Sleep 2s
