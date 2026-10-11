# Demo: untrusted Temporal workers making tool calls, judged by Cedar policies.
#
# Render with:   cd demos/cedar-tool-calls && vhs demo.tape
# (Requires the Temporal CLI, Go, Rust and VHS; the stack is started by up.sh.)

Output out/cedar-tool-calls.mp4

Set Shell "bash"
Set FontSize 17
Set Width 1440
Set Height 900
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
Sleep 9s
Type "clear" Enter

# --- allowed calls --------------------------------------------------------------
Type "# Calls the policy permits reach the trusted handlers:"
Enter
Type "demo run echo charge-ok email-ok"
Enter
Wait+Screen@60s /email sent to alice/
Sleep 4s

# --- blocked calls --------------------------------------------------------------
Type "# Others are escalated for human review, or denied outright:"
Enter
Type "demo run charge-big charge-huge email-bad"
Enter
Wait+Screen@60s /SendEmail not permitted/
Sleep 4s
Type "clear" Enter

# --- local rules ----------------------------------------------------------------
Type "# Child workflows must stay on the worker's own task queue."
Enter
Type "# The proxy enforces that itself; the verifier is never asked:"
Enter
Type "demo run child-cross"
Enter
Wait+Screen@60s /not authorized queue/
Sleep 4s
Type "clear" Enter

# --- change the policy live -----------------------------------------------------
Type "# Policies, not code: raise the review threshold to 10000 ..."
Enter
Type "sed -i '' 's/<= 1000$/<= 10000/' run/policy.cedar"
Enter
Type "grep -A6 '@decision' run/policy.cedar"
Enter
Sleep 3s
Type "./restart-verifier.sh"
Enter
Wait+Screen@30s /reloaded/
Sleep 1s
Type "# ... and the same 5000 charge now goes through:"
Enter
Type "demo run charge-big"
Enter
Wait+Screen@60s /charged 5000/
Sleep 5s

# --- teardown (not recorded) ------------------------------------------------------
Hide
Type "./down.sh" Enter
Sleep 2s
