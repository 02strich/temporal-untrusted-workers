# Demo: an untrusted analytics agent whose SQL tool calls are judged by
# Dogwood. Queries may read metrics and customers but never write; results may
# only be published from runs that never read the customers table.
#
# Render with:   cd demos/dogwood-analytics && vhs demo.tape
# (Requires the Temporal CLI, Go, Rust and VHS; the stack is started by up.sh.)

Output out/dogwood-analytics.mp4

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
Type "(cd $REPO_DIR/verifiers && cargo build -q --release -p dogwood-verifier) && (cd $DEMOS_DIR && go build -o /dev/null ./dogwood-analytics) && (cd $REPO_DIR && go build -o /dev/null ./cmd/temporal-proxy) && clear && echo ready" Enter
Wait+Screen@600s /^ready/
Type "clear" Enter
Sleep 1s
Show

# --- intro --------------------------------------------------------------------
Type "# Northwind's 'Insights' agent answers product questions: it runs SQL through"
Enter
Type "# a trusted RunQuery tool and posts answers with PublishResults. The agent itself"
Enter
Type "# runs as an untrusted Temporal worker, so every tool call goes past Dogwood."
Enter
Sleep 2s

# --- bring up the stack ---------------------------------------------------------
Type "./up.sh"
Enter
Wait+Screen@120s /Insights agent/
Sleep 3s
Type "clear" Enter

# --- the policy -----------------------------------------------------------------
Type "# The verifier parses each query: context.sql has the tables it reads and writes,"
Enter
Type "# and every table read is recorded in the run's history as a read_table event."
Enter
Type "grep -v '^ *//' run/policy.dw | cat -s"
Enter
Sleep 12s
Type "clear" Enter

# --- allowed: metrics only ------------------------------------------------------
Type "# A KPI question: one query on metrics, then a post to #kpi-weekly."
Enter
Type "demo run weekly-kpis"
Enter
Wait+Screen@60s /posted/
Sleep 8s
Type "clear" Enter

# --- denied: the answer was built from customer data ----------------------------
Type "# 'Which accounts drove the spike?' needs the customers table. Reading it is fine,"
Enter
Type "# but now the run's history has read_table customers, so publishing is forbidden:"
Enter
Type "demo run top-accounts"
Enter
Wait+Screen@60s /PublishResults denied/
Sleep 9s
Type "clear" Enter

# --- denied: prompt injection, clean-looking report -----------------------------
Type "# A prompt injection makes the agent look up customer emails first. Its report only"
Enter
Type "# shows KPI numbers - but Dogwood judges the run's history, not the report's text:"
Enter
Type "demo run injected"
Enter
Wait+Screen@60s /PublishResults denied/
Sleep 9s
Type "clear" Enter

# --- denied: writes and DDL -------------------------------------------------------
Type "# Queries are read-only: an UPDATE has writes: [metrics], so no policy permits it."
Enter
Type "demo run fix-the-numbers"
Enter
Wait+Screen@60s /RunQuery not permitted/
Sleep 6s
Type "clear" Enter
Type "# Anything the verifier can't analyze (DROP TABLE) gets no context.sql: denied."
Enter
Type "demo run drop-table"
Enter
Wait+Screen@60s /RunQuery not permitted/
Sleep 5s
Type "clear" Enter

# --- denied: nothing queried ------------------------------------------------------
Type "# And numbers the agent made up, without querying metrics at all, can't be posted:"
Enter
Type "demo run publish-only"
Enter
Wait+Screen@60s /PublishResults not permitted/
Sleep 6s

# --- teardown (not recorded) ------------------------------------------------------
Hide
Type "./down.sh" Enter
Sleep 2s
