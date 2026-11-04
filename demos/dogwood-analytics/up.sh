#!/usr/bin/env bash
# Starts the demo stack in the background:
#   Temporal dev server -> trusted analytics tools -> verifier (Dogwood, or
#   plain Go with VERIFIER=go) -> temporal-proxy
#   (TEMPORAL_PROXY_TOOL_VERIFIER=nexus) -> untrusted agent.
set -euo pipefail
source "$(dirname "$0")/env.sh"
stack_init

# Build everything quietly first.
build_harness
build_proxy
build_verifier
if [ "$VERIFIER" = dogwood ]; then
  cp "$DEMO_DIR/policies/analytics.dw" "$POLICY_FILE"
  # A fresh dev server means fresh workflow runs: start with an empty history.
  rm -f "$RUN_DIR/dogwood.redb"
fi

start_temporal
step "Temporal dev server            $TEMPORAL_ADDRESS"

create_endpoint tool-verifier tool-verifier
step "Nexus endpoint                 tool-verifier"

(cd "$RUN_DIR" && bg handlers "$RUN_DIR/demo" handlers)
step "Trusted analytics tools        RunQuery + PublishResults on task queue analytics"

start_verifier
if [ "$VERIFIER" = dogwood ]; then
  step "Dogwood verifier               policy.dw (schema-validated), history in dogwood.redb"
else
  step "Go verifier                    verifiers/analytics (rules in policy.go)"
fi

start_proxy \
  TEMPORAL_PROXY_TOOL_VERIFIER=nexus \
  TEMPORAL_PROXY_TOOL_VERIFIER_NEXUS_NAMESPACE=default \
  TEMPORAL_PROXY_TOOL_VERIFIER_NEXUS_ENDPOINT=tool-verifier \
  TEMPORAL_PROXY_TOOL_VERIFIER_NEXUS_SERVICE=tool-policy
step "temporal-proxy                 $PROXY_ADDRESS  (tool verifier: nexus)"

(cd "$RUN_DIR" && bg untrusted "$RUN_DIR/demo" untrusted)
sleep 2
step "Insights agent (untrusted)     via proxy, task queue proxy-test-queue"
