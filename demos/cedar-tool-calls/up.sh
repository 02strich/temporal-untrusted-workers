#!/usr/bin/env bash
# Starts the demo stack in the background:
#   Temporal dev server -> trusted handlers -> Cedar verifier -> temporal-proxy
#   (TEMPORAL_PROXY_TOOL_VERIFIER=nexus) -> untrusted worker behind the proxy.
set -euo pipefail
source "$(dirname "$0")/env.sh"
stack_init

# Build everything quietly first.
build_harness
build_proxy
(cd "$REPO_DIR/verifiers/cedar" && cargo build -q --release && cp target/release/cedar-verifier "$RUN_DIR/")
cp "$REPO_DIR/verifiers/cedar/policies/example.cedar" "$POLICY_FILE"

start_temporal
step "Temporal dev server            $TEMPORAL_ADDRESS"

create_endpoint echo nexus-handlers
create_endpoint payments nexus-handlers
create_endpoint tool-verifier tool-verifier
step "Nexus endpoints                echo, payments, tool-verifier"

(cd "$RUN_DIR" && bg handlers "$RUN_DIR/demo" handlers)
step "Trusted handlers               Nexus echo + billing, activity SendEmail"

start_verifier
step "Cedar verifier                 policy.cedar (schema-validated)"

start_proxy \
  TEMPORAL_PROXY_TOOL_VERIFIER=nexus \
  TEMPORAL_PROXY_TOOL_VERIFIER_NEXUS_NAMESPACE=default \
  TEMPORAL_PROXY_TOOL_VERIFIER_NEXUS_ENDPOINT=tool-verifier \
  TEMPORAL_PROXY_TOOL_VERIFIER_NEXUS_SERVICE=tool-policy
step "temporal-proxy                 $PROXY_ADDRESS  (tool verifier: nexus)"

(cd "$RUN_DIR" && bg untrusted "$RUN_DIR/demo" untrusted)
sleep 2
step "Untrusted worker               via proxy, task queue proxy-test-queue"
