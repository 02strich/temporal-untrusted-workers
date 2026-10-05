#!/usr/bin/env bash
# Restarts the Cedar verifier so it picks up edits to run/policy.cedar.
set -euo pipefail
source "$(dirname "$0")/env.sh"
pkill -INT -f "$RUN_DIR/cedar-verifier" || true
while pgrep -f "$RUN_DIR/cedar-verifier" >/dev/null; do sleep 0.2; done
start_verifier
sleep 1.5
step "Cedar verifier reloaded policy.cedar"
