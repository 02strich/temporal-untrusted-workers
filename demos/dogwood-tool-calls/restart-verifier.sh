#!/usr/bin/env bash
# Restarts the Dogwood verifier so it picks up edits to run/policy.dw. The
# history in run/dogwood.redb is kept, and so is the history of every policy
# the edit leaves unchanged.
set -euo pipefail
source "$(dirname "$0")/env.sh"
pkill -INT -f "$RUN_DIR/dogwood-verifier" || true
while pgrep -f "$RUN_DIR/dogwood-verifier" >/dev/null; do sleep 0.2; done
start_verifier
sleep 1.5
step "Dogwood verifier reloaded policy.dw"
