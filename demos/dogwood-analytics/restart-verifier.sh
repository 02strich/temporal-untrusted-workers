#!/usr/bin/env bash
# Restarts the verifier so it picks up edits to run/policy.dw (Dogwood) or a
# rebuilt binary (VERIFIER=go). Dogwood keeps the history in run/dogwood.redb,
# and the history of every policy the edit leaves unchanged; the Go verifier
# starts with an empty memory.
set -euo pipefail
source "$(dirname "$0")/env.sh"
pkill -INT -f "$RUN_DIR/$VERIFIER_BIN" || true
while pgrep -f "$RUN_DIR/$VERIFIER_BIN" >/dev/null; do sleep 0.2; done
[ "$VERIFIER" = go ] && build_verifier
start_verifier
sleep 1.5
step "$VERIFIER_ENGINE verifier restarted"
