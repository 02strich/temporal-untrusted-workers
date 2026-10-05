# Shared helpers for the demos. Each demo's env.sh sets these, then sources
# this file:
#
#   DEMO_DIR        the demo's folder
#   DEMO_PORT_BASE  base for the demo's ports, e.g. 47000. Each demo uses its
#                   own base so demos don't clash with each other or with a
#                   Temporal server you already run on 7233.
#
# It derives RUN_DIR (logs, pids, binaries; git-ignored) and the addresses
# below, and provides functions for the stack pieces most demos share.

DEMOS_DIR="$(cd "$DEMO_DIR/.." && pwd)"
REPO_DIR="$(cd "$DEMOS_DIR/.." && pwd)"
DEMO_NAME="$(basename "$DEMO_DIR")"
RUN_DIR="$DEMO_DIR/run"

TEMPORAL_PORT=$((DEMO_PORT_BASE + 233))
PROXY_PORT=$((DEMO_PORT_BASE + 250))
export TEMPORAL_ADDRESS="127.0.0.1:$TEMPORAL_PORT"
export PROXY_ADDRESS="127.0.0.1:$PROXY_PORT"
export PROXY_LOG="$RUN_DIR/proxy.log"

# step prints a green check line, for progress output in recordings.
step() { printf '  \033[32m✔\033[0m %s\n' "$1"; }

# bg LOG CMD... runs CMD in the background, logging to $RUN_DIR/LOG.log and
# recording its PID for stack_down.
bg() {
  local log=$1
  shift
  "$@" >>"$RUN_DIR/$log.log" 2>&1 &
  echo $! >>"$RUN_DIR/pids"
}

wait_port() { until nc -z 127.0.0.1 "$1" 2>/dev/null; do sleep 0.2; done; }

# stack_init prepares a fresh RUN_DIR.
stack_init() {
  mkdir -p "$RUN_DIR"
  : >"$RUN_DIR/pids"
}

# build_harness builds the demo's Go program (demos/<demo>/main.go) to
# $RUN_DIR/demo.
build_harness() { (cd "$DEMOS_DIR" && go build -o "$RUN_DIR/demo" "./$DEMO_NAME"); }

# build_proxy builds temporal-proxy to $RUN_DIR/temporal-proxy.
build_proxy() { (cd "$REPO_DIR" && go build -o "$RUN_DIR/temporal-proxy" ./cmd/temporal-proxy); }

# start_temporal [ARGS...] starts a Temporal dev server with standalone Nexus
# operations enabled, plus any extra start-dev ARGS, and waits until it serves.
start_temporal() {
  (cd "$RUN_DIR" && exec temporal server start-dev --headless \
    --port "$TEMPORAL_PORT" --http-port $((DEMO_PORT_BASE + 243)) \
    --ui-port $((DEMO_PORT_BASE + 1233)) --metrics-port $((DEMO_PORT_BASE + 2090)) \
    --dynamic-config-value nexusoperation.enableStandalone=true \
    "$@" >"$RUN_DIR/temporal.log" 2>&1) &
  echo $! >>"$RUN_DIR/pids"
  until temporal operator cluster health --address "$TEMPORAL_ADDRESS" >/dev/null 2>&1; do sleep 0.5; done
}

# create_endpoint NAME TASK_QUEUE creates a Nexus endpoint in namespace
# default targeting TASK_QUEUE.
create_endpoint() {
  temporal operator nexus endpoint create --address "$TEMPORAL_ADDRESS" --name "$1" \
    --target-namespace default --target-task-queue "$2" >/dev/null
}

# start_proxy [VAR=VALUE...] starts temporal-proxy in front of the dev server
# with the repo's test auth file, plus any extra environment settings (e.g.
# TEMPORAL_PROXY_TOOL_VERIFIER=nexus), and waits until it listens.
start_proxy() {
  (cd "$RUN_DIR" && bg proxy env \
    TEMPORAL_PROXY_LISTEN_ADDR="$PROXY_ADDRESS" \
    TEMPORAL_PROXY_UPSTREAM_ADDR="$TEMPORAL_ADDRESS" \
    TEMPORAL_PROXY_STATIC_AUTH_FILE="$REPO_DIR/testdata/static-auth.json" \
    "$@" "$RUN_DIR/temporal-proxy")
  wait_port "$PROXY_PORT"
}

# stack_down stops everything the demo started.
stack_down() {
  [ -f "$RUN_DIR/pids" ] && xargs kill <"$RUN_DIR/pids" 2>/dev/null
  pkill -f "$RUN_DIR/" 2>/dev/null
  pkill -f "start-dev --headless --port $TEMPORAL_PORT" 2>/dev/null
  true
}
