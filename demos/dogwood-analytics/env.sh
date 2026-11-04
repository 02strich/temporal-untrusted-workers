# Settings for the dogwood-analytics demo; source this in a shell to use the
# `demo` command against a running stack.
#
# VERIFIER picks the verifier enforcing the policy:
#   dogwood (default)  the Dogwood verifier with policies/analytics.dw
#   go                 verifiers/analytics, the same rules in plain Go
DEMO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEMO_PORT_BASE=55000
source "$DEMO_DIR/../lib/stack.sh"

export VERIFIER="${VERIFIER:-dogwood}"
export POLICY_FILE="$RUN_DIR/policy.dw"
case "$VERIFIER" in
  dogwood)
    VERIFIER_BIN=dogwood-verifier
    # The verifier logs each evaluated Dogwood request (dogwood_request=debug).
    export VERIFIER_TARGET=dogwood_request VERIFIER_ENGINE=Dogwood
    ;;
  go)
    VERIFIER_BIN=analytics-verifier
    export VERIFIER_TARGET=analytics_request VERIFIER_ENGINE=Go
    ;;
  *)
    echo "unknown VERIFIER=$VERIFIER (expected dogwood or go)" >&2
    return 1 2>/dev/null || exit 1
    ;;
esac
# The demo command shows the verifier's evaluations under each case.
export VERIFIER_LOG="$RUN_DIR/$VERIFIER_BIN.log"
alias demo="$RUN_DIR/demo"

# build_verifier builds the selected verifier to $RUN_DIR/$VERIFIER_BIN.
build_verifier() {
  case "$VERIFIER" in
    dogwood) (cd "$REPO_DIR/verifiers" && cargo build -q --release -p dogwood-verifier && cp target/release/dogwood-verifier "$RUN_DIR/") ;;
    go) (cd "$REPO_DIR/verifiers/analytics" && go build -o "$RUN_DIR/analytics-verifier" .) ;;
  esac
}

# start_verifier starts the selected verifier. Dogwood reads $POLICY_FILE,
# the demo's action schema and the verifier's event schema, and keeps its
# history in run/dogwood.redb across restarts; the Go verifier keeps what
# each run read in memory only.
start_verifier() {
  case "$VERIFIER" in
    dogwood)
      (cd "$RUN_DIR" && TEMPORAL_ADDRESS="http://$TEMPORAL_ADDRESS" DOGWOOD_POLICY_FILE="$POLICY_FILE" \
        DOGWOOD_SCHEMA_FILE="$DEMO_DIR/policies/schema.cedarschema" \
        DOGWOOD_EVENT_SCHEMA_FILE="$REPO_DIR/verifiers/dogwood/policies/events.dwschema" \
        DOGWOOD_STORE="$RUN_DIR/dogwood.redb" RUST_LOG=info,dogwood_request=debug \
        bg dogwood-verifier "$RUN_DIR/dogwood-verifier")
      ;;
    go)
      (cd "$RUN_DIR" && TEMPORAL_ADDRESS="$TEMPORAL_ADDRESS" bg analytics-verifier "$RUN_DIR/analytics-verifier")
      ;;
  esac
}
