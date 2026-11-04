# Settings for the dogwood-analytics demo; source this in a shell to use the
# `demo` command against a running stack.
DEMO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEMO_PORT_BASE=55000
source "$DEMO_DIR/../lib/stack.sh"

export POLICY_FILE="$RUN_DIR/policy.dw"
# The verifier logs each evaluated Dogwood request (dogwood_request=debug);
# the demo command shows them under each case.
export VERIFIER_LOG="$RUN_DIR/dogwood-verifier.log"
alias demo="$RUN_DIR/demo"

# start_verifier starts the Dogwood verifier on $POLICY_FILE, with the demo's
# action schema and the verifier's event schema. Its history lives in
# run/dogwood.redb and survives restarts.
start_verifier() {
  (cd "$RUN_DIR" && TEMPORAL_ADDRESS="http://$TEMPORAL_ADDRESS" DOGWOOD_POLICY_FILE="$POLICY_FILE" \
    DOGWOOD_SCHEMA_FILE="$DEMO_DIR/policies/schema.cedarschema" \
    DOGWOOD_EVENT_SCHEMA_FILE="$REPO_DIR/verifiers/dogwood/policies/events.dwschema" \
    DOGWOOD_STORE="$RUN_DIR/dogwood.redb" RUST_LOG=info,dogwood_request=debug \
    bg dogwood-verifier "$RUN_DIR/dogwood-verifier")
}
