# Settings for the cedar-tool-calls demo; source this in a shell to use the
# `demo` command against a running stack.
DEMO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEMO_PORT_BASE=47000
source "$DEMO_DIR/../lib/stack.sh"

export POLICY_FILE="$RUN_DIR/policy.cedar"
CEDAR_SCHEMA_FILE="$REPO_DIR/verifiers/cedar/policies/schema.cedarschema"
alias demo="$RUN_DIR/demo"

# start_verifier starts the Cedar verifier on $POLICY_FILE.
start_verifier() {
  (cd "$RUN_DIR" && TEMPORAL_ADDRESS="http://$TEMPORAL_ADDRESS" CEDAR_POLICY_FILE="$POLICY_FILE" \
    CEDAR_SCHEMA_FILE="$CEDAR_SCHEMA_FILE" bg cedar-verifier "$RUN_DIR/cedar-verifier")
}
