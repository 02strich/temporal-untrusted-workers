//! A temporal-proxy tool-call verifier (TEMPORAL_PROXY_TOOL_VERIFIER=nexus)
//! whose rules are Dogwood policies: Cedar, plus temporal conditions over
//! what each workflow run has received.
//!
//! [`verifier_host`] runs the Nexus worker; [`engine::PolicyEngine`] records
//! input events in a durable Dogwood store and decides tool calls.

mod engine;

use std::path::PathBuf;

use anyhow::Context as _;
use verifier_host::{Config, Verdict, Verifier, env_or};

use crate::engine::{DefaultDecision, PolicyEngine};

impl Verifier for PolicyEngine {
    fn verify(&self, request: &serde_json::Value) -> anyhow::Result<Verdict> {
        PolicyEngine::verify(self, request)
    }

    fn record(&self, request: &serde_json::Value) -> anyhow::Result<usize> {
        PolicyEngine::record(self, request)
    }
}

fn read(var: &str, default: &str) -> anyhow::Result<(String, String)> {
    let file = env_or(var, default);
    let content =
        std::fs::read_to_string(&file).with_context(|| format!("reading {var} {file}"))?;
    Ok((file, content))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    verifier_host::init_tracing();

    let cfg = Config::from_env();
    let (policy_file, policies) = read("DOGWOOD_POLICY_FILE", "policies/example.dw")?;
    let (_, schema) = read("DOGWOOD_SCHEMA_FILE", "policies/schema.cedarschema")?;
    let (_, event_schema) = read("DOGWOOD_EVENT_SCHEMA_FILE", "policies/events.dwschema")?;
    let store = PathBuf::from(env_or("DOGWOOD_STORE", "dogwood.redb"));
    let default_decision: DefaultDecision = env_or("DOGWOOD_DEFAULT_DECISION", "deny")
        .parse()
        .context("DOGWOOD_DEFAULT_DECISION")?;

    let engine = PolicyEngine::open(
        &store,
        &policies,
        &schema,
        &event_schema,
        cfg.input_max_runs,
    )?
    .with_default_decision(default_decision);
    tracing::info!(
        policy_file = %policy_file,
        store = %store.display(),
        ?default_decision,
        "Dogwood policies installed"
    );
    verifier_host::run(cfg, "dogwood-verifier", engine).await
}
