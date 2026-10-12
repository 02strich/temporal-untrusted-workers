//! A temporal-proxy tool-call verifier (TEMPORAL_PROXY_TOOL_VERIFIER=nexus)
//! whose rules are Cedar policies.
//!
//! [`verifier_host`] runs the Nexus worker; each `VerifyToolCalls` request is
//! evaluated with [`policy::PolicyEngine`], with the input events
//! [`inputs::InputStore`] recorded for the calling run as context.

mod inputs;
mod policy;

use std::env;

use anyhow::Context as _;
use verifier_host::{Config, Verdict, Verifier, env_or, proto::Caller};

use crate::inputs::InputStore;
use crate::policy::PolicyEngine;

struct CedarVerifier {
    engine: PolicyEngine,
    inputs: InputStore,
}

impl Verifier for CedarVerifier {
    fn verify(&self, request: &serde_json::Value) -> anyhow::Result<Verdict> {
        let inputs = self.inputs.context_for(&Caller::parse(request));
        self.engine.verify(request, &inputs)
    }

    fn record(&self, request: &serde_json::Value) -> anyhow::Result<usize> {
        self.inputs.record(request)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    verifier_host::init_tracing();

    let cfg = Config::from_env();
    let policy_file = env_or("CEDAR_POLICY_FILE", "policies/example.cedar");
    let schema_file = env::var("CEDAR_SCHEMA_FILE").ok().filter(|f| !f.is_empty());

    let policies = std::fs::read_to_string(&policy_file)
        .with_context(|| format!("reading CEDAR_POLICY_FILE {policy_file}"))?;
    let schema = schema_file
        .as_ref()
        .map(|f| {
            std::fs::read_to_string(f).with_context(|| format!("reading CEDAR_SCHEMA_FILE {f}"))
        })
        .transpose()?;
    let engine = PolicyEngine::new(&policies, schema.as_deref())?;
    tracing::info!(
        policy_file = %policy_file,
        schema_validated = schema_file.is_some(),
        "Cedar policies loaded"
    );

    let verifier = CedarVerifier {
        engine,
        inputs: InputStore::new(cfg.input_max_runs),
    };
    verifier_host::run(cfg, "cedar-verifier", verifier).await
}
