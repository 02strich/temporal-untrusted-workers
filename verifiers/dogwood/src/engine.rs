//! Decides `VerifyToolCallsRequest`s with Dogwood, over the history of what
//! each workflow run received.
//!
//! Every input event temporal-proxy records (`RecordInputEvents`) becomes a
//! `received` event in a durable Dogwood store, and every tool call a
//! `request` event, which Dogwood decides against the policies and the run's
//! history (see `policies/schema.cedarschema` and `policies/events.dwschema`
//! for the model). A request is approved only if every tool call is allowed.
//!
//! A denial normally answers `DECISION_DENIED`. If every `forbid` policy that
//! denied a call is annotated `@decision("escalate")`, the call is escalated
//! to human review instead (`DECISION_ESCALATE`). A denied call outweighs an
//! escalated one.
//!
//! A call no policy decided (no `permit` held and no `forbid` applied,
//! including a tool the schema doesn't declare) is denied, or escalated with
//! [`DefaultDecision::Escalate`]. A call whose evaluation failed is always
//! denied.

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Mutex;

use anyhow::{Context as _, anyhow, bail};
use dogwood_language::{Event, EventBuilder, Value as DwValue};
use dogwood_local_engine::{DecisionResponse, DurableLog, DurableTemporalEngine, Outcome, Verb};
use serde_json::{Map, Value};
use verifier_host::proto::{self, AnyData, Caller, Decision, InputEvents, Verdict, parse_any};

/// The namespace of the action schema's entities and actions.
const NAMESPACE: &str = "Temporal";

/// The event kind of a tool call: a decision.
const REQUEST: &str = "request";
/// The event kind of what a run received: history.
const RECEIVED: &str = "received";

/// Events between snapshots of the store's monitor state.
const SNAPSHOT_INTERVAL: u64 = 1000;

/// The annotation that turns a denying `forbid` policy into an escalation.
const DECISION_ANNOTATION: &str = "decision";
const ESCALATE: &str = "escalate";

/// How many of a run's latest received events are kept for logging.
const LOGGED_HISTORY: usize = 20;

/// What a tool call no policy decided gets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DefaultDecision {
    /// `DECISION_DENIED`, Dogwood's (and Cedar's) default deny.
    #[default]
    Deny,
    /// `DECISION_ESCALATE`: a human decides what no policy covers.
    Escalate,
}

impl FromStr for DefaultDecision {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        match s {
            "deny" => Ok(DefaultDecision::Deny),
            "escalate" => Ok(DefaultDecision::Escalate),
            _ => bail!("unknown default decision {s:?} (expected \"deny\" or \"escalate\")"),
        }
    }
}

/// A Dogwood store with the policies installed.
pub struct PolicyEngine {
    state: Mutex<State>,
    default_decision: DefaultDecision,
}

struct State {
    store: DurableTemporalEngine,
    runs: Runs,
}

impl PolicyEngine {
    /// Opens (or creates) the store at `path` and brings its policies in line
    /// with `policies`. The store keeps its history across restarts, and
    /// policies left unchanged keep theirs (see [`sync_policies`]). Fails if
    /// the policies don't validate against the schemas.
    pub fn open(
        path: &Path,
        policies: &str,
        schema: &str,
        event_schema: &str,
        max_runs: usize,
    ) -> anyhow::Result<Self> {
        let mut store = DurableTemporalEngine::open(path, SNAPSHOT_INTERVAL)
            .map_err(|e| anyhow!("opening Dogwood store {}: {e}", path.display()))?;
        sync_policies(&mut store, policies, schema, event_schema)?;
        Ok(Self {
            state: Mutex::new(State {
                store,
                runs: Runs::new(max_runs),
            }),
            default_decision: DefaultDecision::Deny,
        })
    }

    /// Sets what a tool call no policy decided gets (default: deny).
    pub fn with_default_decision(mut self, default_decision: DefaultDecision) -> Self {
        self.default_decision = default_decision;
        self
    }

    /// Decides a `VerifyToolCallsRequest`, given in the protobuf JSON mapping.
    /// Errors mean the input could not be interpreted, or the store failed.
    pub fn verify(&self, request: &Value) -> anyhow::Result<Verdict> {
        let caller = Caller::parse(request);
        let tool_calls = proto::tool_calls(request)?;
        // Parse every call before recording any of them.
        let calls = tool_calls
            .iter()
            .enumerate()
            .map(|(i, call)| {
                EventInput::new(&caller, &call.parameters).with_context(|| format!("tool call {i}"))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        let principal = entity("Worker", &caller.subject);
        let resource = entity("TaskQueue", &caller.task_queue);
        let mut state = self.state.lock().expect("dogwood state lock");
        let mut escalation = None;
        for (call, input) in tool_calls.iter().zip(calls) {
            let event = input
                .builder(&call.name, REQUEST)
                .principal(&principal)
                .resource(&resource);
            let submitted = state
                .store
                .submit(event)
                .map_err(|e| anyhow!("deciding tool {}: {e}", call.name))?;
            let Outcome::Decision(response) = submitted.outcome else {
                bail!(
                    "Dogwood recorded tool call {} without deciding it",
                    call.name
                );
            };
            for error in response.diagnostics().errors() {
                tracing::warn!(%error, tool = %call.name, "Dogwood evaluation error");
            }
            // One JSON line per evaluation, for showing what a policy saw
            // (e.g. in the demo): RUST_LOG=dogwood_request=debug.
            if tracing::enabled!(target: "dogwood_request", tracing::Level::DEBUG) {
                let line = serde_json::json!({
                    "workflowId": caller.workflow_id,
                    "principal": principal,
                    "action": action(&call.name),
                    "resource": resource,
                    "context": input.json,
                    "history": state.runs.history(&caller.run_key()),
                    "decision": if response.allowed() { "allow" } else { "deny" },
                    "policies": policy_names(&response),
                });
                tracing::debug!(target: "dogwood_request", "{line}");
            }
            if response.allowed() {
                continue;
            }
            let verdict = deny_verdict(&call.name, &response, self.default_decision);
            if verdict.decision == Decision::Denied {
                return Ok(verdict);
            }
            // Keep checking: a later denied call outweighs this escalation.
            escalation.get_or_insert(verdict);
        }
        Ok(escalation.unwrap_or_else(Verdict::approve))
    }

    /// Records a `RecordInputEventsRequest`'s events as the run's history.
    /// Events already recorded for the run (same event ID or update ID) are
    /// skipped, as long as the run is among the last `max_runs` seen. Returns
    /// the number of new events.
    pub fn record(&self, request: &Value) -> anyhow::Result<usize> {
        // Parse everything before recording, so a malformed request records
        // nothing.
        let request = InputEvents::parse(request)?;
        let mut events = Vec::with_capacity(request.events.len());
        for (i, event) in request.events.iter().enumerate() {
            let Some(name) = received_action(&event.kind, &event.name) else {
                tracing::debug!(kind = %event.kind, name = %event.name, "skipping input event of unknown kind");
                continue;
            };
            let input = EventInput::new(&request.caller, &event.payloads)
                .with_context(|| format!("input event {i}"))?;
            events.push((event, name, input));
        }

        let key = request.caller.run_key();
        let mut state = self.state.lock().expect("dogwood state lock");
        let mut added = 0;
        for (event, name, input) in events {
            let id = EventKey::of(event);
            if state.runs.seen(&key, &id) {
                continue;
            }
            state
                .store
                .submit(input.builder(&name, RECEIVED))
                .map_err(|e| anyhow!("recording {name}: {e}"))?;
            // Only once it's durably recorded: a failed batch is redelivered.
            state.runs.add(&key, id, &name, &input.json);
            added += 1;
        }
        Ok(added)
    }
}

/// Installs `policies` in a new store. In a store that has policies already,
/// it adds and deletes policies one by one instead: `install` would start
/// every policy over with an empty history, while a policy Dogwood keeps
/// keeps the history it accumulated (e.g. an approval signal recorded before
/// a restart).
fn sync_policies(
    store: &mut DurableTemporalEngine,
    policies: &str,
    schema: &str,
    event_schema: &str,
) -> anyhow::Result<()> {
    let install = |store: &mut DurableTemporalEngine| {
        store
            .install(policies, schema, Some(event_schema), None)
            .map(|_| ())
            .map_err(|e| anyhow!("installing Dogwood policies: {e}"))
    };
    if !store.has_policy() {
        return install(store);
    }
    if store.event_schema().as_deref() != Some(event_schema) {
        bail!(
            "the event schema differs from the one the Dogwood store {} was created with; \
             start with a new store to change it",
            store.path().display()
        );
    }

    // The policies' canonical statements, as the store would list them, from
    // a throwaway in-memory store (which validates them, too).
    let mut scratch = DurableTemporalEngine::open_with_log(
        DurableLog::ephemeral().map_err(|e| anyhow!("creating scratch store: {e}"))?,
        PathBuf::from("scratch"),
        0,
    )
    .map_err(|e| anyhow!("creating scratch store: {e}"))?;
    install(&mut scratch)?;
    let mut added: Vec<String> = scratch.list().into_iter().map(|p| p.statement).collect();

    // Keep what is unchanged, delete what is gone, add what is new; deletes
    // before a schema change, which revalidates the remaining policies.
    let mut verbs = Vec::new();
    for policy in store.list() {
        match added.iter().position(|s| *s == policy.statement) {
            Some(i) => {
                added.remove(i);
            }
            None => verbs.push(Verb::Delete { id: policy.token }),
        }
    }
    let deleted = verbs.len();
    if store.action_schema().as_deref() != Some(schema) {
        verbs.push(Verb::SetActionSchema {
            action_schema: schema.to_owned(),
        });
    }
    let new = added.len();
    verbs.extend(added.into_iter().map(|policy| Verb::Add { policy }));
    if !verbs.is_empty() {
        store
            .batch(verbs)
            .map_err(|e| anyhow!("updating Dogwood policies: {e}"))?;
        tracing::info!(added = new, deleted, "Dogwood policies updated");
    }
    Ok(())
}

/// The action a received input event is recorded as, or None for an unknown
/// kind.
fn received_action(kind: &str, name: &str) -> Option<String> {
    match kind {
        "workflow" => Some(format!("workflow/{name}")),
        "signal" => Some(format!("signal/{name}")),
        "update" => Some(format!("update/{name}")),
        // An activity or Nexus result is recorded under the tool's own name.
        "activity" | "nexus" => Some(name.to_owned()),
        _ => None,
    }
}

fn deny_verdict(tool: &str, response: &DecisionResponse, default: DefaultDecision) -> Verdict {
    let determining: Vec<_> = response.diagnostics().reason().collect();
    if determining.is_empty() {
        // A policy that failed to evaluate may have been the one deciding:
        // fail closed rather than treat the call as undecided.
        if let Some(error) = response.diagnostics().errors().next() {
            return Verdict {
                decision: Decision::Denied,
                reason: format!("tool {tool} denied: Dogwood policy evaluation failed: {error}"),
            };
        }
        return match default {
            DefaultDecision::Deny => Verdict {
                decision: Decision::Denied,
                reason: format!("tool {tool} not permitted by any Dogwood policy"),
            },
            DefaultDecision::Escalate => Verdict {
                decision: Decision::Escalate,
                reason: format!("tool {tool} not decided by any Dogwood policy"),
            },
        };
    }
    let escalate = determining
        .iter()
        .all(|p| p.annotation(DECISION_ANNOTATION) == Some(ESCALATE));
    let names = policy_names(response).join(", ");
    if escalate {
        Verdict {
            decision: Decision::Escalate,
            reason: format!("tool {tool} escalated by Dogwood policy {names}"),
        }
    } else {
        Verdict {
            decision: Decision::Denied,
            reason: format!("tool {tool} denied by Dogwood policy {names}"),
        }
    }
}

/// The policies that determined a decision, by `@id` (or the store's token
/// for a policy without one).
fn policy_names(response: &DecisionResponse) -> Vec<String> {
    response
        .diagnostics()
        .reason()
        .map(|p| {
            p.annotation_id()
                .map_or_else(|| p.token.to_string(), str::to_owned)
        })
        .collect()
}

/// `Temporal::Action::"<name>"`, for logging.
fn action(name: &str) -> String {
    entity("Action", name)
}

/// `Temporal::<type>::"<id>"`.
fn entity(entity_type: &str, id: &str) -> String {
    format!("{NAMESPACE}::{entity_type}::{}", Value::from(id))
}

/// An event's context, `{input, caller}`: as Dogwood values, and as JSON (for
/// logging what Dogwood evaluated).
struct EventInput {
    fields: Vec<(&'static str, String, DwValue)>,
    json: Value,
}

impl EventInput {
    /// The context of a tool call or input event of `caller`'s run with the
    /// given parameters (protobuf JSON Anys).
    fn new(caller: &Caller, parameters: &[Value]) -> anyhow::Result<Self> {
        let mut input = Map::new();
        input.insert("parameterCount".to_owned(), parameters.len().into());
        for (i, any) in parameters.iter().enumerate() {
            let value = parameter(any).with_context(|| format!("parameter {i}"))?;
            if let Some(value) = value {
                input.insert(format!("arg{i}"), value);
            }
        }
        let caller = serde_json::json!({
            "namespace": caller.namespace,
            "workflowId": caller.workflow_id,
            "run": caller.run_key(),
            "taskQueue": caller.task_queue,
        });
        let json = serde_json::json!({"input": input, "caller": caller});

        let mut fields = Vec::new();
        for group in ["input", "caller"] {
            for (name, value) in json[group].as_object().into_iter().flatten() {
                fields.push((group, name.clone(), to_dogwood(value)));
            }
        }
        Ok(Self { fields, json })
    }

    /// An event of `kind` for the action `name` with this context: both as
    /// the event's own fields (which temporal patterns match) and as its
    /// request context (which conditions read as `context.*`).
    fn builder(&self, name: &str, kind: &str) -> EventBuilder {
        let mut event = Event::builder(&format!("{NAMESPACE}::Action::{name}"), kind);
        for (group, name, value) in &self.fields {
            event =
                event
                    .field(group, name, value.clone())
                    .request_context(group, name, value.clone());
        }
        event
    }
}

/// A parameter (a protobuf JSON Any) as policies see it: a plain JSON value
/// (google.protobuf.Value and other well-known types) as itself, a message
/// as a record of its fields, a payload the proxy couldn't interpret as an
/// empty record. None if nothing of it can be represented.
fn parameter(any: &Value) -> anyhow::Result<Option<Value>> {
    let value = match parse_any(any)?.1 {
        AnyData::Opaque => Value::Object(Map::new()),
        AnyData::Value(value) => value,
        AnyData::Fields(fields) => Value::Object(fields),
    };
    Ok(representable(&value))
}

/// The part of a JSON value Dogwood (that is, Cedar) can represent: floats
/// and nulls have no equivalent and are dropped.
fn representable(value: &Value) -> Option<Value> {
    match value {
        Value::Null => None,
        Value::Number(n) => n.as_i64().map(Value::from),
        Value::Array(items) => Some(Value::Array(
            items.iter().filter_map(representable).collect(),
        )),
        Value::Object(fields) => Some(Value::Object(
            fields
                .iter()
                .filter_map(|(k, v)| representable(v).map(|v| (k.clone(), v)))
                .collect(),
        )),
        Value::Bool(_) | Value::String(_) => Some(value.clone()),
    }
}

/// Converts a representable JSON value (see [`representable`]); arrays
/// become sets.
fn to_dogwood(value: &Value) -> DwValue {
    match value {
        Value::Null => DwValue::Null,
        Value::Bool(b) => DwValue::Bool(*b),
        Value::Number(n) => n.as_i64().map_or(DwValue::Null, DwValue::Int),
        Value::String(s) => DwValue::String(s.clone()),
        Value::Array(items) => DwValue::Array(items.iter().map(to_dogwood).collect()),
        Value::Object(fields) => DwValue::Object(
            fields
                .iter()
                .map(|(k, v)| (k.clone(), to_dogwood(v)))
                .collect(),
        ),
    }
}

/// Identifies an input event within its run.
#[derive(PartialEq, Eq, Hash)]
enum EventKey {
    Update(String),
    Event(i64),
    /// No ID: never deduplicated.
    None,
}

impl EventKey {
    fn of(event: &proto::InputEvent) -> Self {
        match (&event.update_id, event.event_id) {
            (Some(update_id), _) => EventKey::Update(update_id.clone()),
            (None, Some(event_id)) => EventKey::Event(event_id),
            (None, None) => EventKey::None,
        }
    }
}

/// What the verifier remembers of each recent run: the input events already
/// recorded, and the latest ones for logging. The store itself has the full
/// history; this is only to avoid recording redelivered events twice.
struct Runs {
    max_runs: usize,
    runs: HashMap<String, RunLog>,
    // Insertion order, for evicting the oldest run when full.
    order: VecDeque<String>,
}

#[derive(Default)]
struct RunLog {
    seen: HashSet<EventKey>,
    history: VecDeque<Value>,
}

impl Runs {
    fn new(max_runs: usize) -> Self {
        Self {
            max_runs: max_runs.max(1),
            runs: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    fn seen(&self, run: &str, id: &EventKey) -> bool {
        *id != EventKey::None && self.runs.get(run).is_some_and(|r| r.seen.contains(id))
    }

    fn add(&mut self, run: &str, id: EventKey, action: &str, context: &Value) {
        if !self.runs.contains_key(run) {
            while self.runs.len() >= self.max_runs {
                let Some(oldest) = self.order.pop_front() else {
                    break;
                };
                self.runs.remove(&oldest);
            }
            self.order.push_back(run.to_owned());
        }
        let log = self.runs.entry(run.to_owned()).or_default();
        if id != EventKey::None {
            log.seen.insert(id);
        }
        if log.history.len() == LOGGED_HISTORY {
            log.history.pop_front();
        }
        log.history.push_back(
            serde_json::json!({"action": self::action(action), "input": context["input"]}),
        );
    }

    /// The run's latest received events, oldest first.
    fn history(&self, run: &str) -> Vec<Value> {
        self.runs
            .get(run)
            .map(|r| r.history.iter().cloned().collect())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const EXAMPLE_POLICY: &str = include_str!("../policies/example.dw");
    const SCHEMA: &str = include_str!("../policies/schema.cedarschema");
    const EVENT_SCHEMA: &str = include_str!("../policies/events.dwschema");

    /// A store file removed when dropped.
    struct TempStore(PathBuf);

    impl TempStore {
        fn new() -> Self {
            static NEXT: AtomicUsize = AtomicUsize::new(0);
            Self(std::env::temp_dir().join(format!(
                "dogwood-verifier-test-{}-{}.redb",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            )))
        }

        fn open(&self, policies: &str) -> PolicyEngine {
            PolicyEngine::open(&self.0, policies, SCHEMA, EVENT_SCHEMA, 100)
                .expect("policies validate against the schemas")
        }

        fn open_escalating(&self, policies: &str) -> PolicyEngine {
            self.open(policies)
                .with_default_decision(DefaultDecision::Escalate)
        }
    }

    impl Drop for TempStore {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn caller(run: &str) -> Value {
        json!({
            "namespace": "ns-a",
            "subject": "fleet-a",
            "taskQueue": "queue-a",
            "workflowId": "billing-42",
            "runId": run,
        })
    }

    fn request(run: &str, tool_calls: Value) -> Value {
        json!({"caller": caller(run), "toolCalls": tool_calls})
    }

    fn value_param(value: Value) -> Value {
        json!({"@type": "type.googleapis.com/google.protobuf.Value", "value": value})
    }

    fn call(name: &str, parameters: Vec<Value>) -> Value {
        json!({"name": name, "parameters": parameters})
    }

    fn charge(amount: Value, currency: &str) -> Value {
        call(
            "payments/billing/Charge",
            vec![value_param(json!({"amount": amount, "currency": currency}))],
        )
    }

    fn verify(engine: &PolicyEngine, tool_calls: Value) -> Verdict {
        engine.verify(&request("r1", tool_calls)).unwrap()
    }

    fn assert_decision(engine: &PolicyEngine, tool_calls: Value, decision: Decision, reason: &str) {
        let verdict = verify(engine, tool_calls);
        assert_eq!(verdict.decision, decision, "{verdict:?}");
        assert!(verdict.reason.contains(reason), "{verdict:?}");
    }

    fn record(engine: &PolicyEngine, run: &str, events: Value) -> usize {
        engine
            .record(&json!({"caller": caller(run), "fromHistoryStart": true, "events": events}))
            .unwrap()
    }

    fn workflow_input(event_id: i64, input: Value) -> Value {
        json!({"kind": "INPUT_EVENT_KIND_WORKFLOW_INPUT", "name": "ToolWorkflow",
               "eventId": event_id.to_string(), "payloads": [value_param(input)]})
    }

    fn approval(event_id: i64, refund_id: &str) -> Value {
        json!({"kind": "INPUT_EVENT_KIND_SIGNAL", "name": "approval", "eventId": event_id.to_string(),
               "payloads": [value_param(json!({"refundId": refund_id, "approvedBy": "manager@example.com"}))]})
    }

    #[test]
    fn echo_is_allowed() {
        let store = TempStore::new();
        let engine = store.open(EXAMPLE_POLICY);
        assert!(
            verify(
                &engine,
                json!([call("echo/echo/Echo", vec![value_param(json!("hi"))])])
            )
            .allowed()
        );
    }

    #[test]
    fn charge_limits() {
        let store = TempStore::new();
        let engine = store.open(EXAMPLE_POLICY);
        assert!(verify(&engine, json!([charge(json!(500), "USD")])).allowed());
        assert!(verify(&engine, json!([charge(json!(1000), "EUR")])).allowed());
        assert_decision(
            &engine,
            json!([charge(json!(5000), "USD")]),
            Decision::Escalate,
            "tool payments/billing/Charge escalated by Dogwood policy charge-over-1000-needs-review",
        );
        assert_decision(
            &engine,
            json!([charge(json!(50000), "USD")]),
            Decision::Denied,
            "charge-limit-10000",
        );
        assert_decision(
            &engine,
            json!([charge(json!(500), "GBP")]),
            Decision::Denied,
            "not permitted by any Dogwood policy",
        );
        // A float amount is dropped, so the charge has no amount.
        assert_decision(
            &engine,
            json!([charge(json!(1.5), "USD")]),
            Decision::Denied,
            "charge-limit-10000",
        );
        // Without parameters, nothing permits it.
        assert_decision(
            &engine,
            json!([call("payments/billing/Charge", vec![])]),
            Decision::Denied,
            "denied by Dogwood policy",
        );
    }

    #[test]
    fn undeclared_tool_is_denied() {
        let store = TempStore::new();
        let engine = store.open(EXAMPLE_POLICY);
        assert_decision(
            &engine,
            json!([call("payments/billing/Refund", vec![])]),
            Decision::Denied,
            "tool payments/billing/Refund not permitted by any Dogwood policy",
        );
    }

    #[test]
    fn denial_outweighs_escalation() {
        let store = TempStore::new();
        let engine = store.open(EXAMPLE_POLICY);
        assert_decision(
            &engine,
            json!([charge(json!(5000), "USD"), call("nope/Nope", vec![])]),
            Decision::Denied,
            "tool nope/Nope",
        );
        assert_decision(
            &engine,
            json!([charge(json!(5000), "USD"), call("echo/echo/Echo", vec![])]),
            Decision::Escalate,
            "escalated",
        );
    }

    #[test]
    fn no_tool_calls_is_approved() {
        let store = TempStore::new();
        let engine = store.open(EXAMPLE_POLICY);
        assert!(verify(&engine, json!([])).allowed());
        assert!(
            engine
                .verify(&json!({"caller": caller("r1")}))
                .unwrap()
                .allowed()
        );
    }

    #[test]
    fn send_email_only_to_the_customer() {
        let store = TempStore::new();
        let engine = store.open(EXAMPLE_POLICY);
        let send = |to: &str| {
            json!([call(
                "trusted-tools/SendEmail",
                vec![value_param(json!(to))]
            )])
        };

        // Nothing recorded yet.
        assert!(!verify(&engine, send("alice@example.com")).allowed());

        let input =
            json!({"case": "email-ok", "customerEmail": "alice@example.com", "refundId": "R-1001"});
        assert_eq!(record(&engine, "r1", json!([workflow_input(1, input)])), 1);
        assert!(verify(&engine, send("alice@example.com")).allowed());
        assert_decision(
            &engine,
            send("mallory@evil.com"),
            Decision::Denied,
            "tool trusted-tools/SendEmail not permitted by any Dogwood policy",
        );
        // Another run of the same workflow doesn't see r1's input.
        assert!(
            !engine
                .verify(&request("r2", send("alice@example.com")))
                .unwrap()
                .allowed()
        );
    }

    #[test]
    fn refund_needs_matching_approval_signal() {
        let store = TempStore::new();
        let engine = store.open(EXAMPLE_POLICY);
        let refund = || {
            json!([call(
                "trusted-tools/IssueRefund",
                vec![value_param(json!("R-1001"))]
            )])
        };

        // The refund ID in the workflow's own input doesn't count.
        record(
            &engine,
            "r1",
            json!([workflow_input(1, json!({"refundId": "R-1001"}))]),
        );
        assert!(!verify(&engine, refund()).allowed());

        record(&engine, "r1", json!([approval(5, "R-2002")]));
        assert!(!verify(&engine, refund()).allowed());

        record(&engine, "r1", json!([approval(9, "R-1001")]));
        assert!(verify(&engine, refund()).allowed());
    }

    #[test]
    fn records_each_event_once() {
        let store = TempStore::new();
        let engine = store.open(EXAMPLE_POLICY);
        let events = json!([
            workflow_input(1, json!({"customerEmail": "alice@example.com"})),
            approval(5, "R-1001"),
            {"kind": "INPUT_EVENT_KIND_UPDATE", "name": "chat", "updateId": "u-1",
             "payloads": [value_param(json!("hello"))]},
            {"kind": 2, "name": "crm/LookupCustomer", "eventId": 12,
             "payloads": [{"@type": "type.googleapis.com/acme.v1.Customer", "email": "bob@example.com"}]},
            {"kind": "INPUT_EVENT_KIND_UNSPECIFIED", "name": "?", "eventId": "13"},
        ]);
        assert_eq!(record(&engine, "r1", events.clone()), 4);
        assert_eq!(
            record(&engine, "r1", events),
            0,
            "re-sent events are skipped"
        );
        assert_eq!(record(&engine, "r2", json!([approval(5, "R-1001")])), 1);
    }

    #[test]
    fn malformed_input_records_nothing() {
        let store = TempStore::new();
        let engine = store.open(EXAMPLE_POLICY);
        let bad = json!({"caller": caller("r1"), "events": [
            workflow_input(1, json!({"customerEmail": "alice@example.com"})),
            {"kind": "INPUT_EVENT_KIND_SIGNAL", "name": "bad", "eventId": "3", "payloads": [{"no": "type"}]},
        ]});
        assert!(engine.record(&bad).is_err());
        let send = json!([call(
            "trusted-tools/SendEmail",
            vec![value_param(json!("alice@example.com"))]
        )]);
        assert!(!verify(&engine, send).allowed());

        assert!(engine.verify(&json!({"toolCalls": {}})).is_err());
        assert!(
            engine
                .verify(&json!({"toolCalls": [{"parameters": []}]}))
                .is_err()
        );
        assert!(
            engine
                .verify(&request(
                    "r1",
                    json!([call("echo/echo/Echo", vec![json!({"no": "type"})])])
                ))
                .is_err()
        );
    }

    #[test]
    fn history_survives_restart_and_policy_change() {
        let store = TempStore::new();
        {
            let engine = store.open(EXAMPLE_POLICY);
            record(&engine, "r1", json!([approval(5, "R-1001")]));
            assert_eq!(
                verify(&engine, json!([charge(json!(5000), "USD")])).decision,
                Decision::Escalate
            );
        }
        // Reopen with the review threshold raised, as the demo does.
        let raised = EXAMPLE_POLICY.replace("amount <= 1000\n", "amount <= 10000\n");
        assert_ne!(raised, EXAMPLE_POLICY);
        let engine = store.open(&raised);
        assert!(verify(&engine, json!([charge(json!(5000), "USD")])).allowed());
        let refund = json!([call(
            "trusted-tools/IssueRefund",
            vec![value_param(json!("R-1001"))]
        )]);
        assert!(
            verify(&engine, refund).allowed(),
            "the approval is still in the history"
        );
        drop(engine);

        // A policy removed from the file is removed from the store.
        let without_echo = raised.replace(
            "@id(\"allow-echo\")\npermit (principal, action == Temporal::Action::\"echo/echo/Echo\", resource);\n",
            "",
        );
        assert_ne!(without_echo, raised);
        let engine = store.open(&without_echo);
        assert!(!verify(&engine, json!([call("echo/echo/Echo", vec![])])).allowed());
    }

    #[test]
    fn undecided_calls_escalate_when_configured() {
        let store = TempStore::new();
        let engine = store.open_escalating(EXAMPLE_POLICY);
        // Permitted calls are still allowed, and forbids still decide.
        assert!(verify(&engine, json!([charge(json!(500), "USD")])).allowed());
        assert_decision(
            &engine,
            json!([charge(json!(5000), "USD")]),
            Decision::Escalate,
            "escalated by Dogwood policy charge-over-1000-needs-review",
        );
        assert_decision(
            &engine,
            json!([charge(json!(50000), "USD")]),
            Decision::Denied,
            "denied by Dogwood policy charge-limit-10000",
        );
        // No permit holds: a currency outside USD and EUR, an email to an
        // address the run never received, an undeclared tool.
        assert_decision(
            &engine,
            json!([charge(json!(500), "GBP")]),
            Decision::Escalate,
            "tool payments/billing/Charge not decided by any Dogwood policy",
        );
        let send = json!([call(
            "trusted-tools/SendEmail",
            vec![value_param(json!("mallory@evil.com"))]
        )]);
        assert_decision(
            &engine,
            send,
            Decision::Escalate,
            "not decided by any Dogwood policy",
        );
        assert_decision(
            &engine,
            json!([call("payments/billing/Refund", vec![])]),
            Decision::Escalate,
            "tool payments/billing/Refund not decided by any Dogwood policy",
        );
        // A call denied outright still outweighs an escalated one.
        assert_decision(
            &engine,
            json!([
                call("payments/billing/Refund", vec![]),
                charge(json!(50000), "USD")
            ]),
            Decision::Denied,
            "charge-limit-10000",
        );
    }

    #[test]
    fn evaluation_errors_deny_even_when_escalating() {
        let store = TempStore::new();
        // SendEmail's arg0 is declared a String; a record makes `like` fail.
        let policies = format!(
            "{EXAMPLE_POLICY}\n@id(\"email-example-com\")\n\
             permit (principal, action == Temporal::Action::\"trusted-tools/SendEmail\", resource)\n\
             when {{ context.input has arg0 && context.input.arg0 like \"*@example.com\" }};\n"
        );
        let engine = store.open_escalating(&policies);
        let send = |arg: Value| json!([call("trusted-tools/SendEmail", vec![value_param(arg)])]);
        assert!(verify(&engine, send(json!("bob@example.com"))).allowed());
        assert_decision(
            &engine,
            send(json!({"to": "bob@example.com"})),
            Decision::Denied,
            "tool trusted-tools/SendEmail denied: Dogwood policy evaluation failed",
        );
    }

    #[test]
    fn default_decision_from_str() {
        assert_eq!(
            "deny".parse::<DefaultDecision>().unwrap(),
            DefaultDecision::Deny
        );
        assert_eq!(
            "escalate".parse::<DefaultDecision>().unwrap(),
            DefaultDecision::Escalate
        );
        assert!("allow".parse::<DefaultDecision>().is_err());
        assert_eq!(DefaultDecision::default(), DefaultDecision::Deny);
    }

    #[test]
    fn invalid_policy_is_rejected() {
        let store = TempStore::new();
        let bad = r#"permit (principal, action == Temporal::Action::"echo/echo/Echo", resource)
                     when { context.input.nope == 1 };"#;
        assert!(PolicyEngine::open(&store.0, bad, SCHEMA, EVENT_SCHEMA, 10).is_err());
    }

    #[test]
    fn event_context() {
        let caller = Caller::parse(&json!({"caller": caller("r1")}));
        let input = EventInput::new(
            &caller,
            &[
                value_param(json!({"amount": 500, "currency": "USD", "ratio": 0.5, "note": null})),
                json!({"@type": "type.googleapis.com/temporal.api.common.v1.Payload", "data": "xyz"}),
                value_param(json!(null)),
            ],
        )
        .unwrap();
        assert_eq!(
            input.json,
            json!({
                "input": {
                    "parameterCount": 3,
                    "arg0": {"amount": 500, "currency": "USD"},
                    "arg1": {},
                },
                "caller": {
                    "namespace": "ns-a",
                    "workflowId": "billing-42",
                    "run": "ns-a/billing-42/r1",
                    "taskQueue": "queue-a",
                },
            })
        );
        assert_eq!(entity("Worker", "a\"b"), r#"Temporal::Worker::"a\"b""#);
    }
}
