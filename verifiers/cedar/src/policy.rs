//! Evaluates a `VerifyToolCallsRequest` against Cedar policies.
//!
//! Every tool call becomes one Cedar authorization request (see
//! `policies/schema.cedarschema` for the model); the request is approved only
//! if every tool call is allowed.
//!
//! A denial normally answers `DECISION_DENIED`. If every `forbid` policy that
//! denied a call is annotated `@decision("escalate")`, the call is escalated
//! to human review instead (`DECISION_ESCALATE`). A denied call outweighs an
//! escalated one.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::str::FromStr;

use anyhow::{Context as _, anyhow, bail};
use cedar_policy::{
    Authorizer, Context, Decision as CedarDecision, Entities, Entity, EntityId, EntityTypeName,
    EntityUid, PolicySet, Request, RestrictedExpression, Schema, ValidationMode, Validator,
};
use serde_json::{Map, Value};
use verifier_host::proto::{self, AnyData, Caller, Decision, Verdict, parse_any};

/// One recorded input event: its name, how many events of that type and
/// name the run received, and the latest one's payloads (protobuf JSON Anys).
#[derive(Debug, Default, Clone)]
pub struct RecordedEvent {
    pub name: String,
    pub count: u64,
    pub payloads: Vec<Value>,
}

/// The context a workflow run has received, as the policies see it:
/// `context.workflowInput`, `context.events` and `context.inputEventsComplete`.
#[derive(Debug, Default, Clone)]
pub struct InputContext {
    /// The workflow's start input, if recorded.
    pub workflow_input: Option<RecordedEvent>,
    /// The latest event of each name, by type (activity, nexus, signal,
    /// update).
    pub events: BTreeMap<String, BTreeMap<String, RecordedEvent>>,
    /// Whether the verifier has the run's context from its start.
    pub complete: bool,
}

/// The event types in `context.events`; each is always present.
const EVENT_TYPES: [&str; 4] = ["activity", "nexus", "signal", "update"];

/// The annotation that turns a denying `forbid` policy into an escalation.
const DECISION_ANNOTATION: &str = "decision";
const ESCALATE: &str = "escalate";

/// A loaded, optionally schema-validated, Cedar policy set.
pub struct PolicyEngine {
    policies: PolicySet,
    authorizer: Authorizer,
}

impl PolicyEngine {
    /// Parses `policies`, validating them against `schema` when given.
    pub fn new(policies: &str, schema: Option<&str>) -> anyhow::Result<Self> {
        let policies =
            PolicySet::from_str(policies).map_err(|e| anyhow!("parsing Cedar policies: {e}"))?;
        if let Some(schema) = schema {
            let (schema, warnings) = Schema::from_cedarschema_str(schema)
                .map_err(|e| anyhow!("parsing Cedar schema: {e}"))?;
            for warning in warnings {
                tracing::warn!(%warning, "Cedar schema warning");
            }
            let result = Validator::new(schema).validate(&policies, ValidationMode::Strict);
            if !result.validation_passed() {
                let errors: Vec<String> =
                    result.validation_errors().map(|e| e.to_string()).collect();
                bail!(
                    "Cedar policies do not validate against the schema: {}",
                    errors.join("; ")
                );
            }
            for warning in result.validation_warnings() {
                tracing::warn!(%warning, "Cedar policy validation warning");
            }
        }
        Ok(Self {
            policies,
            authorizer: Authorizer::new(),
        })
    }

    /// Decides a `VerifyToolCallsRequest`, given in the protobuf JSON mapping.
    /// Errors mean the input could not be interpreted.
    pub fn verify(&self, request: &Value, inputs: &InputContext) -> anyhow::Result<Verdict> {
        let Caller {
            namespace,
            subject,
            task_queue,
            workflow_id,
            ..
        } = Caller::parse(request);
        let tool_calls = proto::tool_calls(request)?;

        let principal = uid("Worker", &subject)?;
        let resource = uid("TaskQueue", &task_queue)?;
        let entities = Entities::from_entities(
            [
                Entity::new(
                    principal.clone(),
                    HashMap::from([(
                        "namespace".to_owned(),
                        RestrictedExpression::new_string(namespace.clone()),
                    )]),
                    HashSet::new(),
                )?,
                Entity::new(
                    resource.clone(),
                    HashMap::from([
                        (
                            "name".to_owned(),
                            RestrictedExpression::new_string(task_queue),
                        ),
                        (
                            "namespace".to_owned(),
                            RestrictedExpression::new_string(namespace),
                        ),
                    ]),
                    HashSet::new(),
                )?,
            ],
            None,
        )?;

        let mut escalation = None;
        for (i, call) in tool_calls.iter().enumerate() {
            let call = ToolCall::parse(call).with_context(|| format!("tool call {i}"))?;
            let (context, context_json) = call.context(&workflow_id, inputs)?;
            let request = Request::new(
                principal.clone(),
                uid("Action", &call.name)?,
                resource.clone(),
                context,
                None,
            )?;
            let response = self
                .authorizer
                .is_authorized(&request, &self.policies, &entities);
            for error in response.diagnostics().errors() {
                tracing::warn!(%error, tool = %call.name, "Cedar evaluation error");
            }
            // One JSON line per evaluation, for showing what a policy saw
            // (e.g. in the demo): RUST_LOG=cedar_request=debug.
            if tracing::enabled!(target: "cedar_request", tracing::Level::DEBUG) {
                let decision = if response.decision() == CedarDecision::Allow {
                    "allow"
                } else {
                    "deny"
                };
                // The policies that determined the decision, by @id.
                let policies: Vec<&str> = response
                    .diagnostics()
                    .reason()
                    .map(|id| {
                        self.policies
                            .annotation(id, "id")
                            .unwrap_or_else(|| id.as_ref())
                    })
                    .collect();
                let line = serde_json::json!({
                    "workflowId": workflow_id,
                    "principal": principal.to_string(),
                    "action": uid("Action", &call.name)?.to_string(),
                    "resource": resource.to_string(),
                    "context": context_json,
                    "decision": decision,
                    "policies": policies,
                });
                tracing::debug!(target: "cedar_request", "{line}");
            }
            if response.decision() == CedarDecision::Allow {
                continue;
            }
            let verdict = self.deny_verdict(&call.name, &response);
            if verdict.decision == Decision::Denied {
                return Ok(verdict);
            }
            // Keep checking: a later denied call outweighs this escalation.
            escalation.get_or_insert(verdict);
        }
        Ok(escalation.unwrap_or_else(Verdict::approve))
    }

    fn deny_verdict(&self, tool: &str, response: &cedar_policy::Response) -> Verdict {
        let determining: Vec<_> = response.diagnostics().reason().collect();
        if determining.is_empty() {
            return Verdict {
                decision: Decision::Denied,
                reason: format!("tool {tool} not permitted by any Cedar policy"),
            };
        }
        let escalate = determining
            .iter()
            .all(|id| self.policies.annotation(id, DECISION_ANNOTATION) == Some(ESCALATE));
        let names: Vec<&str> = determining
            .iter()
            .map(|id| {
                self.policies
                    .annotation(id, "id")
                    .unwrap_or_else(|| id.as_ref())
            })
            .collect();
        if escalate {
            Verdict {
                decision: Decision::Escalate,
                reason: format!("tool {tool} escalated by Cedar policy {}", names.join(", ")),
            }
        } else {
            Verdict {
                decision: Decision::Denied,
                reason: format!("tool {tool} denied by Cedar policy {}", names.join(", ")),
            }
        }
    }
}

/// A tool call as the policies see it.
#[derive(Debug)]
struct ToolCall {
    name: String,
    parameters: Vec<Parameter>,
}

/// One parameter: its protobuf type and its data as a Cedar record.
#[derive(Debug)]
struct Parameter {
    type_name: String,
    data: Vec<(String, RestrictedExpression)>,
    /// `data` as JSON, for logging what Cedar evaluated.
    data_json: Map<String, Value>,
}

impl ToolCall {
    fn parse(call: &proto::ToolCall) -> anyhow::Result<Self> {
        let parameters = call
            .parameters
            .iter()
            .enumerate()
            .map(|(i, p)| Parameter::parse(p).with_context(|| format!("parameter {i}")))
            .collect::<anyhow::Result<_>>()?;
        Ok(Self {
            name: call.name.clone(),
            parameters,
        })
    }

    /// The Cedar context, and the same as JSON (for logging what Cedar
    /// evaluated).
    fn context(
        &self,
        workflow_id: &str,
        inputs: &InputContext,
    ) -> anyhow::Result<(Context, Value)> {
        let (parameters, parameters_json) = parameters_record(&self.parameters)?;

        let (workflow_input, workflow_input_json) = match &inputs.workflow_input {
            Some(input) => event_record(None, input)?,
            None => event_record(None, &RecordedEvent::default())?,
        };

        let mut events = Vec::with_capacity(EVENT_TYPES.len());
        let mut events_json = Map::new();
        for event_type in EVENT_TYPES {
            let mut by_name = Vec::new();
            let mut by_name_json = Map::new();
            for (name, event) in inputs.events.get(event_type).into_iter().flatten() {
                let (record, json) = event_record(Some(event_type), event)?;
                by_name.push((name.clone(), record));
                by_name_json.insert(name.clone(), json);
            }
            events.push((
                event_type.to_owned(),
                RestrictedExpression::new_record(by_name)?,
            ));
            events_json.insert(event_type.to_owned(), Value::Object(by_name_json));
        }

        let context = Context::from_pairs([
            (
                "workflowId".to_owned(),
                RestrictedExpression::new_string(workflow_id.to_owned()),
            ),
            (
                "parameterCount".to_owned(),
                RestrictedExpression::new_long(self.parameters.len() as i64),
            ),
            ("parameters".to_owned(), parameters),
            ("workflowInput".to_owned(), workflow_input),
            (
                "events".to_owned(),
                RestrictedExpression::new_record(events)?,
            ),
            (
                "inputEventsComplete".to_owned(),
                RestrictedExpression::new_bool(inputs.complete),
            ),
        ])?;
        let json = serde_json::json!({
            "workflowId": workflow_id,
            "parameterCount": self.parameters.len(),
            "parameters": parameters_json,
            "workflowInput": workflow_input_json,
            "events": events_json,
            "inputEventsComplete": inputs.complete,
        });
        Ok((context, json))
    }
}

/// `{arg0: {type, data}, arg1: …}` for a list of parameters (a tool call's,
/// or an input event's payloads), as a Cedar record and as JSON.
fn parameters_record(parameters: &[Parameter]) -> anyhow::Result<(RestrictedExpression, Value)> {
    let mut record = Vec::with_capacity(parameters.len());
    let mut json = Map::new();
    for (i, param) in parameters.iter().enumerate() {
        record.push((
            format!("arg{i}"),
            RestrictedExpression::new_record([
                (
                    "type".to_owned(),
                    RestrictedExpression::new_string(param.type_name.clone()),
                ),
                (
                    "data".to_owned(),
                    RestrictedExpression::new_record(param.data.clone())?,
                ),
            ])?,
        ));
        json.insert(
            format!("arg{i}"),
            serde_json::json!({"type": param.type_name, "data": param.data_json}),
        );
    }
    Ok((
        RestrictedExpression::new_record(record)?,
        Value::Object(json),
    ))
}

/// An input event entry: `{type, name, count, parameters}` for events, or
/// `{name, parameters}` for the workflow input (`event_type` None).
fn event_record(
    event_type: Option<&str>,
    event: &RecordedEvent,
) -> anyhow::Result<(RestrictedExpression, Value)> {
    let parameters = event
        .payloads
        .iter()
        .map(Parameter::parse)
        .collect::<anyhow::Result<Vec<_>>>()?;
    let (parameters, parameters_json) = parameters_record(&parameters)?;
    let mut fields = vec![
        (
            "name".to_owned(),
            RestrictedExpression::new_string(event.name.clone()),
        ),
        ("parameters".to_owned(), parameters),
    ];
    let mut json = serde_json::json!({"name": event.name, "parameters": parameters_json});
    if let Some(event_type) = event_type {
        fields.push((
            "type".to_owned(),
            RestrictedExpression::new_string(event_type.to_owned()),
        ));
        fields.push((
            "count".to_owned(),
            RestrictedExpression::new_long(event.count as i64),
        ));
        json = serde_json::json!({
            "type": event_type,
            "name": event.name,
            "count": event.count,
            "parameters": parameters_json,
        });
    }
    Ok((RestrictedExpression::new_record(fields)?, json))
}

impl Parameter {
    /// Interprets the protobuf JSON form of a google.protobuf.Any.
    fn parse(any: &Value) -> anyhow::Result<Self> {
        let (type_name, data) = parse_any(any)?;
        // Keep only what Cedar can represent, so the logged JSON matches the
        // record the policies see.
        let data_json: Map<String, Value> = match data {
            AnyData::Opaque => Map::new(),
            AnyData::Value(Value::Object(fields)) | AnyData::Fields(fields) => fields
                .iter()
                .filter_map(|(k, v)| cedar_json(v).map(|v| (k.clone(), v)))
                .collect(),
            AnyData::Value(value) => cedar_json(&value)
                .map(|v| Map::from_iter([("value".to_owned(), v)]))
                .unwrap_or_default(),
        };
        Ok(Self {
            type_name,
            data: record_fields(&data_json),
            data_json,
        })
    }
}

/// Converts JSON object fields to Cedar record fields, dropping values Cedar
/// can't represent.
fn record_fields(fields: &Map<String, Value>) -> Vec<(String, RestrictedExpression)> {
    fields
        .iter()
        .filter_map(|(k, v)| to_cedar(v).map(|v| (k.clone(), v)))
        .collect()
}

/// The part of a JSON value Cedar can represent (see to_cedar), as JSON.
fn cedar_json(value: &Value) -> Option<Value> {
    match value {
        Value::Null => None,
        Value::Number(n) => n.as_i64().map(Value::from),
        Value::Array(items) => Some(Value::Array(items.iter().filter_map(cedar_json).collect())),
        Value::Object(fields) => Some(Value::Object(
            fields
                .iter()
                .filter_map(|(k, v)| cedar_json(v).map(|v| (k.clone(), v)))
                .collect(),
        )),
        Value::Bool(_) | Value::String(_) => Some(value.clone()),
    }
}

/// Converts a JSON value to a Cedar value. Floats and nulls have no Cedar
/// equivalent and are dropped (`None`); arrays become sets.
fn to_cedar(value: &Value) -> Option<RestrictedExpression> {
    match value {
        Value::Null => None,
        Value::Bool(b) => Some(RestrictedExpression::new_bool(*b)),
        Value::Number(n) => n.as_i64().map(RestrictedExpression::new_long),
        Value::String(s) => Some(RestrictedExpression::new_string(s.clone())),
        Value::Array(items) => Some(RestrictedExpression::new_set(
            items.iter().filter_map(to_cedar),
        )),
        Value::Object(fields) => RestrictedExpression::new_record(record_fields(fields)).ok(),
    }
}

fn uid(entity_type: &str, id: &str) -> anyhow::Result<EntityUid> {
    Ok(EntityUid::from_type_name_and_id(
        EntityTypeName::from_str(entity_type)?,
        EntityId::new(id),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    impl PolicyEngine {
        /// verify for a run the verifier has no input events for.
        fn verify_no_inputs(&self, request: &Value) -> anyhow::Result<Verdict> {
            self.verify(request, &InputContext::default())
        }
    }

    const EXAMPLE_POLICY: &str = include_str!("../policies/example.cedar");
    const SCHEMA: &str = include_str!("../policies/schema.cedarschema");

    fn engine() -> PolicyEngine {
        PolicyEngine::new(EXAMPLE_POLICY, Some(SCHEMA))
            .expect("example policy validates against schema")
    }

    fn request(tool_calls: Value) -> Value {
        json!({
            "caller": {
                "namespace": "ns-a",
                "subject": "fleet-a",
                "taskQueue": "queue-a",
                "workflowId": "billing-42",
            },
            "toolCalls": tool_calls,
        })
    }

    fn verify(tool_calls: Value) -> Verdict {
        engine()
            .verify_no_inputs(&request(tool_calls))
            .expect("valid input")
    }

    fn assert_allowed(tool_calls: Value) {
        let verdict = verify(tool_calls);
        assert!(verdict.allowed(), "expected allow, got {verdict:?}");
    }

    fn assert_decision(tool_calls: Value, decision: Decision, reason: &str) {
        let verdict = verify(tool_calls);
        assert_eq!(verdict.decision, decision, "verdict {verdict:?}");
        assert!(
            verdict.reason.contains(reason),
            "reason {:?} should contain {reason:?}",
            verdict.reason
        );
    }

    fn assert_denied(tool_calls: Value, reason: &str) {
        assert_decision(tool_calls, Decision::Denied, reason);
    }

    fn assert_escalated(tool_calls: Value, reason: &str) {
        assert_decision(tool_calls, Decision::Escalate, reason);
    }

    /// A google.protobuf.Value parameter, as the proxy renders json/plain input.
    fn value_param(value: Value) -> Value {
        json!({"@type": "type.googleapis.com/google.protobuf.Value", "value": value})
    }

    fn call(name: &str, parameters: Vec<Value>) -> Value {
        json!([{"name": name, "parameters": parameters}])
    }

    fn charge(amount: Value, currency: &str) -> Value {
        call(
            "payments/billing/Charge",
            vec![value_param(json!({"amount": amount, "currency": currency}))],
        )
    }

    #[test]
    fn echo_is_allowed() {
        assert_allowed(call("echo/echo/Echo", vec![value_param(json!("hi"))]));
        assert_allowed(call("echo/echo/Echo", vec![]));
    }

    #[test]
    fn charge_limits() {
        assert_allowed(charge(json!(500), "USD"));
        assert_allowed(charge(json!(1000), "EUR"));
        assert_escalated(
            charge(json!(5000), "USD"),
            "tool payments/billing/Charge escalated by Cedar policy charge-over-1000-needs-review",
        );
        assert_escalated(charge(json!(10000), "USD"), "charge-over-1000-needs-review");
        // Over the hard limit both forbids apply; the plain one wins.
        assert_denied(charge(json!(50000), "USD"), "charge-limit-10000");
        assert_denied(
            charge(json!(500), "GBP"),
            "tool payments/billing/Charge not permitted by any Cedar policy",
        );
        // A float amount can't be represented in Cedar, so it's dropped, which
        // the limit treats like a missing amount.
        assert_denied(charge(json!(500.5), "USD"), "charge-limit-10000");
        // No parameters at all: the limit forbids it (forbid wins over permit).
        assert_denied(
            call("payments/billing/Charge", vec![]),
            "charge-limit-10000",
        );
    }

    #[test]
    fn undeclared_tool_is_denied() {
        assert_denied(
            call(
                "forbidden-queue/EchoActivity",
                vec![value_param(json!("x"))],
            ),
            "tool forbidden-queue/EchoActivity not permitted by any Cedar policy",
        );
    }

    #[test]
    fn first_denied_call_denies_request() {
        let mut calls = call("echo/echo/Echo", vec![]);
        calls
            .as_array_mut()
            .unwrap()
            .extend(charge(json!(50000), "USD").as_array().unwrap().clone());
        assert_denied(calls, "charge-limit-10000");
    }

    #[test]
    fn denial_outweighs_escalation() {
        // An escalated call followed by a denied one: the request is denied.
        let mut calls = charge(json!(5000), "USD");
        calls.as_array_mut().unwrap().extend(
            call(
                "trusted-tools/SendEmail",
                vec![value_param(json!("a@evil.com"))],
            )
            .as_array()
            .unwrap()
            .clone(),
        );
        assert_denied(calls, "SendEmail not permitted");

        // An escalated call next to allowed ones: the request is escalated.
        let mut calls = call("echo/echo/Echo", vec![]);
        calls
            .as_array_mut()
            .unwrap()
            .extend(charge(json!(5000), "USD").as_array().unwrap().clone());
        assert_escalated(calls, "charge-over-1000-needs-review");
    }

    #[test]
    fn no_tool_calls() {
        assert_allowed(json!([]));
        assert!(
            engine()
                .verify_no_inputs(&json!({"caller": {"taskQueue": "queue-a"}}))
                .unwrap()
                .allowed()
        );
    }

    #[test]
    fn parameter_mapping() {
        let message = Parameter::parse(&json!({
            "@type": "type.googleapis.com/acme.billing.v1.ChargeRequest",
            "amount": 5,
            "currency": "USD",
            "ratio": 0.5,
            "note": null,
        }))
        .unwrap();
        assert_eq!(message.type_name, "acme.billing.v1.ChargeRequest");
        let mut keys: Vec<_> = message.data.iter().map(|(k, _)| k.as_str()).collect();
        keys.sort();
        assert_eq!(keys, ["amount", "currency"]);

        let opaque = Parameter::parse(&json!({
            "@type": "type.googleapis.com/temporal.api.common.v1.Payload",
            "metadata": {"encoding": "YmluYXJ5L2VuY3J5cHRlZA=="},
            "data": "c2VjcmV0",
        }))
        .unwrap();
        assert!(opaque.data.is_empty());

        let scalar = Parameter::parse(&value_param(json!("hi"))).unwrap();
        assert_eq!(scalar.type_name, "google.protobuf.Value");
        assert_eq!(scalar.data.len(), 1);
        assert_eq!(scalar.data[0].0, "value");

        assert!(Parameter::parse(&json!({"no": "type"})).is_err());
        assert!(Parameter::parse(&json!("not an any")).is_err());
    }

    #[test]
    fn multiple_parameters_and_context_fields() {
        let policies = format!(
            "{EXAMPLE_POLICY}\n{}",
            r#"@id("two-args-from-billing-workflows")
            permit (principal, action == Action::"echo/echo/Echo", resource)
            when {
              context.workflowId like "billing-*" &&
              context.parameterCount == 2 &&
              context.parameters has arg1 &&
              context.parameters.arg1.type == "acme.v1.Note"
            };"#
        );
        // No schema: the extra policy uses fields the example schema doesn't
        // declare for echo.
        let engine = PolicyEngine::new(&policies, None).unwrap();
        let req = request(call(
            "echo/echo/Echo",
            vec![
                value_param(json!("hi")),
                json!({"@type": "type.googleapis.com/acme.v1.Note", "text": "x"}),
            ],
        ));
        assert!(engine.verify_no_inputs(&req).unwrap().allowed());
    }

    /// A protobuf JSON Any of a google.protobuf.Value, as the proxy sends
    /// json/plain payloads.
    fn value_any(value: Value) -> Value {
        json!({"@type": "type.googleapis.com/google.protobuf.Value", "value": value})
    }

    fn recorded(name: &str, count: u64, data: Value) -> RecordedEvent {
        RecordedEvent {
            name: name.to_owned(),
            count,
            payloads: vec![value_any(data)],
        }
    }

    fn started_with(input: Value) -> InputContext {
        InputContext {
            workflow_input: Some(recorded("ToolWorkflow", 1, input)),
            complete: true,
            ..Default::default()
        }
    }

    fn with_signal(mut inputs: InputContext, name: &str, count: u64, data: Value) -> InputContext {
        inputs
            .events
            .entry("signal".to_owned())
            .or_default()
            .insert(name.to_owned(), recorded(name, count, data));
        inputs
    }

    #[test]
    fn send_email_only_to_the_customer() {
        let send = |to: &str| {
            request(call(
                "trusted-tools/SendEmail",
                vec![value_param(json!(to))],
            ))
        };
        let mut inputs =
            started_with(json!({"case": "email", "customerEmail": "alice@example.com"}));
        let e = engine();
        let allowed = e.verify(&send("alice@example.com"), &inputs).unwrap();
        assert!(allowed.allowed(), "{allowed:?}");
        let denied = e.verify(&send("mallory@evil.com"), &inputs).unwrap();
        assert_eq!(denied.decision, Decision::Denied);
        assert!(denied.reason.contains("not permitted"));

        // Without the run's full context the address can't be vouched for.
        inputs.complete = false;
        assert!(
            !e.verify(&send("alice@example.com"), &inputs)
                .unwrap()
                .allowed()
        );
        assert!(
            !e.verify_no_inputs(&send("alice@example.com"))
                .unwrap()
                .allowed()
        );
    }

    #[test]
    fn refund_needs_matching_approval_signal() {
        let refund = |id: &str| {
            request(call(
                "trusted-tools/IssueRefund",
                vec![value_param(json!(id))],
            ))
        };
        let started = started_with(json!({"refundId": "R-1001"}));
        let approved = with_signal(
            started.clone(),
            "approval",
            1,
            json!({"refundId": "R-1001", "approvedBy": "manager@example.com"}),
        );
        let e = engine();
        let verdict = e.verify(&refund("R-1001"), &approved).unwrap();
        assert!(verdict.allowed(), "{verdict:?}");
        // An approval names a specific refund.
        assert!(!e.verify(&refund("R-9999"), &approved).unwrap().allowed());
        // The refund ID in the workflow input isn't an approval.
        assert!(!e.verify(&refund("R-1001"), &started).unwrap().allowed());
        // Policies see the latest approval signal.
        let reapproved = with_signal(approved, "approval", 2, json!({"refundId": "R-2002"}));
        assert!(!e.verify(&refund("R-1001"), &reapproved).unwrap().allowed());
        assert!(e.verify(&refund("R-2002"), &reapproved).unwrap().allowed());
    }

    #[test]
    fn events_by_type_and_name() {
        // Activity and Nexus names contain "/", so policies quote them.
        let schema = SCHEMA.replace(
            "  activity: {},",
            r#"  activity: { "crm/LookupCustomer"?: { type: String, name: String, count: Long,
                 parameters: { arg0?: { type: String, data: { email?: String } } } } },"#,
        );
        let policies = r#"
            @id("email-crm-contact")
            permit (principal, action == Action::"trusted-tools/SendEmail", resource)
            when {
              context.events.activity has "crm/LookupCustomer" &&
              context.events.activity["crm/LookupCustomer"].count >= 1 &&
              context.events.activity["crm/LookupCustomer"].parameters has arg0 &&
              context.events.activity["crm/LookupCustomer"].parameters.arg0.data has email &&
              context.parameters has arg0 && context.parameters.arg0.data has value &&
              context.parameters.arg0.data.value == context.events.activity["crm/LookupCustomer"].parameters.arg0.data.email
            };
        "#;
        let engine = PolicyEngine::new(policies, Some(&schema)).expect("validates against schema");
        let send = |to: &str| {
            request(call(
                "trusted-tools/SendEmail",
                vec![value_param(json!(to))],
            ))
        };
        let mut inputs = InputContext::default();
        inputs.events.entry("activity".to_owned()).or_default().insert(
            "crm/LookupCustomer".to_owned(),
            RecordedEvent {
                name: "crm/LookupCustomer".to_owned(),
                count: 1,
                payloads: vec![json!({"@type": "type.googleapis.com/acme.v1.Customer", "email": "bob@example.com"})],
            },
        );
        assert!(
            engine
                .verify(&send("bob@example.com"), &inputs)
                .unwrap()
                .allowed()
        );
        assert!(
            !engine
                .verify(&send("eve@example.com"), &inputs)
                .unwrap()
                .allowed()
        );
    }

    #[test]
    fn context_json_matches_what_cedar_sees() {
        let call = ToolCall::parse(&proto::ToolCall {
            name: "payments/billing/Charge".to_owned(),
            parameters: vec![value_param(
                json!({"amount": 500, "currency": "USD", "ratio": 0.5, "note": null}),
            )],
        })
        .unwrap();
        let inputs = with_signal(
            started_with(json!({"customerEmail": "alice@example.com"})),
            "approval",
            2,
            json!({"refundId": "R-1001"}),
        );
        let (_, json) = call.context("wf-1", &inputs).unwrap();
        assert_eq!(
            json,
            json!({
                "workflowId": "wf-1",
                "parameterCount": 1,
                "parameters": {"arg0": {"type": "google.protobuf.Value", "data": {"amount": 500, "currency": "USD"}}},
                "workflowInput": {
                    "name": "ToolWorkflow",
                    "parameters": {"arg0": {"type": "google.protobuf.Value", "data": {"customerEmail": "alice@example.com"}}},
                },
                "events": {
                    "activity": {},
                    "nexus": {},
                    "signal": {"approval": {
                        "type": "signal",
                        "name": "approval",
                        "count": 2,
                        "parameters": {"arg0": {"type": "google.protobuf.Value", "data": {"refundId": "R-1001"}}},
                    }},
                    "update": {},
                },
                "inputEventsComplete": true,
            })
        );
    }

    #[test]
    fn workflow_id_condition() {
        let policies = r#"
            @id("billing-workflows-only")
            permit (principal, action == Action::"echo/echo/Echo", resource)
            when { context.workflowId like "billing-*" };
        "#;
        let engine = PolicyEngine::new(policies, Some(SCHEMA)).unwrap();
        assert!(
            engine
                .verify_no_inputs(&request(call("echo/echo/Echo", vec![])))
                .unwrap()
                .allowed()
        );
        let mut other = request(call("echo/echo/Echo", vec![]));
        other["caller"]["workflowId"] = json!("marketing-1");
        assert!(!engine.verify_no_inputs(&other).unwrap().allowed());
    }

    #[test]
    fn accepts_proto_field_names() {
        let verdict = engine()
            .verify_no_inputs(&json!({
                "caller": {"task_queue": "queue-a", "workflow_id": "wf"},
                "tool_calls": [{"name": "echo/echo/Echo"}],
            }))
            .unwrap();
        assert!(verdict.allowed());
    }

    #[test]
    fn readme_declaring_a_tool_example() {
        // The "Declaring a tool" walkthrough from README.md.
        let schema = format!(
            "{SCHEMA}\n{}",
            r#"type RefundArguments = { arg0?: { type: String, data: { orderId?: String, amount?: Long } } };
            action "payments/billing/Refund" appliesTo {
              principal: Worker,
              resource: TaskQueue,
              context: { workflowId: String, parameterCount: Long, parameters: RefundArguments },
            };"#
        );
        let policies = format!(
            "{EXAMPLE_POLICY}\n{}",
            r#"@id("refunds-from-order-workflows")
            permit (principal, action == Action::"payments/billing/Refund", resource)
            when {
              context.workflowId like "order-*" &&
              context.parameters has arg0 && context.parameters.arg0.data has amount &&
              context.parameters.arg0.data.amount <= 100
            };"#
        );
        let engine = PolicyEngine::new(&policies, Some(&schema)).expect("README example validates");
        let mut req = request(call(
            "payments/billing/Refund",
            vec![value_param(json!({"orderId": "o-1", "amount": 50}))],
        ));
        req["caller"]["workflowId"] = json!("order-7");
        assert!(engine.verify_no_inputs(&req).unwrap().allowed());
        req["caller"]["workflowId"] = json!("billing-7");
        assert!(!engine.verify_no_inputs(&req).unwrap().allowed());
    }

    #[test]
    fn invalid_policy_is_rejected_by_schema() {
        let err = PolicyEngine::new(
            r#"permit (principal, action == Action::"echo/echo/Echo", resource) when { context.nope == "" };"#,
            Some(SCHEMA),
        );
        assert!(err.is_err());
    }

    #[test]
    fn malformed_input() {
        assert!(
            engine()
                .verify_no_inputs(&json!({"toolCalls": "nope"}))
                .is_err()
        );
        assert!(
            engine()
                .verify_no_inputs(&json!({"toolCalls": [42]}))
                .is_err()
        );
        assert!(
            engine()
                .verify_no_inputs(&json!({"toolCalls": [{"parameters": []}]}))
                .is_err()
        );
        assert!(
            engine()
                .verify_no_inputs(
                    &json!({"toolCalls": [{"name": "x", "parameters": [{"no": "type"}]}]})
                )
                .is_err()
        );
    }
}
