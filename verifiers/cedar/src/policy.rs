//! Evaluates a `VerifyToolCallsRequest` against Cedar policies.
//!
//! Every tool call becomes one Cedar authorization request (see
//! `policies/schema.cedarschema` for the model); the request is allowed only
//! if every tool call is.

use std::collections::{HashMap, HashSet};
use std::str::FromStr;

use anyhow::{Context as _, anyhow, bail};
use cedar_policy::{
    Authorizer, Context, Decision, Entities, Entity, EntityId, EntityTypeName, EntityUid,
    PolicySet, Request, RestrictedExpression, Schema, ValidationMode, Validator,
};
use serde_json::{Map, Value};

/// The verdict returned to the proxy as a `VerifyToolCallsResponse`.
#[derive(Debug, PartialEq, Eq)]
pub struct Verdict {
    pub allowed: bool,
    pub reason: String,
}

impl Verdict {
    fn allow() -> Self {
        Self {
            allowed: true,
            reason: String::new(),
        }
    }
}

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
    pub fn verify(&self, request: &Value) -> anyhow::Result<Verdict> {
        let caller = field(request, "caller", "caller").unwrap_or(&Value::Null);
        let namespace = string_field(caller, "namespace", "namespace");
        let subject = string_field(caller, "subject", "subject");
        let task_queue = string_field(caller, "taskQueue", "task_queue");
        let workflow_id = string_field(caller, "workflowId", "workflow_id");
        let tool_calls = match field(request, "toolCalls", "tool_calls") {
            None | Some(Value::Null) => &[][..],
            Some(Value::Array(calls)) => calls.as_slice(),
            Some(_) => bail!("toolCalls must be an array"),
        };

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

        for (i, call) in tool_calls.iter().enumerate() {
            let call = ToolCall::parse(call).with_context(|| format!("tool call {i}"))?;
            let request = Request::new(
                principal.clone(),
                uid("Action", &call.name)?,
                resource.clone(),
                call.context(&workflow_id)?,
                None,
            )?;
            let response = self
                .authorizer
                .is_authorized(&request, &self.policies, &entities);
            for error in response.diagnostics().errors() {
                tracing::warn!(%error, tool = %call.name, "Cedar evaluation error");
            }
            if response.decision() == Decision::Deny {
                return Ok(Verdict {
                    allowed: false,
                    reason: self.deny_reason(&call.name, &response),
                });
            }
        }
        Ok(Verdict::allow())
    }

    fn deny_reason(&self, tool: &str, response: &cedar_policy::Response) -> String {
        let policies: Vec<&str> = response
            .diagnostics()
            .reason()
            .map(|id| {
                self.policies
                    .annotation(id, "id")
                    .unwrap_or_else(|| id.as_ref())
            })
            .collect();
        if policies.is_empty() {
            format!("tool {tool} not permitted by any Cedar policy")
        } else {
            format!("tool {tool} denied by Cedar policy {}", policies.join(", "))
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
}

impl ToolCall {
    fn parse(call: &Value) -> anyhow::Result<Self> {
        let name = string_field(call, "name", "name");
        if name.is_empty() {
            bail!("tool call has no name");
        }
        let parameters = match call.get("parameters") {
            None | Some(Value::Null) => Vec::new(),
            Some(Value::Array(params)) => params
                .iter()
                .enumerate()
                .map(|(i, p)| Parameter::parse(p).with_context(|| format!("parameter {i}")))
                .collect::<anyhow::Result<_>>()?,
            Some(_) => bail!("parameters must be an array"),
        };
        Ok(Self { name, parameters })
    }

    fn context(&self, workflow_id: &str) -> anyhow::Result<Context> {
        let mut parameters = Vec::with_capacity(self.parameters.len());
        for (i, param) in self.parameters.iter().enumerate() {
            let record = RestrictedExpression::new_record([
                (
                    "type".to_owned(),
                    RestrictedExpression::new_string(param.type_name.clone()),
                ),
                (
                    "data".to_owned(),
                    RestrictedExpression::new_record(param.data.clone())?,
                ),
            ])?;
            parameters.push((format!("arg{i}"), record));
        }
        Ok(Context::from_pairs([
            (
                "workflowId".to_owned(),
                RestrictedExpression::new_string(workflow_id.to_owned()),
            ),
            (
                "parameterCount".to_owned(),
                RestrictedExpression::new_long(self.parameters.len() as i64),
            ),
            (
                "parameters".to_owned(),
                RestrictedExpression::new_record(parameters)?,
            ),
        ])?)
    }
}

/// Well-known types whose protobuf JSON form is not a plain object of fields;
/// inside an Any they appear under a "value" key.
const VALUE_FORM_TYPES: &[&str] = &[
    "google.protobuf.Any",
    "google.protobuf.Struct",
    "google.protobuf.Value",
    "google.protobuf.ListValue",
    "google.protobuf.Timestamp",
    "google.protobuf.Duration",
    "google.protobuf.FieldMask",
    "google.protobuf.DoubleValue",
    "google.protobuf.FloatValue",
    "google.protobuf.Int64Value",
    "google.protobuf.UInt64Value",
    "google.protobuf.Int32Value",
    "google.protobuf.UInt32Value",
    "google.protobuf.BoolValue",
    "google.protobuf.StringValue",
    "google.protobuf.BytesValue",
];

/// The proxy packs payloads it can't interpret (e.g. encrypted ones) as-is.
const OPAQUE_PAYLOAD_TYPE: &str = "temporal.api.common.v1.Payload";

impl Parameter {
    /// Interprets the protobuf JSON form of a google.protobuf.Any.
    fn parse(any: &Value) -> anyhow::Result<Self> {
        let Some(object) = any.as_object() else {
            bail!("parameter must be a JSON object (a google.protobuf.Any)");
        };
        let type_url = object
            .get("@type")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("parameter has no \"@type\""))?;
        let type_name = type_url.rsplit('/').next().unwrap_or(type_url).to_owned();

        let data = if type_name == OPAQUE_PAYLOAD_TYPE {
            Vec::new()
        } else if VALUE_FORM_TYPES.contains(&type_name.as_str()) {
            match object.get("value") {
                Some(Value::Object(fields)) => record_fields(fields),
                Some(value) => to_cedar(value)
                    .map(|v| vec![("value".to_owned(), v)])
                    .unwrap_or_default(),
                None => Vec::new(),
            }
        } else {
            let mut fields = object.clone();
            fields.remove("@type");
            record_fields(&fields)
        };
        Ok(Self { type_name, data })
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

/// Looks up a field by its protobuf JSON name, falling back to the original
/// proto field name (both are valid protobuf JSON input).
fn field<'a>(value: &'a Value, camel: &str, snake: &str) -> Option<&'a Value> {
    value.get(camel).or_else(|| value.get(snake))
}

fn string_field(value: &Value, camel: &str, snake: &str) -> String {
    field(value, camel, snake)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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
        engine().verify(&request(tool_calls)).expect("valid input")
    }

    fn assert_allowed(tool_calls: Value) {
        let verdict = verify(tool_calls);
        assert!(verdict.allowed, "expected allow, got {verdict:?}");
    }

    fn assert_denied(tool_calls: Value, reason: &str) {
        let verdict = verify(tool_calls);
        assert!(!verdict.allowed, "expected deny");
        assert!(
            verdict.reason.contains(reason),
            "reason {:?} should contain {reason:?}",
            verdict.reason
        );
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
        assert_denied(charge(json!(5000), "USD"), "charge-limit-1000");
        assert_denied(
            charge(json!(500), "GBP"),
            "tool payments/billing/Charge not permitted by any Cedar policy",
        );
        // A float amount can't be represented in Cedar, so it's dropped, which
        // the limit treats like a missing amount.
        assert_denied(charge(json!(500.5), "USD"), "charge-limit-1000");
        // No parameters at all: the limit forbids it (forbid wins over permit).
        assert_denied(call("payments/billing/Charge", vec![]), "charge-limit-1000");
    }

    #[test]
    fn send_email_domain() {
        let send = |to: &str| call("trusted-tools/SendEmail", vec![value_param(json!(to))]);
        assert_allowed(send("a@example.com"));
        assert_denied(send("a@evil.com"), "not permitted");
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
            .extend(charge(json!(5000), "USD").as_array().unwrap().clone());
        assert_denied(calls, "charge-limit-1000");
    }

    #[test]
    fn no_tool_calls() {
        assert_allowed(json!([]));
        assert!(
            engine()
                .verify(&json!({"caller": {"taskQueue": "queue-a"}}))
                .unwrap()
                .allowed
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
        assert!(engine.verify(&req).unwrap().allowed);
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
                .verify(&request(call("echo/echo/Echo", vec![])))
                .unwrap()
                .allowed
        );
        let mut other = request(call("echo/echo/Echo", vec![]));
        other["caller"]["workflowId"] = json!("marketing-1");
        assert!(!engine.verify(&other).unwrap().allowed);
    }

    #[test]
    fn accepts_proto_field_names() {
        let verdict = engine()
            .verify(&json!({
                "caller": {"task_queue": "queue-a", "workflow_id": "wf"},
                "tool_calls": [{"name": "echo/echo/Echo"}],
            }))
            .unwrap();
        assert!(verdict.allowed);
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
        assert!(engine.verify(&req).unwrap().allowed);
        req["caller"]["workflowId"] = json!("billing-7");
        assert!(!engine.verify(&req).unwrap().allowed);
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
        assert!(engine().verify(&json!({"toolCalls": "nope"})).is_err());
        assert!(engine().verify(&json!({"toolCalls": [42]})).is_err());
        assert!(
            engine()
                .verify(&json!({"toolCalls": [{"parameters": []}]}))
                .is_err()
        );
        assert!(
            engine()
                .verify(&json!({"toolCalls": [{"name": "x", "parameters": [{"no": "type"}]}]}))
                .is_err()
        );
    }
}
