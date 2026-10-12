//! The tool verifier's Nexus contract (`proto/toolpolicy/v1/toolpolicy.proto`)
//! as the verifiers see it: requests arrive in the protobuf JSON mapping, and
//! verdicts go back as a `VerifyToolCallsResponse`.

use anyhow::{anyhow, bail};
use serde_json::{Map, Value};

/// A `toolpolicy.v1.Decision`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Approved,
    Denied,
    Escalate,
}

impl Decision {
    /// The enum value's name in the protobuf JSON mapping.
    pub fn proto_name(self) -> &'static str {
        match self {
            Decision::Approved => "DECISION_APPROVED",
            Decision::Denied => "DECISION_DENIED",
            Decision::Escalate => "DECISION_ESCALATE",
        }
    }
}

/// The verdict returned to the proxy as a `VerifyToolCallsResponse`.
#[derive(Debug, PartialEq, Eq)]
pub struct Verdict {
    pub decision: Decision,
    pub reason: String,
}

impl Verdict {
    pub fn approve() -> Self {
        Self {
            decision: Decision::Approved,
            reason: String::new(),
        }
    }

    pub fn allowed(&self) -> bool {
        self.decision == Decision::Approved
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

/// The content of a google.protobuf.Any in protobuf JSON form.
pub enum AnyData {
    /// A payload the proxy couldn't interpret.
    Opaque,
    /// A well-known type in "value" form: the Any's `value`.
    Value(Value),
    /// A message type: its fields.
    Fields(Map<String, Value>),
}

/// Splits the protobuf JSON form of a google.protobuf.Any into its type name
/// (without the `type.googleapis.com/` prefix) and its content.
pub fn parse_any(any: &Value) -> anyhow::Result<(String, AnyData)> {
    let Some(object) = any.as_object() else {
        bail!("expected a JSON object (a google.protobuf.Any)");
    };
    let type_url = object
        .get("@type")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("Any has no \"@type\""))?;
    let type_name = type_url.rsplit('/').next().unwrap_or(type_url).to_owned();

    let data = if type_name == OPAQUE_PAYLOAD_TYPE {
        AnyData::Opaque
    } else if VALUE_FORM_TYPES.contains(&type_name.as_str()) {
        AnyData::Value(object.get("value").cloned().unwrap_or(Value::Null))
    } else {
        let mut fields = object.clone();
        fields.remove("@type");
        AnyData::Fields(fields)
    };
    Ok((type_name, data))
}

/// Looks up a field by its protobuf JSON name, falling back to the original
/// proto field name (both are valid protobuf JSON input).
pub fn field<'a>(value: &'a Value, camel: &str, snake: &str) -> Option<&'a Value> {
    value.get(camel).or_else(|| value.get(snake))
}

pub fn string_field(value: &Value, camel: &str, snake: &str) -> String {
    field(value, camel, snake)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

/// A `VerifyToolCallsRequest`'s caller.
#[derive(Debug, Default, Clone)]
pub struct Caller {
    pub namespace: String,
    pub subject: String,
    pub task_queue: String,
    pub workflow_id: String,
    pub run_id: String,
}

impl Caller {
    pub fn parse(request: &Value) -> Self {
        let caller = field(request, "caller", "caller").unwrap_or(&Value::Null);
        Self {
            namespace: string_field(caller, "namespace", "namespace"),
            subject: string_field(caller, "subject", "subject"),
            task_queue: string_field(caller, "taskQueue", "task_queue"),
            workflow_id: string_field(caller, "workflowId", "workflow_id"),
            run_id: string_field(caller, "runId", "run_id"),
        }
    }

    /// Identifies the caller's workflow run: `<namespace>/<workflowId>/<runId>`.
    pub fn run_key(&self) -> String {
        format!("{}/{}/{}", self.namespace, self.workflow_id, self.run_id)
    }
}

/// One tool call of a `VerifyToolCallsRequest`: its name and its parameters
/// (protobuf JSON Anys).
#[derive(Debug)]
pub struct ToolCall {
    pub name: String,
    pub parameters: Vec<Value>,
}

/// The tool calls of a `VerifyToolCallsRequest`.
pub fn tool_calls(request: &Value) -> anyhow::Result<Vec<ToolCall>> {
    let calls = match field(request, "toolCalls", "tool_calls") {
        None | Some(Value::Null) => return Ok(Vec::new()),
        Some(Value::Array(calls)) => calls,
        Some(_) => bail!("toolCalls must be an array"),
    };
    calls
        .iter()
        .enumerate()
        .map(|(i, call)| {
            let name = string_field(call, "name", "name");
            if name.is_empty() {
                bail!("tool call {i}: tool call has no name");
            }
            let parameters = match call.get("parameters") {
                None | Some(Value::Null) => Vec::new(),
                Some(Value::Array(params)) => params.clone(),
                Some(_) => bail!("tool call {i}: parameters must be an array"),
            };
            Ok(ToolCall { name, parameters })
        })
        .collect()
}

/// A `RecordInputEventsRequest`: the input events a workflow run received.
#[derive(Debug)]
pub struct InputEvents {
    pub caller: Caller,
    /// Whether `events` start at the run's first history event.
    pub from_history_start: bool,
    /// Oldest first.
    pub events: Vec<InputEvent>,
}

/// One input event, its payloads checked to be well-formed Anys.
#[derive(Debug)]
pub struct InputEvent {
    /// The short kind: `workflow`, `activity`, `nexus`, `signal`, `update`,
    /// or `unknown`.
    pub kind: String,
    /// The workflow type, `<taskQueue>/<activityType>`,
    /// `<endpoint>/<service>/<operation>`, or the signal or update name.
    pub name: String,
    /// The payloads as protobuf JSON Anys.
    pub payloads: Vec<Value>,
    pub event_id: Option<i64>,
    pub update_id: Option<String>,
}

impl InputEvents {
    /// Parses a `RecordInputEventsRequest` (protobuf JSON mapping). Fails if
    /// any event is malformed, so a verifier can record all or nothing.
    pub fn parse(request: &Value) -> anyhow::Result<Self> {
        let events = match request.get("events") {
            None | Some(Value::Null) => &[][..],
            Some(Value::Array(events)) => events.as_slice(),
            Some(_) => bail!("events must be an array"),
        };
        let from_history_start = field(request, "fromHistoryStart", "from_history_start")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        Ok(Self {
            caller: Caller::parse(request),
            from_history_start,
            events: events
                .iter()
                .map(InputEvent::parse)
                .collect::<anyhow::Result<_>>()?,
        })
    }
}

impl InputEvent {
    fn parse(event: &Value) -> anyhow::Result<Self> {
        let kind = match event.get("kind") {
            Some(Value::String(s)) => short_kind(s),
            Some(Value::Number(n)) => short_kind_number(n.as_i64().unwrap_or(0)),
            _ => bail!("input event has no kind"),
        };
        let name = string_field(event, "name", "name");
        // int64 fields are strings in the protobuf JSON mapping; accept numbers too.
        let event_id = match field(event, "eventId", "event_id") {
            Some(Value::String(s)) => Some(s.parse::<i64>()?),
            Some(Value::Number(n)) => n.as_i64(),
            _ => None,
        }
        .filter(|id| *id != 0);
        let update_id =
            Some(string_field(event, "updateId", "update_id")).filter(|s| !s.is_empty());

        let payloads = match event.get("payloads") {
            Some(Value::Array(payloads)) => payloads.clone(),
            _ => Vec::new(),
        };
        for payload in &payloads {
            parse_any(payload)?;
        }
        Ok(Self {
            kind,
            name,
            payloads,
            event_id,
            update_id,
        })
    }
}

/// Maps an `InputEventKind` enum name to the short kind policies use.
fn short_kind(name: &str) -> String {
    match name {
        "INPUT_EVENT_KIND_WORKFLOW_INPUT" => "workflow",
        "INPUT_EVENT_KIND_ACTIVITY_RESULT" => "activity",
        "INPUT_EVENT_KIND_NEXUS_OPERATION_RESULT" => "nexus",
        "INPUT_EVENT_KIND_SIGNAL" => "signal",
        "INPUT_EVENT_KIND_UPDATE" => "update",
        _ => "unknown",
    }
    .to_owned()
}

fn short_kind_number(n: i64) -> String {
    match n {
        1 => "workflow",
        2 => "activity",
        3 => "nexus",
        4 => "signal",
        5 => "update",
        _ => "unknown",
    }
    .to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_input_events() {
        let events = InputEvents::parse(&json!({
            "caller": {"namespace": "ns", "workflowId": "wf", "runId": "r1"},
            "from_history_start": true,
            "events": [
                {"kind": "INPUT_EVENT_KIND_SIGNAL", "name": "approval", "eventId": "5",
                 "payloads": [{"@type": "type.googleapis.com/google.protobuf.Value", "value": 1}]},
                {"kind": 5, "name": "chat", "update_id": "u-1"},
            ],
        }))
        .unwrap();
        assert_eq!(events.caller.run_key(), "ns/wf/r1");
        assert!(events.from_history_start);
        assert_eq!(events.events[0].kind, "signal");
        assert_eq!(events.events[0].event_id, Some(5));
        assert_eq!(events.events[1].kind, "update");
        assert_eq!(events.events[1].update_id.as_deref(), Some("u-1"));
    }

    #[test]
    fn rejects_malformed_payloads_and_calls() {
        assert!(
            InputEvents::parse(&json!({"events": [{"kind": 4, "payloads": [{"no": "type"}]}]}))
                .is_err()
        );
        assert!(InputEvents::parse(&json!({"events": [{"name": "no kind"}]})).is_err());
        assert!(tool_calls(&json!({"toolCalls": [{"parameters": []}]})).is_err());
        assert!(tool_calls(&json!({"toolCalls": {}})).is_err());
        assert!(tool_calls(&json!({})).unwrap().is_empty());
    }
}
