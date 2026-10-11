//! Stores the input events temporal-proxy records for each workflow run
//! (`RecordInputEvents`), so `VerifyToolCalls` can expose them to policies as
//! `context.workflowInput` and `context.events`.
//!
//! The store is in memory: a verifier restart loses it, and with several
//! verifier instances each only sees the events delivered to it. Run one
//! verifier instance, or expect incomplete context otherwise.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Mutex;

use anyhow::bail;
use serde_json::Value;

use crate::policy::{InputContext, RecordedEvent, parse_any};

/// What the store keeps per run: the workflow's start input, and the latest
/// event of each type (activity, nexus, signal, update) and name.
#[derive(Debug, Default)]
struct RunInputs {
    workflow_input: Option<RecordedEvent>,
    events: BTreeMap<String, BTreeMap<String, RecordedEvent>>,
    seen_event_ids: HashSet<i64>,
    seen_update_ids: HashSet<String>,
    complete: bool,
}

/// An in-memory, size-bounded store of input events per workflow run.
pub struct InputStore {
    max_runs: usize,
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    runs: HashMap<String, RunInputs>,
    // Insertion order, for evicting the oldest run when full.
    order: VecDeque<String>,
}

impl InputStore {
    pub fn new(max_runs: usize) -> Self {
        Self {
            max_runs: max_runs.max(1),
            inner: Mutex::new(Inner::default()),
        }
    }

    /// Records a `RecordInputEventsRequest` (protobuf JSON mapping). Events
    /// already recorded for the run (same event ID or update ID) are skipped.
    /// Returns the number of new events.
    pub fn record(&self, request: &Value) -> anyhow::Result<usize> {
        let key = run_key(request.get("caller").unwrap_or(&Value::Null));
        let events = match request.get("events") {
            None | Some(Value::Null) => &[][..],
            Some(Value::Array(events)) => events.as_slice(),
            Some(_) => bail!("events must be an array"),
        };
        let from_start = request
            .get("fromHistoryStart")
            .or_else(|| request.get("from_history_start"))
            .and_then(Value::as_bool)
            .unwrap_or(false);

        // Parse everything before touching the store, so a malformed request
        // records nothing.
        let mut parsed = Vec::with_capacity(events.len());
        for event in events {
            parsed.push(ParsedEvent::parse(event)?);
        }

        let mut inner = self.inner.lock().expect("input store lock");
        if !inner.runs.contains_key(&key) {
            while inner.runs.len() >= self.max_runs {
                let Some(oldest) = inner.order.pop_front() else {
                    break;
                };
                inner.runs.remove(&oldest);
            }
            inner.order.push_back(key.clone());
        }
        let run = inner.runs.entry(key).or_default();
        run.complete |= from_start;
        let mut added = 0;
        for event in parsed {
            let new = match (&event.update_id, event.event_id) {
                (Some(update_id), _) => run.seen_update_ids.insert(update_id.clone()),
                (None, Some(event_id)) => run.seen_event_ids.insert(event_id),
                (None, None) => true,
            };
            if !new {
                continue;
            }
            added += 1;
            if event.kind == "workflow" {
                run.workflow_input = Some(RecordedEvent {
                    name: event.name,
                    count: 1,
                    payloads: event.payloads,
                });
                continue;
            }
            // Events arrive oldest first: a newer event of the same type and
            // name replaces the older one.
            let entry = run
                .events
                .entry(event.kind)
                .or_default()
                .entry(event.name.clone())
                .or_insert_with(|| RecordedEvent {
                    name: event.name,
                    ..Default::default()
                });
            entry.count += 1;
            entry.payloads = event.payloads;
        }
        Ok(added)
    }

    /// The input context for the run a `VerifyToolCallsRequest`'s caller
    /// identifies; empty and incomplete if nothing was recorded for it.
    pub fn context_for(&self, caller: &Value) -> InputContext {
        let inner = self.inner.lock().expect("input store lock");
        match inner.runs.get(&run_key(caller)) {
            Some(run) => InputContext {
                workflow_input: run.workflow_input.clone(),
                events: run.events.clone(),
                complete: run.complete,
            },
            None => InputContext::default(),
        }
    }
}

fn run_key(caller: &Value) -> String {
    let get = |camel: &str, snake: &str| {
        caller
            .get(camel)
            .or_else(|| caller.get(snake))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned()
    };
    format!(
        "{}/{}/{}",
        get("namespace", "namespace"),
        get("workflowId", "workflow_id"),
        get("runId", "run_id")
    )
}

struct ParsedEvent {
    kind: String,
    name: String,
    // The payloads as protobuf JSON Anys, checked to be well-formed.
    payloads: Vec<Value>,
    event_id: Option<i64>,
    update_id: Option<String>,
}

impl ParsedEvent {
    fn parse(event: &Value) -> anyhow::Result<Self> {
        let kind = match event.get("kind") {
            Some(Value::String(s)) => short_kind(s),
            Some(Value::Number(n)) => short_kind_number(n.as_i64().unwrap_or(0)),
            _ => bail!("input event has no kind"),
        };
        let name = event
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        // int64 fields are strings in the protobuf JSON mapping; accept numbers too.
        let event_id = match event.get("eventId").or_else(|| event.get("event_id")) {
            Some(Value::String(s)) => Some(s.parse::<i64>()?),
            Some(Value::Number(n)) => n.as_i64(),
            _ => None,
        }
        .filter(|id| *id != 0);
        let update_id = event
            .get("updateId")
            .or_else(|| event.get("update_id"))
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_owned);

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

    fn caller(run: &str) -> Value {
        json!({"namespace": "ns", "workflowId": "wf", "runId": run})
    }

    fn value_param(value: Value) -> Value {
        json!({"@type": "type.googleapis.com/google.protobuf.Value", "value": value})
    }

    #[test]
    fn keeps_workflow_input_and_latest_event_per_name() {
        let store = InputStore::new(10);
        let batch = json!({
            "caller": caller("r1"),
            "fromHistoryStart": true,
            "events": [
                {"kind": "INPUT_EVENT_KIND_WORKFLOW_INPUT", "name": "AgentWorkflow", "eventId": "1",
                 "payloads": [value_param(json!({"customerEmail": "alice@example.com"}))]},
                {"kind": "INPUT_EVENT_KIND_SIGNAL", "name": "approval", "eventId": "5",
                 "payloads": [value_param(json!({"refundId": "R-1001"}))]},
                {"kind": "INPUT_EVENT_KIND_SIGNAL", "name": "approval", "eventId": "9",
                 "payloads": [value_param(json!({"refundId": "R-2002"}))]},
                {"kind": "INPUT_EVENT_KIND_UPDATE", "name": "chat", "updateId": "u-1",
                 "payloads": [value_param(json!("hello"))]},
                {"kind": 2, "name": "crm/LookupCustomer", "eventId": 12,
                 "payloads": [{"@type": "type.googleapis.com/acme.v1.Customer", "email": "bob@example.com"}]},
            ],
        });
        assert_eq!(store.record(&batch).unwrap(), 5);
        // Re-sent events are skipped.
        assert_eq!(store.record(&batch).unwrap(), 0);

        let ctx = store.context_for(&caller("r1"));
        assert!(ctx.complete);
        let input = ctx.workflow_input.as_ref().unwrap();
        assert_eq!(input.name, "AgentWorkflow");
        assert_eq!(
            input.payloads[0]["value"]["customerEmail"],
            "alice@example.com"
        );

        let approval = &ctx.events["signal"]["approval"];
        assert_eq!(approval.count, 2);
        assert_eq!(
            approval.payloads[0]["value"]["refundId"], "R-2002",
            "latest wins"
        );
        assert_eq!(ctx.events["update"]["chat"].payloads[0]["value"], "hello");
        assert_eq!(
            ctx.events["activity"]["crm/LookupCustomer"].payloads[0]["email"],
            "bob@example.com"
        );

        // Other runs are separate, and unknown runs are empty and incomplete.
        let other = store.context_for(&caller("r2"));
        assert!(!other.complete && other.workflow_input.is_none() && other.events.is_empty());
    }

    #[test]
    fn partial_history_is_incomplete() {
        let store = InputStore::new(10);
        store
            .record(&json!({"caller": caller("r1"), "events": [
                {"kind": "INPUT_EVENT_KIND_ACTIVITY_RESULT", "name": "crm/Lookup", "eventId": "7",
                 "payloads": [value_param(json!("ok"))]},
            ]}))
            .unwrap();
        let ctx = store.context_for(&caller("r1"));
        assert!(!ctx.complete);
        assert!(ctx.workflow_input.is_none());
        assert_eq!(ctx.events["activity"]["crm/Lookup"].count, 1);
    }

    #[test]
    fn evicts_oldest_run_when_full() {
        let store = InputStore::new(2);
        for run in ["r1", "r2", "r3"] {
            store
                .record(&json!({"caller": caller(run), "fromHistoryStart": true}))
                .unwrap();
        }
        assert!(!store.context_for(&caller("r1")).complete, "r1 was evicted");
        assert!(store.context_for(&caller("r3")).complete);
    }

    #[test]
    fn malformed_request_records_nothing() {
        let store = InputStore::new(10);
        let bad = json!({"caller": caller("r1"), "fromHistoryStart": true, "events": [
            {"kind": "INPUT_EVENT_KIND_SIGNAL", "name": "ok", "eventId": "2"},
            {"kind": "INPUT_EVENT_KIND_SIGNAL", "name": "bad", "eventId": "3", "payloads": [{"no": "type"}]},
        ]});
        assert!(store.record(&bad).is_err());
        assert!(!store.context_for(&caller("r1")).complete);
    }
}
