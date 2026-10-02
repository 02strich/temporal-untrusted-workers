//! Evaluates a `VerifyCommandsRequest` against Cedar policies.
//!
//! Every command becomes one Cedar authorization request (see
//! `policies/schema.cedarschema` for the model); the call is allowed only if
//! every command is.

use std::collections::{HashMap, HashSet};
use std::str::FromStr;

use anyhow::{Context as _, anyhow, bail};
use cedar_policy::{
    Authorizer, Context, Decision, Entities, Entity, EntityId, EntityTypeName, EntityUid,
    PolicySet, Request, RestrictedExpression, Schema, ValidationMode, Validator,
};
use serde_json::Value;

/// The verdict returned to the proxy as a `VerifyCommandsResponse`.
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

    /// Decides a `VerifyCommandsRequest`, given in the protobuf JSON mapping.
    /// Errors mean the input could not be interpreted.
    pub fn verify(&self, request: &Value) -> anyhow::Result<Verdict> {
        let namespace = string_field(request, "namespace", "namespace");
        let task_queue = string_field(request, "taskQueue", "task_queue");
        let subject = string_field(request, "subject", "subject");
        let task_queues: Vec<String> = field(request, "taskQueues", "task_queues")
            .and_then(Value::as_array)
            .map(|qs| {
                qs.iter()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default();
        let commands = match field(request, "commands", "commands") {
            None | Some(Value::Null) => &[][..],
            Some(Value::Array(commands)) => commands.as_slice(),
            Some(_) => bail!("commands must be an array"),
        };

        let principal = uid("Worker", &subject)?;
        let resource = uid("TaskQueue", &task_queue)?;
        let entities = Entities::from_entities(
            [
                Entity::new(
                    principal.clone(),
                    HashMap::from([
                        (
                            "namespace".to_owned(),
                            RestrictedExpression::new_string(namespace.clone()),
                        ),
                        (
                            "taskQueues".to_owned(),
                            RestrictedExpression::new_set(
                                task_queues
                                    .into_iter()
                                    .map(RestrictedExpression::new_string),
                            ),
                        ),
                    ]),
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

        for (i, command) in commands.iter().enumerate() {
            let command = CommandFacts::extract(command).with_context(|| format!("command {i}"))?;
            let request = Request::new(
                principal.clone(),
                uid("Action", &command.action)?,
                resource.clone(),
                command.context()?,
                None,
            )?;
            let response = self
                .authorizer
                .is_authorized(&request, &self.policies, &entities);
            for error in response.diagnostics().errors() {
                tracing::warn!(%error, action = %command.action, "Cedar evaluation error");
            }
            if response.decision() == Decision::Deny {
                return Ok(Verdict {
                    allowed: false,
                    reason: self.deny_reason(&command.action, &response),
                });
            }
        }
        Ok(Verdict::allow())
    }

    fn deny_reason(&self, action: &str, response: &cedar_policy::Response) -> String {
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
            format!("{action} not permitted by any Cedar policy")
        } else {
            format!("{action} denied by Cedar policy {}", policies.join(", "))
        }
    }
}

/// The parts of a command the policies can see.
#[derive(Debug, Default)]
struct CommandFacts {
    action: String,
    target_task_queue: String,
    target_namespace: String,
    activity_type: String,
    workflow_type: String,
}

impl CommandFacts {
    fn extract(command: &Value) -> anyhow::Result<Self> {
        let Some(command) = command.as_object() else {
            bail!("command must be an object");
        };

        // The attributes oneof determines the command, like in the proxy's
        // built-in policy; commandType is only a fallback.
        let attributes = command.iter().find_map(|(key, value)| {
            let name = key
                .strip_suffix("CommandAttributes")
                .or_else(|| key.strip_suffix("_command_attributes"))?;
            Some((pascal_case(name), value))
        });
        let Some((action, attributes)) = attributes.or_else(|| {
            let command_type = command
                .get("commandType")
                .or_else(|| command.get("command_type"))?
                .as_str()?;
            let name = command_type
                .strip_prefix("COMMAND_TYPE_")
                .unwrap_or(command_type);
            Some((pascal_case(&name.to_ascii_lowercase()), &Value::Null))
        }) else {
            bail!("command has neither attributes nor a commandType");
        };

        let nested_name = |camel: &str, snake: &str| {
            field(attributes, camel, snake)
                .and_then(|v| v.get("name"))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned()
        };
        Ok(Self {
            target_task_queue: nested_name("taskQueue", "task_queue"),
            target_namespace: string_field(attributes, "namespace", "namespace"),
            activity_type: nested_name("activityType", "activity_type"),
            workflow_type: nested_name("workflowType", "workflow_type"),
            action,
        })
    }

    fn context(&self) -> anyhow::Result<Context> {
        Ok(Context::from_pairs([
            (
                "targetTaskQueue".to_owned(),
                RestrictedExpression::new_string(self.target_task_queue.clone()),
            ),
            (
                "targetNamespace".to_owned(),
                RestrictedExpression::new_string(self.target_namespace.clone()),
            ),
            (
                "activityType".to_owned(),
                RestrictedExpression::new_string(self.activity_type.clone()),
            ),
            (
                "workflowType".to_owned(),
                RestrictedExpression::new_string(self.workflow_type.clone()),
            ),
        ])?)
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

/// Converts `scheduleActivityTask` or `schedule_activity_task` to
/// `ScheduleActivityTask`.
fn pascal_case(name: &str) -> String {
    name.split('_')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            chars
                .next()
                .map(|c| c.to_ascii_uppercase().to_string() + chars.as_str())
                .unwrap_or_default()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const BUILTIN_POLICY: &str = include_str!("../policies/builtin.cedar");
    const SCHEMA: &str = include_str!("../policies/schema.cedarschema");

    fn engine() -> PolicyEngine {
        PolicyEngine::new(BUILTIN_POLICY, Some(SCHEMA))
            .expect("builtin policy validates against schema")
    }

    fn verify(commands: Value) -> Verdict {
        engine()
            .verify(&json!({
                "namespace": "ns-a",
                "taskQueue": "queue-a",
                "taskQueues": ["queue-a", "queue-b"],
                "subject": "fleet-a",
                "commands": commands,
            }))
            .expect("valid input")
    }

    fn assert_allowed(commands: Value) {
        let verdict = verify(commands);
        assert!(verdict.allowed, "expected allow, got {verdict:?}");
    }

    fn assert_denied(commands: Value, policy: &str) {
        let verdict = verify(commands);
        assert!(!verdict.allowed, "expected deny");
        assert!(
            verdict.reason.contains(policy),
            "reason {:?} should name policy {policy}",
            verdict.reason
        );
    }

    fn schedule_activity(task_queue: &str) -> Value {
        json!([{
            "commandType": "COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK",
            "scheduleActivityTaskCommandAttributes": {
                "activityId": "1",
                "activityType": {"name": "Echo"},
                "taskQueue": {"name": task_queue, "kind": "TASK_QUEUE_KIND_NORMAL"},
                "retryPolicy": {"backoffCoefficient": 2.0}
            }
        }])
    }

    fn start_child(namespace: Option<&str>, task_queue: &str) -> Value {
        let mut attributes =
            json!({"workflowType": {"name": "Child"}, "taskQueue": {"name": task_queue}});
        if let Some(ns) = namespace {
            attributes["namespace"] = json!(ns);
        }
        json!([{"startChildWorkflowExecutionCommandAttributes": attributes}])
    }

    #[test]
    fn schedule_activity_task() {
        assert_allowed(schedule_activity("queue-a"));
        assert_denied(
            schedule_activity("queue-b"),
            "activities-stay-on-token-queue",
        );
        assert_denied(schedule_activity(""), "activities-stay-on-token-queue");
    }

    #[test]
    fn start_child_workflow_execution() {
        assert_allowed(start_child(Some("ns-a"), "queue-a"));
        assert_denied(
            start_child(Some("ns-a"), "queue-b"),
            "child-workflows-stay-on-token-queue-and-namespace",
        );
        assert_denied(
            start_child(Some("ns-b"), "queue-a"),
            "child-workflows-stay-on-token-queue-and-namespace",
        );
        // An unset namespace means "same as parent".
        assert_allowed(start_child(None, "queue-a"));
    }

    #[test]
    fn continue_as_new_workflow_execution() {
        // An unset task queue means "same queue".
        assert_allowed(json!([{"continueAsNewWorkflowExecutionCommandAttributes": {}}]));
        assert_denied(
            json!([{"continueAsNewWorkflowExecutionCommandAttributes": {"taskQueue": {"name": "queue-b"}}}]),
            "continue-as-new-stays-on-token-queue",
        );
    }

    #[test]
    fn untargeted_commands_pass_through() {
        assert_allowed(json!([
            {"recordMarkerCommandAttributes": {"markerName": "m"}},
            {"completeWorkflowExecutionCommandAttributes": {}},
            {"commandType": "COMMAND_TYPE_START_TIMER"},
        ]));
    }

    #[test]
    fn no_commands() {
        assert_allowed(json!([]));
        assert!(
            engine()
                .verify(&json!({"namespace": "ns-a", "taskQueue": "queue-a"}))
                .unwrap()
                .allowed
        );
    }

    #[test]
    fn first_violation_denies_whole_call() {
        let mut commands = schedule_activity("queue-a");
        commands
            .as_array_mut()
            .unwrap()
            .extend(schedule_activity("queue-b").as_array().unwrap().clone());
        assert_denied(commands, "activities-stay-on-token-queue");
    }

    #[test]
    fn accepts_proto_field_names() {
        let verdict = engine()
            .verify(&json!({
                "namespace": "ns-a",
                "task_queue": "queue-a",
                "commands": [{"schedule_activity_task_command_attributes": {"task_queue": {"name": "queue-b"}}}],
            }))
            .unwrap();
        assert!(!verdict.allowed);
    }

    #[test]
    fn default_deny_without_permit() {
        let engine = PolicyEngine::new("", None).unwrap();
        let verdict = engine
            .verify(&json!({"namespace": "ns-a", "taskQueue": "queue-a", "commands": [{"commandType": "COMMAND_TYPE_START_TIMER"}]}))
            .unwrap();
        assert_eq!(
            verdict,
            Verdict {
                allowed: false,
                reason: "StartTimer not permitted by any Cedar policy".to_owned()
            }
        );
    }

    #[test]
    fn activity_type_allowlist_example() {
        // The extension example from README.md, layered on the built-in policy.
        let policies = format!(
            "{BUILTIN_POLICY}\n{}",
            r#"@id("allowed-activity-types")
            forbid (principal, action == Action::"ScheduleActivityTask", resource)
            unless { ["Echo", "SendEmail"].contains(context.activityType) };"#
        );
        let engine = PolicyEngine::new(&policies, Some(SCHEMA)).expect("example validates");
        let request = |activity_type: &str| {
            json!({
                "namespace": "ns-a",
                "taskQueue": "queue-a",
                "commands": [{"scheduleActivityTaskCommandAttributes": {
                    "activityType": {"name": activity_type},
                    "taskQueue": {"name": "queue-a"},
                }}],
            })
        };
        assert!(engine.verify(&request("Echo")).unwrap().allowed);
        let verdict = engine.verify(&request("DropTables")).unwrap();
        assert!(!verdict.allowed && verdict.reason.contains("allowed-activity-types"));
    }

    #[test]
    fn invalid_policy_is_rejected_by_schema() {
        let err = PolicyEngine::new(
            r#"permit (principal, action, resource) when { context.nope == "" };"#,
            Some(SCHEMA),
        );
        assert!(err.is_err());
    }

    #[test]
    fn malformed_input() {
        assert!(engine().verify(&json!({"commands": "nope"})).is_err());
        assert!(engine().verify(&json!({"commands": [42]})).is_err());
        assert!(engine().verify(&json!({"commands": [{"foo": 1}]})).is_err());
    }
}
