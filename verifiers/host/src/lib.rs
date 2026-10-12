//! Hosts a temporal-proxy tool-call verifier (TEMPORAL_PROXY_TOOL_VERIFIER=nexus)
//! as a Nexus service, shared by the verifiers in this workspace.
//!
//! The Temporal Rust SDK has no Nexus handler API yet, so this runs a
//! Nexus-only worker directly on temporalio-sdk-core: it polls Nexus tasks,
//! hands each `VerifyToolCalls` and `RecordInputEvents` start request to a
//! [`Verifier`], and completes it synchronously.

pub mod proto;

use std::{collections::HashMap, env, sync::Arc, time::Duration};

use anyhow::{Context as _, anyhow};
use temporalio_client::{Connection, ConnectionOptions, TlsOptions};
use temporalio_common::{
    protos::{
        coresdk::nexus::{NexusTask, NexusTaskCompletion, nexus_task, nexus_task_completion},
        temporal::api::{
            common::v1::Payload,
            nexus::v1::{
                Failure, HandlerError, Response, StartOperationRequest, StartOperationResponse,
                request, response, start_operation_response,
            },
        },
    },
    worker::WorkerTaskTypes,
};
use temporalio_sdk_core::{
    CoreRuntime, PollError, RuntimeOptions, Url, Worker, WorkerConfig, WorkerVersioningStrategy,
    init_worker,
};
use tokio::task::JoinSet;

pub use proto::{Decision, Verdict};

/// How long to wait for a graceful worker shutdown before exiting anyway.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

const VERIFY_RESPONSE_MESSAGE_TYPE: &str =
    "temporal_untrusted_workers.toolpolicy.v1.VerifyToolCallsResponse";
const RECORD_RESPONSE_MESSAGE_TYPE: &str =
    "temporal_untrusted_workers.toolpolicy.v1.RecordInputEventsResponse";

/// Decides tool calls. Both methods take the request in the protobuf JSON
/// mapping; an error means it could not be interpreted, and is answered with
/// a Nexus `BAD_REQUEST` handler error, which the proxy treats as "verifier
/// unavailable".
pub trait Verifier: Send + Sync + 'static {
    /// Decides a `VerifyToolCallsRequest`.
    fn verify(&self, request: &serde_json::Value) -> anyhow::Result<Verdict>;

    /// Takes a `RecordInputEventsRequest`'s events, to use as context for the
    /// run's later tool calls. Returns the number of new events.
    fn record(&self, request: &serde_json::Value) -> anyhow::Result<usize>;
}

/// The settings every verifier shares, from the environment.
pub struct Config {
    pub address: String,
    pub namespace: String,
    pub api_key: Option<String>,
    pub task_queue: String,
    pub service: String,
    pub verify_operation: String,
    pub record_operation: String,
    pub input_max_runs: usize,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            address: env_or("TEMPORAL_ADDRESS", "http://127.0.0.1:7233"),
            namespace: env_or("TEMPORAL_NAMESPACE", "default"),
            api_key: env::var("TEMPORAL_API_KEY").ok().filter(|k| !k.is_empty()),
            task_queue: env_or("VERIFIER_TASK_QUEUE", "tool-verifier"),
            service: env_or("VERIFIER_SERVICE", "tool-policy"),
            verify_operation: env_or("VERIFIER_VERIFY_OPERATION", "VerifyToolCalls"),
            record_operation: env_or("VERIFIER_RECORD_OPERATION", "RecordInputEvents"),
            input_max_runs: env_or("VERIFIER_INPUT_MAX_RUNS", "10000")
                .parse()
                .unwrap_or(10_000),
        }
    }
}

/// The environment variable `key`, or `default` if it is unset.
pub fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_owned())
}

/// Logs to stderr, filtered by RUST_LOG (default `info`).
pub fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
}

/// Serves `verifier` until Ctrl-C or SIGTERM, then drains gracefully. `name`
/// is used in the worker identity and logs (e.g. "cedar-verifier").
pub async fn run(cfg: Config, name: &str, verifier: impl Verifier) -> anyhow::Result<()> {
    let runtime = CoreRuntime::new_assume_tokio(RuntimeOptions::default())?;
    let worker = connect_worker(&cfg, name, &runtime).await?;
    tracing::info!(
        address = %cfg.address,
        namespace = %cfg.namespace,
        task_queue = %cfg.task_queue,
        service = %cfg.service,
        verify_operation = %cfg.verify_operation,
        record_operation = %cfg.record_operation,
        "{name} running"
    );

    let handler = Arc::new(Handler {
        verifier,
        service: cfg.service,
        verify_operation: cfg.verify_operation,
        record_operation: cfg.record_operation,
    });
    let worker = Arc::new(worker);
    // Only a weak handle stays here, so serve() can take sole ownership of the
    // worker for finalize_shutdown.
    let shutdown_handle = Arc::downgrade(&worker);
    let serve = serve(worker, handler);
    tokio::pin!(serve);

    tokio::select! {
        result = &mut serve => return result,
        () = shutdown_signal() => {}
    }
    tracing::info!("shutting down");
    if let Some(worker) = shutdown_handle.upgrade() {
        worker.initiate_shutdown();
    }
    // Draining normally takes well under a second, but the server does not
    // always release an outstanding Nexus long poll on ShutdownWorker. Don't
    // hang on it: a verification left unanswered is retried by the server.
    tokio::select! {
        result = &mut serve => result,
        () = tokio::time::sleep(SHUTDOWN_TIMEOUT) => {
            tracing::warn!(timeout = ?SHUTDOWN_TIMEOUT, "graceful shutdown timed out; exiting");
            Ok(())
        }
        () = shutdown_signal() => {
            tracing::warn!("interrupted again; exiting without waiting for graceful shutdown");
            Ok(())
        }
    }
}

/// Handles one Nexus task; shared by all task-handling Tokio tasks.
struct Handler<V> {
    verifier: V,
    service: String,
    verify_operation: String,
    record_operation: String,
}

/// Polls Nexus tasks and handles each on its own Tokio task until the worker
/// shuts down, then waits for in-flight tasks and finalizes the worker.
async fn serve<V: Verifier>(worker: Arc<Worker>, handler: Arc<Handler<V>>) -> anyhow::Result<()> {
    let mut in_flight = JoinSet::new();
    loop {
        let task = match worker.poll_nexus_task().await {
            Ok(task) => task,
            Err(PollError::ShutDown) => break,
            Err(err) => return Err(anyhow!(err).context("polling nexus task")),
        };
        // Reap finished handlers so the set doesn't grow without bound.
        while in_flight.try_join_next().is_some() {}
        let (worker, handler) = (worker.clone(), handler.clone());
        in_flight.spawn(async move { handle(&worker, handler, task).await });
    }

    while in_flight.join_next().await.is_some() {}
    let worker =
        Arc::try_unwrap(worker).map_err(|_| anyhow!("worker still referenced at shutdown"))?;
    worker.finalize_shutdown().await;
    Ok(())
}

/// Resolves on Ctrl-C or, on Unix, SIGTERM (what container runtimes send).
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("installing SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}

async fn connect_worker(cfg: &Config, name: &str, runtime: &CoreRuntime) -> anyhow::Result<Worker> {
    let url = Url::parse(&cfg.address)
        .with_context(|| format!("parsing TEMPORAL_ADDRESS {}", cfg.address))?;
    let options = ConnectionOptions::new(url)
        .identity(format!("{name}@{}", std::process::id()))
        .maybe_api_key(cfg.api_key.clone())
        .maybe_tls_options(cfg.api_key.as_ref().map(|_| TlsOptions::default()))
        .build();
    let connection = Connection::connect(options)
        .await
        .context("connecting to Temporal")?;

    let worker_config = WorkerConfig::builder()
        .namespace(cfg.namespace.clone())
        .task_queue(cfg.task_queue.clone())
        .task_types(WorkerTaskTypes {
            enable_workflows: false,
            enable_local_activities: false,
            enable_remote_activities: false,
            enable_nexus: true,
        })
        .versioning_strategy(WorkerVersioningStrategy::None {
            build_id: String::new(),
        })
        .build()
        .map_err(|e| anyhow!("invalid worker config: {e}"))?;
    init_worker(runtime, worker_config, connection)
}

async fn handle<V: Verifier>(worker: &Worker, handler: Arc<Handler<V>>, task: NexusTask) {
    let poll = match task.variant {
        Some(nexus_task::Variant::Task(poll)) => poll,
        // Core tells us when a task it handed out timed out or was
        // cancelled; verification is synchronous, so there's nothing to
        // stop.
        Some(nexus_task::Variant::CancelTask(cancel)) => {
            tracing::debug!(reason = cancel.reason, "nexus task cancelled");
            return;
        }
        None => return,
    };
    let status = match poll.request.and_then(|r| r.variant) {
        // Verifiers may block (e.g. on disk), so keep them off the async
        // worker threads.
        Some(request::Variant::StartOperation(start)) => {
            tokio::task::spawn_blocking(move || handler.start_operation(start))
                .await
                .unwrap_or_else(|err| {
                    handler_error("INTERNAL", &format!("verifier panicked: {err}"))
                })
        }
        Some(request::Variant::CancelOperation(_)) => handler_error(
            "NOT_IMPLEMENTED",
            "VerifyToolCalls is synchronous and cannot be cancelled",
        ),
        None => handler_error("BAD_REQUEST", "nexus task has no request"),
    };
    let completion = NexusTaskCompletion {
        task_token: poll.task_token,
        status: Some(status),
    };
    if let Err(err) = worker.complete_nexus_task(completion).await {
        tracing::warn!(error = %err, "completing nexus task failed");
    }
}

impl<V: Verifier> Handler<V> {
    fn start_operation(&self, start: StartOperationRequest) -> nexus_task_completion::Status {
        if start.service == self.service && start.operation == self.verify_operation {
            return self.verify_tool_calls(start);
        }
        if start.service == self.service && start.operation == self.record_operation {
            return self.record_input_events(start);
        }
        handler_error(
            "NOT_FOUND",
            &format!("unknown operation {}/{}", start.service, start.operation),
        )
    }

    /// Decides a `VerifyToolCallsRequest`.
    fn verify_tool_calls(&self, start: StartOperationRequest) -> nexus_task_completion::Status {
        let input = start.payload.map(|p| p.data).unwrap_or_default();
        let verdict = match serde_json::from_slice::<serde_json::Value>(&input)
            .map_err(anyhow::Error::from)
            .and_then(|request| self.verifier.verify(&request))
        {
            Ok(verdict) => verdict,
            Err(err) => {
                tracing::warn!(error = %format!("{err:#}"), "rejecting malformed tool calls");
                return handler_error(
                    "BAD_REQUEST",
                    &format!("invalid VerifyToolCallsRequest: {err:#}"),
                );
            }
        };
        if !verdict.allowed() {
            tracing::info!(
                decision = verdict.decision.proto_name(),
                reason = %verdict.reason,
                "tool calls not approved"
            );
        }
        let body = serde_json::json!({
            "decision": verdict.decision.proto_name(),
            "reason": verdict.reason,
        });
        sync_success(json_proto_payload(&body, VERIFY_RESPONSE_MESSAGE_TYPE))
    }

    /// Hands a `RecordInputEventsRequest`'s events to the verifier. They are
    /// never judged, only used as context for the run's later tool calls.
    fn record_input_events(&self, start: StartOperationRequest) -> nexus_task_completion::Status {
        let input = start.payload.map(|p| p.data).unwrap_or_default();
        match serde_json::from_slice::<serde_json::Value>(&input)
            .map_err(anyhow::Error::from)
            .and_then(|request| self.verifier.record(&request))
        {
            Ok(added) => {
                tracing::debug!(added, "input events recorded");
                sync_success(json_proto_payload(
                    &serde_json::json!({}),
                    RECORD_RESPONSE_MESSAGE_TYPE,
                ))
            }
            Err(err) => {
                tracing::warn!(error = %format!("{err:#}"), "rejecting malformed input events");
                handler_error(
                    "BAD_REQUEST",
                    &format!("invalid RecordInputEventsRequest: {err:#}"),
                )
            }
        }
    }
}

fn sync_success(payload: Payload) -> nexus_task_completion::Status {
    nexus_task_completion::Status::Completed(Response {
        variant: Some(response::Variant::StartOperation(StartOperationResponse {
            variant: Some(start_operation_response::Variant::SyncSuccess(
                start_operation_response::Sync {
                    payload: Some(payload),
                    links: vec![],
                },
            )),
        })),
    })
}

/// Encodes body (a message in the protobuf JSON mapping) as a "json/protobuf"
/// payload of message_type.
fn json_proto_payload(body: &serde_json::Value, message_type: &str) -> Payload {
    Payload {
        metadata: HashMap::from([
            ("encoding".to_owned(), b"json/protobuf".to_vec()),
            ("messageType".to_owned(), message_type.as_bytes().to_vec()),
        ]),
        data: body.to_string().into_bytes(),
        ..Default::default()
    }
}

/// A Nexus handler error. The proxy treats any failed operation as "verifier
/// unavailable" and fails closed.
fn handler_error(error_type: &str, message: &str) -> nexus_task_completion::Status {
    #[allow(deprecated)]
    nexus_task_completion::Status::Error(HandlerError {
        error_type: error_type.to_owned(),
        failure: Some(Failure {
            message: message.to_owned(),
            ..Default::default()
        }),
        ..Default::default()
    })
}
