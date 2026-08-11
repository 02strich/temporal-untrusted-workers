# temporal-untrusted-workers

A gRPC proxy that lets **untrusted workers** connect to a Temporal server safely.

A normal Temporal client can call the entire WorkflowService API — start and terminate workflows,
send signals, read history across a namespace, and so on. That is far too much authority to hand to
a worker fleet you do not fully trust (a customer's workers, workers in a less-trusted network, a
multi-tenant pool). This proxy sits between such workers and the real Temporal server and forwards
**only** the RPCs a worker needs to process tasks, pinning each authenticated worker identity to a
single `namespace` + `task queue`.

```
untrusted workers ──gRPC──▶  temporal-proxy  ──gRPC──▶  Temporal server / Temporal Cloud
     (API key or JWT)        (allowlist + auth +          (real credentials held only
                              scoping + token cache)        by the proxy)
```

The proxy holds the real upstream credentials; workers never see them. Workers authenticate to the
proxy with their own weaker credential (a static API key, or a Google-signed JWT such as a Cloud Run
identity token).

---

## How it works

Everything is enforced by a single gRPC unary interceptor (`internal/proxy/interceptor.go`) that runs
before any request is forwarded. For each incoming call it:

1. **Allowlists the RPC.** Only the methods in `internal/rpcpolicy/allowlist.go` are permitted; every
   other RPC is rejected with `PermissionDenied` before a handler runs. The allowed set is the
   worker task-processing surface:
   - `PollWorkflowTaskQueue`, `PollActivityTaskQueue`
   - `RespondWorkflowTaskCompleted`, `RespondWorkflowTaskFailed`
   - `RespondActivityTaskCompleted`, `RespondActivityTaskFailed`, `RespondActivityTaskCanceled`
   - `RecordActivityTaskHeartbeat`, `RespondQueryTaskCompleted`
   - `GetSystemInfo`, `DescribeNamespace` (needed by the SDK at worker startup)
   - `ShutdownWorker`

2. **Authenticates** the caller from the `authorization: Bearer <credential>` metadata (the same
   convention the Temporal SDK's API-key credential uses) and resolves it to an `Identity`
   (`namespace`, `task_queue`, `subject`). See [Authentication modes](#authentication-modes).

3. **Scopes** the request to that identity (`internal/scope`):
   - Poll RPCs must target the identity's namespace + task queue (sticky queues are authorized by
     their self-declared normal-queue name).
   - Token RPCs (Respond*/Heartbeat) must present a task token the proxy previously handed out for
     this identity — tracked in an in-memory **token cache** (`internal/tokencache`).
   - `RespondWorkflowTaskCompleted` additionally has every emitted **command** validated so a
     workflow cannot schedule activities / child workflows / continue-as-new onto another queue or
     namespace.

4. **Logs billable actions.** On success, the proxy emits a `cloud action consumed` log line per
   billable [Temporal Cloud action](https://docs.temporal.io/cloud/actions) it can observe in worker
   traffic (commands in `RespondWorkflowTaskCompleted`, plus activity heartbeats), attributed to the
   identity. See `internal/actions`. This is a worker-attributable *lower bound* — client-initiated
   actions (workflow starts, signals, queries, schedules, resets) never traverse the proxy.

---

## Authentication

Workers authenticate to the proxy using one of two modes, selected at startup with
`TEMPORAL_PROXY_AUTH_MODE`. Both resolve a credential to the same identity table, loaded once at
startup from a single JSON **auth file**.

### The auth file

One file holds both credential tables; a deployment uses only the section that matches its auth mode.

```json
{
  "keys": {
    "wk_live_abc123...": { "namespace": "default", "task_queue": "my-queue", "subject": "fleet-a" }
  },
  "emails": {
    "worker@my-project.iam.gserviceaccount.com": { "namespace": "default", "task_queue": "my-queue", "subject": "fleet-a" }
  }
}
```

- `namespace` and `task_queue` are required per entry; `subject` is an optional label used only in logs.
- API keys are stored hashed (sha256) in memory; emails are stored as-is.
- A default file is baked into the proxy image at build time (`cmd/temporal-proxy/kodata/static-auth.json`,
  exposed at runtime via `KO_DATA_PATH`), so the container starts out of the box. Mount your own file
  over it, or point `TEMPORAL_PROXY_STATIC_AUTH_FILE` elsewhere, to authorize real workers.

### Modes

| Mode | `TEMPORAL_PROXY_AUTH_MODE` | Credential presented by the worker | Resolved via |
|------|---------------------------|------------------------------------|--------------|
| **static** (default) | `static` | A shared API key string | the `keys` table |
| **jwt** | `jwt` | A Google-signed ID token (e.g. a Cloud Run identity token) | the `email` claim → the `emails` table |

In `jwt` mode the proxy verifies the token's signature against Google's public certs, checks its
audience against `TEMPORAL_PROXY_JWT_AUDIENCE`, requires `email_verified`, and maps the `email` claim
to the `emails` table. `TEMPORAL_PROXY_JWT_AUDIENCE` is required in this mode.

---

## Configuration (proxy)

All configuration is via environment variables (`internal/config/config.go`).

### Worker-facing (downstream)

| Variable | Default | Description |
|----------|---------|-------------|
| `TEMPORAL_PROXY_LISTEN_ADDR` | `127.0.0.1:7243` | Address the proxy listens on. Use `0.0.0.0:8080` on Cloud Run. |
| `TEMPORAL_PROXY_AUTH_MODE` | `static` | `static` or `jwt`. |
| `TEMPORAL_PROXY_STATIC_AUTH_FILE` | baked-in default | Path to the unified auth file. |
| `TEMPORAL_PROXY_JWT_AUDIENCE` | — | Required in `jwt` mode; expected `aud` of the worker's ID token. |
| `TEMPORAL_PROXY_DOWNSTREAM_TLS_MODE` | `plaintext` | `plaintext` or `tls`. Leave `plaintext` behind Cloud Run (which terminates TLS). |
| `TEMPORAL_PROXY_DOWNSTREAM_TLS_CERT_FILE` / `..._KEY_FILE` | — | Required when downstream TLS mode is `tls`. |

### Upstream (proxy → Temporal)

| Variable | Default | Description |
|----------|---------|-------------|
| `TEMPORAL_PROXY_UPSTREAM_ADDR` | `127.0.0.1:7233` | The real Temporal frontend (e.g. `<ns>.<acct>.tmprl.cloud:7233`). |
| `TEMPORAL_PROXY_UPSTREAM_AUTH_MODE` | `none` | `none` or `api-key`. |
| `TEMPORAL_PROXY_UPSTREAM_API_KEY` | — | Required when upstream auth mode is `api-key`. Keep in a secret. |
| `TEMPORAL_PROXY_UPSTREAM_TLS_MODE` | `plaintext` | `plaintext` or `tls`. Use `tls` for Temporal Cloud. |
| `TEMPORAL_PROXY_UPSTREAM_TLS_CA_FILE` | system roots | Custom CA bundle. Empty = system roots (correct for Temporal Cloud). |
| `TEMPORAL_PROXY_UPSTREAM_TLS_SERVER_NAME` | — | Override SNI / cert hostname. |
| `TEMPORAL_PROXY_UPSTREAM_TLS_SKIP_VERIFY` | `false` | Dev only — disables cert verification. |

### Other

| Variable | Default | Description |
|----------|---------|-------------|
| `TEMPORAL_PROXY_TOKEN_CACHE_TTL` | `1h` | How long an issued task token stays valid in the cache. |
| `TEMPORAL_PROXY_TOKEN_CACHE_MAX_SIZE` | `100000` | Max task tokens cached (per instance). |
| `TEMPORAL_PROXY_LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error`. Action logs are at `info`. |

---

## The verify examples

`examples/` is a separate Go module (`examples/go.mod`) holding two small programs used to exercise
the proxy end-to-end. They are also the reference for how a real worker and control-plane client
interact with the proxy.

- **`examples/verify-worker`** — a minimal Temporal SDK worker that connects **through the proxy**
  using an API-key/JWT credential, exactly as an untrusted worker would. It registers `EchoWorkflow`
  (schedules an activity) and `CrossQueueWorkflow` (deliberately targets a forbidden queue so the
  proxy's command validation is observable). On startup it also fetches and logs its Cloud Run
  identity token if one is available.
- **`examples/verify-client`** — a **direct** (non-proxied) client that talks straight to the real
  Temporal server to start the workflow (`StartWorkflowExecution` is *not* an allowed proxy RPC), then
  awaits the result produced by the proxied worker, and asserts the cross-queue command was blocked.

### Verify env vars

`verify-worker`:

| Variable | Default | Description |
|----------|---------|-------------|
| `VERIFY_PROXY_ADDR` | `127.0.0.1:7243` | The proxy address to connect to. |
| `VERIFY_NAMESPACE` | `default` | Namespace (must match the identity's). |
| `VERIFY_TASK_QUEUE` | `proxy-test-queue` | Task queue (must match the identity's). |
| `VERIFY_AUTH_MODE` | `static` | `static` (use `VERIFY_API_KEY`) or `jwt` (use the Cloud Run identity token). Mirrors the proxy. |
| `VERIFY_API_KEY` | — | Required in `static` mode. |
| `VERIFY_CLOUDRUN_TOKEN_AUDIENCE` | `$VERIFY_PROXY_ADDR` | Audience requested for the Cloud Run identity token; match the proxy's `TEMPORAL_PROXY_JWT_AUDIENCE`. |
| `VERIFY_TLS_MODE` | `plaintext` | `plaintext` or `tls`. Use `tls` to reach a Cloud Run proxy. Also `VERIFY_TLS_CA_FILE`, `VERIFY_TLS_SERVER_NAME`, `VERIFY_TLS_SKIP_VERIFY`. |

`verify-client` uses `VERIFY_UPSTREAM_ADDR` (default `127.0.0.1:7233`), `VERIFY_NAMESPACE`,
`VERIFY_TASK_QUEUE`, and the same `VERIFY_TLS_*` set.

---

## Local development

```bash
# 1. A local Temporal dev server (frontend on :7233)
temporal server start-dev

# 2. The proxy: upstream = dev server, downstream = :7243, static auth from testdata
TEMPORAL_PROXY_UPSTREAM_ADDR=127.0.0.1:7233 \
TEMPORAL_PROXY_LISTEN_ADDR=127.0.0.1:7243 \
TEMPORAL_PROXY_STATIC_AUTH_FILE=testdata/static-auth.json \
go run ./cmd/temporal-proxy

# 3. The worker, through the proxy (testkey123 → default / proxy-test-queue in the testdata file)
cd examples && VERIFY_API_KEY=testkey123 go run ./verify-worker

# 4. Drive a workflow directly against the dev server and assert end-to-end behavior
cd examples && go run ./verify-client
```

---

## Building container images

Images are built and pushed with [`ko`](https://ko.build) (no Dockerfile). See the `Makefile`.

```bash
# install ko if needed
make ko-install

# build & push both images under one repo
make images KO_DOCKER_REPO=ghcr.io/you/repo

# or individually, with a tag
make image-proxy         KO_DOCKER_REPO=ghcr.io/you/repo TAGS=v0.1.0
make image-verify-worker KO_DOCKER_REPO=ghcr.io/you/repo TAGS=v0.1.0
```

This publishes `$(KO_DOCKER_REPO)/temporal-proxy` and `$(KO_DOCKER_REPO)/verify-worker` (multi-arch
`linux/amd64,linux/arm64` by default). `verify-worker` builds from the `examples/` module.

---

## Deploying on Cloud Run

A natural topology: run the **proxy as a Cloud Run service** (it serves gRPC) and the
**verify-worker as a Cloud Run worker pool** (it polls outbound and serves nothing). The JWT auth
mode is designed for this — the worker pool's service-account identity token *is* the credential the
proxy validates.

### 1. Proxy — a Cloud Run **service**

```bash
gcloud run deploy temporal-proxy \
  --image=ghcr.io/you/repo/temporal-proxy:v0.1.0 \
  --use-http2 \
  --allow-unauthenticated \
  --min-instances=1 --max-instances=1 \
  --port=8080 \
  --set-env-vars=TEMPORAL_PROXY_LISTEN_ADDR=0.0.0.0:8080 \
  --set-env-vars=TEMPORAL_PROXY_AUTH_MODE=jwt \
  --set-env-vars=TEMPORAL_PROXY_JWT_AUDIENCE=https://temporal-proxy-xxxx.run.app \
  --set-env-vars=TEMPORAL_PROXY_UPSTREAM_ADDR=<ns>.<acct>.tmprl.cloud:7233 \
  --set-env-vars=TEMPORAL_PROXY_UPSTREAM_TLS_MODE=tls \
  --set-env-vars=TEMPORAL_PROXY_UPSTREAM_AUTH_MODE=api-key \
  --set-secrets=TEMPORAL_PROXY_UPSTREAM_API_KEY=temporal-api-key:latest \
  --set-secrets=/etc/temporal/auth.json=proxy-auth-file:latest \
  --set-env-vars=TEMPORAL_PROXY_STATIC_AUTH_FILE=/etc/temporal/auth.json
```

Cloud Run specifics that matter here:

- **`--use-http2`** — the proxy speaks gRPC (HTTP/2). Cloud Run terminates public TLS and forwards
  cleartext HTTP/2 (h2c) to the container, so keep `TEMPORAL_PROXY_DOWNSTREAM_TLS_MODE=plaintext` and
  listen on `0.0.0.0:8080`.
- **`--allow-unauthenticated`** — the proxy does its *own* application-level auth. Cloud Run's IAM
  invoker auth also consumes the `Authorization: Bearer …` header, which would collide with the
  API-key/JWT the worker sends there. Let the proxy authenticate instead of Cloud Run IAM. (In `jwt`
  mode the proxy is effectively validating a Google identity token itself — the same kind of token
  Cloud Run IAM would check — but mapping its email to a namespace/queue rather than an IAM role.)
- **`--min-instances=1 --max-instances=1`** — the task-token cache is **in-memory and per-instance**.
  A worker's `Poll` (which caches a token) and its follow-up `Respond` (which presents it) must land
  on the same instance. Pinning to a single instance guarantees that. Scaling horizontally would
  require sticky routing or a shared token store, which is not implemented — treat single-instance as
  the supported configuration.
- The auth file and upstream API key are injected from **Secret Manager** (a mounted secret volume
  and an env secret, respectively).

### 2. verify-worker — a Cloud Run **worker pool**

Worker pools run pull-based workloads that don't serve requests — a perfect fit for a Temporal worker
that polls outbound. Deploy it with its own service account whose email is listed in the proxy's
`emails` table.

```bash
gcloud run worker-pools deploy verify-worker \
  --image=ghcr.io/you/repo/verify-worker:v0.1.0 \
  --service-account=worker@my-project.iam.gserviceaccount.com \
  --set-env-vars=VERIFY_PROXY_ADDR=temporal-proxy-xxxx.run.app:443 \
  --set-env-vars=VERIFY_TLS_MODE=tls \
  --set-env-vars=VERIFY_AUTH_MODE=jwt \
  --set-env-vars=VERIFY_CLOUDRUN_TOKEN_AUDIENCE=https://temporal-proxy-xxxx.run.app \
  --set-env-vars=VERIFY_NAMESPACE=default \
  --set-env-vars=VERIFY_TASK_QUEUE=proxy-test-queue
```

Key points:

- **JWT auth end-to-end:** in `jwt` mode the worker fetches its service-account identity token from
  the GCP metadata server and presents it as the credential. `VERIFY_CLOUDRUN_TOKEN_AUDIENCE` (the
  audience it requests) **must equal** the proxy's `TEMPORAL_PROXY_JWT_AUDIENCE`, and the worker's SA
  email must be an entry in the proxy's `emails` table. No shared secret is exchanged.
- **TLS to the proxy:** the worker connects to the Cloud Run service's public TLS endpoint on `:443`,
  so set `VERIFY_TLS_MODE=tls` (Cloud Run presents a publicly-trusted cert; system roots suffice).
- To scale the fleet, raise the worker pool's instance count. Multiple worker instances are fine —
  the per-instance token cache constraint applies only to the **proxy** service.
- Swap `verify-worker` for your own worker image the same way; the proxy is worker-agnostic (it only
  depends on `go.temporal.io/api`, so any SDK/language that speaks the same gRPC surface works).
