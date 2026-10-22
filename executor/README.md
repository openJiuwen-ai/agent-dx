[中文](README.zh.md) | **English**

# agent-dx-executor

Platform-owned FaaS code package for custom-image Agent instances. It starts
the Executor HTTP Server and user processes from a standard YuanRong FaaS
initializer. The wheel is preinstalled into the Agent base image and is not a
user-facing SDK.

The Executor has no dependency on the agent-dx SDK. Sandbox capability is
provided by the `yr.agentexecutor.sandbox` subpackage — a ported copy of the
openYuanRong Runtime `yr.sandbox` SDK that reuses the native RPC
(`@yr.instance`/`yr.get`) of the image-bundled `yr` package to create and
operate remote child sandbox containers. Consequently `yr` is not listed as a
package dependency, but the image must still contain a compatible openYuanRong
Python Runtime — that runtime also loads the Executor's entries as an ordinary
FaaS code package. Packaging dependencies are `aiohttp`, `websockets`,
`httpx`, and `multidict` (needed by the sandbox tunnel components).

Install the Executor into the Python environment used by the YuanRong Runtime.
It is image infrastructure and does not use the function's
`DELEGATE_DOWNLOAD`; that remains available for an optional user process code
package. Build the image with, for example:

```text
python -m pip install --no-deps agent_dx_executor-<version>-py3-none-any.whl
```

The package supports Python 3.9 and later without a minor-version-specific
dependency. The wheel is deliberately rooted in Python's `platlib`, so the
`yr.agentexecutor` package is colocated with the native
openYuanRong Runtime on systems where `purelib` and `platlib` map to `lib` and
`lib64`, respectively. Current primary runtime targets are Python 3.9 and
Python 3.11.

FaaS entries:

```text
initializer: yr.agentexecutor.handler.initialize
handler:     yr.agentexecutor.handler.handle
pre-stop:    yr.agentexecutor.handler.pre_stop
```

## Sandbox API

At startup the Executor creates a `SandboxManager` and injects it into the
HTTP Server, which listens on `0.0.0.0:18093` by default (overridable with the
`YR_AGENT_EXECUTOR_HOST`/`YR_AGENT_EXECUTOR_PORT` environment variables) but
accepts only loopback-originated requests. Same-machine processes create,
operate, and destroy **remote child sandbox containers** over loopback HTTP —
the target is a separate child container, not the function-instance container
itself. Sandboxes are not precreated: the caller first creates one with
`POST /api/sandbox/v1/sandboxes`, receives a sandbox id, and puts it into the
URL path of subsequent requests; multiple sandboxes are isolated by id, each
with an independent lifecycle.

The API follows the platform yaml contracts (management plane `sandbox.yaml` +
data plane `data-plane.yaml`) and consists of two families:

- **Management plane** — `POST /api/sandbox/v1/sandboxes` (create) and
  `DELETE /api/sandbox/v1/sandboxes/{sandboxId}` (delete). Successes use the
  Envelope form (`data` is a Base64-encoded JSON payload); failures use the
  ErrorEnvelope form.
- **Data plane** — `POST /direct/{instanceId}/invoke`, a single
  action-dispatch endpoint carrying raw action JSON; action-level failures
  ride HTTP 200 with the error in the body.

The legacy `/v1/sandbox/sandboxes` prefix is **not registered**: every request
to it returns 404 `{"message": "endpoint not found"}`.

### Common conventions

- **Base address**: the sandbox API serves same-machine processes only and
  must be accessed via the loopback address
  `http://127.0.0.1:{port}/...`. Requests whose source IP is outside the
  loopback ranges (IPv4 `127.0.0.0/8`, IPv6 `::1`, and IPv4-mapped loopback
  addresses) are rejected with 403 — even a same-machine caller using the
  machine IP is refused:

  ```json
  {"message": "sandbox API is loopback-only"}
  ```

- **X-Request-Id (management plane)**: optional request identifier (the
  idempotency key: a retry must keep the same id and the same body). When
  present it is echoed in the `X-Request-Id` response header on success and
  error responses alike; when omitted the server generates a UUID and echoes
  it the same way. The effective id is also reported as `requestId` inside
  the envelope.
- **X-Trace-ID** (private convention, outside the contract): optional trace id
  on both planes. The adapter reads it and passes it to the underlying RPC,
  and reuses it for the agent enter/exit log pair. The old `trace_id` body
  field is rejected with 400 (`trace_id moved to X-Trace-ID header`).
- **Request format**: request bodies are JSON and must carry
  `Content-Type: application/json` (otherwise 400); the body may use either
  `Content-Length` or chunked transfer encoding. A body that is not valid
  JSON, or is not a JSON object, yields 400.
- **Size limits**: sandbox API requests and responses each have an
  independent default limit of 512 MiB; exceeding either returns 413. The
  limits are independent of the Frontend file API's `max_file_size`. Binary
  content travels hex-encoded (see the file actions), so its effective size
  is half the limit.
- **Concurrency**: the HTTP Server processes at most 64 concurrent requests
  globally (across all endpoints); beyond the limit it returns 503 and closes
  the connection:

  ```json
  {"message": "executor is busy"}
  ```

- **Management-plane error model**: every non-200 management-plane response
  is an ErrorEnvelope carrying a symbolic code, a retry hint and an outcome,
  plus the effective request id both in the header and the body:

  ```json
  {"code": 400, "message": "cpu must be an integer", "data": null,
   "error": {"code": "invalid", "retry": "never",
             "outcome": "not_started", "requestId": "req-create-013"}}
  ```

  | HTTP | error.code | retry | outcome | Trigger |
  |---|---|---|---|---|
  | 400 | `invalid` | `never` | `not_started` | Parameter type/value errors, wrong `Content-Type`, invalid JSON |
  | 404 | `not_found` | `never` | `terminal` | Unknown sandbox id, unregistered sub-resource |
  | 405 | `method_not_allowed` | `never` | `not_started` | Path exists but the method is not registered; the response carries `Allow` |
  | 413 | `too_large` | `never` | `not_started` | Request or response exceeds the size limit |
  | 500 | `internal` | `after_backoff` | `unknown` | Underlying RPC/IO failure |

- **Data-plane error model**: transport and argument errors (400 unknown
  action or bad args, 404 unknown instance or path shape, 405 with `Allow`,
  413, 500) return `{"error": "..."}` bodies. Action-level failures — the
  dispatch went through but the operation itself failed (missing file, RPC
  failure, ...) — ride HTTP 200 with the error inside the action result (see
  each action below).

### 1. Create sandbox: `POST /api/sandbox/v1/sandboxes`

```http
POST /api/sandbox/v1/sandboxes HTTP/1.1
Content-Type: application/json
X-Request-Id: req-create-013
X-Trace-ID: trace-abc

{
  "image": "python:3.12-slim",
  "runtime": "supervisor",
  "cpu": 2000,
  "memory": 4096,
  "env": {"PATH": "/usr/bin"},
  "ports": ["tcp:8080"],
  "idle_timeout": 600,
  "extra_config": {"working_dir": "/sandbox"}
}
```

Headers: `X-Request-Id` (optional, generated/echoed), `X-Trace-ID`
(optional, private tracing).

Body fields (the 13 contract fields the adapter consumes):

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `name` | string | no | none | Sandbox name; the instance is created and addressed under this name |
| `image` | string | no | none | Docker image name; effective only when `runtime="docker"` |
| `runtime` | string | no | `""` | Runtime/executor type, passed through to the underlying executor selection; an empty string keeps the backend default executor |
| `rootfs` | string or object | no | none | Root filesystem: a string is passed through as-is; an object `{type, imageurl, ...}` is assembled into the rootfs JSON with `mounts` merged in. Mutually exclusive with `image` |
| `cpu` | int | no | none | CPU resource, millicores |
| `memory` | int | no | none | Memory, MiB |
| `env` | object\<string,string\> | no | inherit sandbox runtime | Environment variables; when specified they apply wholesale (not merged with the default environment) |
| `ports` | array\<string\> | no | none | Port forwarding list; each element is `"proto:port"` (e.g. `"tcp:8080"`) or `"port"` |
| `idleTimeoutSeconds` | int | no | `300` | Idle timeout in seconds; must be non-negative (0 allowed). `idle_timeout` is accepted as a legacy alias |
| `mounts` | array\<object\> | no | none | Mount declarations; merged verbatim into the rootfs JSON `mounts` key and passed to the backend. Requires an object `rootfs` or `image` (a string `rootfs` cannot carry mounts) |
| `snapshotId` | string | no | none | Create from a snapshot: the request is routed to the restore branch (`Sandbox(checkpoint_id=...)`) and all other parameters are ignored |
| `tunnel` | object | no | none | `{enabled, proxyPort}`: `proxyPort` maps to the underlying proxy port; `enabled=true` requires `extra_config.upstream` (else 400); `enabled=false` explicitly disables the upstream-implied tunnel |
| `extra_config` | object | no | none | Runtime custom-key channel: the keys `upstream`/`working_dir`/`user` are extracted and mapped to the corresponding underlying fields; the remaining keys are passed through as a whole |

The executor type is carried by the top-level `runtime` field. The private
fields `upstream`/`working_dir`/`user` live inside `extra_config`, and
`trace_id` in the `X-Trace-ID` header: a body carrying any of them at the top
level — or `sandbox_type` inside `extra_config` — returns 400 with a hint
about the accepted location.

**Success (200)** — create is synchronous and blocking: a 200 means the child
sandbox is ready (creation includes a readiness verification), no polling
required. The response header echoes `X-Request-Id`; `data` is a Base64-encoded
JSON payload:

```json
{"code": 200, "message": "ok",
 "data": "eyJzYW5kYm94SWQiOiJzYngtMTIzIi..."}
```

The `data` payload decodes to:

```json
{"sandboxId": "34420000-0000-4000-886a-980025037460",
 "instanceId": "34420000-0000-4000-886a-980025037460",
 "status": "running", "requestId": "req-create-013"}
```

`sandboxId` and `instanceId` carry the same value (the platform instance id
returned by the underlying create); `status` is always `"running"` and
`requestId` is the effective request id. All subsequent operations put the id
into the URL path.

**Failure responses** use the ErrorEnvelope form, e.g.:

- 400 — invalid parameter type/value (`cpu must be an integer`,
  `idle_timeout must be non-negative`, `rootfs and image are mutually
  exclusive`, `tunnel.enabled=true requires extra_config.upstream`, ...).
- 500 — creation failed; `message` is `create sandbox failed: ` plus the
  underlying cause (`Failed to initialize sandbox instance: ...`). Note that
  `ports` element format (`proto:port`) is only validated at creation time,
  so an invalid element also surfaces as 500.

### 2. Delete sandbox: `DELETE /api/sandbox/v1/sandboxes/{sandboxId}`

```http
DELETE /api/sandbox/v1/sandboxes/34420000-0000-4000-886a-980025037460 HTTP/1.1
X-Request-Id: req-del-007
```

| Parameter | Type | Required | Description |
|---|---|---|---|
| `{sandboxId}` (path) | string | yes | Sandbox id, the `sandboxId`/`instanceId` returned by create |
| `X-Request-Id` (header) | string | no | Generated/echoed like on create |

No request body.

**Success (200)** — `data` decodes to `{"success": true}`:

```json
{"code": 200, "message": "ok", "data": "eyJzdWNjZXNzIjp0cnVlfQ=="}
```

**Sandbox not found (404)** — deleting an unknown or already-deleted sandbox
returns 404 with an ErrorEnvelope (changed from the legacy 200 +
`success:false`):

```json
{"code": 404, "message": "sandbox sbx-123 not found", "data": null,
 "error": {"code": "not_found", "retry": "never", "outcome": "terminal",
           "requestId": "req-del-007"}}
```

### 3. Invoke action: `POST /direct/{instanceId}/invoke`

A single data-plane endpoint dispatches the supported actions. The response
is raw action JSON (no envelope); action-level failures ride HTTP 200 with
the error in the body.

Common parameters:

| Parameter | Location | Required | Default | Description |
|---|---|---|---|---|
| `{instanceId}` | path | yes | — | Sandbox instance id |
| `X-ADX-Request-ID` | header | no | see `requestId` | Request identifier (idempotency key); priority: header > body.requestId > server-generated UUID |
| `X-Trace-ID` | header | no | `""` | Trace id (private convention, outside the contract) |
| `action` | body | yes | — | One of the 6 supported actions: `process.exec`, `process.run`, `file.read`, `file.write`, `file.list`, `file.search`. Contract actions outside this set — including the chunked `file.read_chunk`/`file.write_chunk` — return 400 |
| `args` | body | no | `{}` | Per-action arguments (below) |
| `requestId` | body | no | server-generated UUID | Body form of the request identifier |

**action `process.exec` / `process.run`** — command execution (the two names
are synonyms):

| args | Type | Required | Default | Description |
|---|---|---|---|---|
| `cmd` | string | one of `cmd`/`command` (`cmd` wins) | none | Shell command |
| `command` | string | no | none | `cmd` alias |
| `cwd` | string | no | none | Working directory (`working_dir` accepted as an alias) |
| `envs` | object\<string,string\> | no | none | Environment variables (`env` accepted as an alias) |
| `timeout` | number, >= 0 | no | unlimited | Timeout in seconds |

```json
{"action": "process.exec", "args": {"cmd": "ls -la", "cwd": "/ws", "timeout": 300},
 "requestId": "invoke-001"}
```

Response: `{"stdout": "...", "stderr": "...", "exit_code": 0}` — note
`exit_code` (the legacy `returncode` field was renamed). An RPC failure still
returns 200: `{"stdout": "", "stderr": "execute failed: ...", "exit_code": -1}`.

**action `file.read`** — whole-file read:

| args | Type | Required | Default | Description |
|---|---|---|---|---|
| `path` | string | yes | — | Absolute file path |
| `binary` | bool | no | `false` | `true`: `data` is **hex**-encoded; `false`: UTF-8 text |

Response: `{"data": "...", "error": null}`; action failure (e.g. missing
file): `{"data": null, "error": "read file failed: ..."}` (still HTTP 200).

**action `file.write`** — whole-file write (parent directories created
automatically):

| args | Type | Required | Default | Description |
|---|---|---|---|---|
| `path` | string | yes | — | Target path |
| `data` | string | yes | — | Content; **hex** when `binary=true` |
| `binary` | bool | no | `false` | Encoding selector, same as `file.read` |

Response: `{"name": "a.txt", "path": "/ws/a.txt", "type": "file", "size": 5,
"error": null}`.

**action `file.list`** — directory listing:

| args | Type | Required | Default | Description |
|---|---|---|---|---|
| `path` | string | yes | — | Directory path |
| `depth` | int | no | `1` (clamped to 1–64) | Recursion depth |

Response entries align with the execd structure — `type` is the category
(`file`/`dir`), `modified_time` is unix seconds, and `permissions` is always
`null` (the Python layer has no permission data; a known difference from the
execd implementation, which returns an octal permission string):

```json
{"entries": [{"name": "a.py", "path": "/ws/a.py", "type": "file",
              "size": 1024, "permissions": null,
              "modified_time": 1695441203.5}], "error": null}
```

**action `file.search`** — recursive filename glob search:

| args | Type | Required | Default | Description |
|---|---|---|---|---|
| `path` | string | yes | — | Search root |
| `pattern` | string | yes | — | Filename fnmatch glob (e.g. `*.log`) |
| `exclude_patterns` | array\<string\> | no | none | Exclusion globs (also filename fnmatch) |
| `depth` | int | no | `64` (clamped to 1–64) | Recursion depth cap, applied by the adapter over relative path depth |

Response: file entries in the `file.list` shape, only matching files.

### Lifecycle and reclamation

- `sandboxId` equals `instanceId` (the platform instance id, UUID form);
  functionsystem maintains the instance-to-physical-sandbox mapping
  internally, so callers never need to know the physical sandbox id.
- Named creates: when `name` is set, the instance is created and addressed
  under the runtime namespace (`{ns}-{name}`); the adapter pins the namespace
  so create-side registration and invoke-side addressing stay consistent.
- create is synchronous and blocking: 200 means ready, 500 means failure, no
  polling.
- Multiple instances: several sandboxes may be created at once, operated
  independently and isolated by id; deleting one does not affect the others.
- Delete is terminal: a second delete of the same id returns 404.
- Best-effort teardown: when the FaaS `pre_stop` triggers the Executor's
  `stop()`, it shuts down in the order "stop HTTP Server → stop user
  processes with grace → `terminate_all()` best-effort destroys every child
  sandbox"; the startup-failure path likewise tears down already-created
  sandboxes to avoid leaking remote containers.
- The sandbox API is independent of the `/v1/files/upload`,
  `/v1/files/download`, and `/v1/files/list` management-plane file endpoints:
  the latter are the file-transfer channel between the Frontend and the
  function-instance container; the sandbox handler does not reuse their
  `FileHandler`, and size limits are separate.
## User processes

Bootstrap commands are read from `YR_RUNTIME_BOOTSTRAP_CMD` with the same
best-effort behavior as the original Python Runtime implementation: at most 64
argv arrays are considered, malformed entries and individual start failures
are logged and skipped.

Process stdin is connected to `/dev/null`. Stdout and stderr are combined into
the append-only `${GLOG_log_dir}/${YR_RUNTIME_ID}.std` (one file per runtime,
shared by all commands); when `GLOG_log_dir` is unset it defaults to
`/home/snuser/log/`, and when the log cannot be opened both output streams fall
back to `/dev/null`. A missing or unsafe runtime ID is replaced with a
path-safe file name.

The FaaS `PRE_STOP_TIMEOUT` bounds shutdown. By default the Executor reserves
two seconds of that timeout for forced termination and final cleanup, and uses
the remaining time as the child-process SIGTERM grace period.
