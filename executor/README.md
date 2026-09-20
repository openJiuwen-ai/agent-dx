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
`POST /v1/sandbox/sandboxes`, receives an `instance_id`, and puts it into the
URL path of subsequent requests; multiple sandboxes are isolated by id, each
with an independent lifecycle.

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

- **Resource-style URLs**: paths follow
  `/v1/sandbox/sandboxes[/{id}[/{action}]]`, with methods chosen by HTTP
  semantics — pure reads GET, create/action POST, overwrite PUT, delete
  DELETE.
- **Request format**: `create`/`execute` request bodies are JSON and must
  carry `Content-Type: application/json` (otherwise 400); the body may use
  either `Content-Length` or chunked transfer encoding. The `files/write`
  body is the raw file content (not JSON) and must carry `Content-Length`
  (missing → 400).
- **Size limits**: sandbox API requests (JSON bodies and the raw
  `files/write` body) and responses each have an independent default limit of
  512 MiB; exceeding either returns 413. The limits are independent of the
  Frontend file API's `max_file_size`. Binary content travels Base64-encoded,
  so its effective size is about three quarters of the limit.
- **Concurrency**: the HTTP Server processes at most 64 concurrent requests
  globally (across all endpoints); beyond the limit it returns 503 and closes
  the connection:

  ```json
  {"message":"executor is busy"}
  ```

- **trace_id**: every endpoint accepts an optional `trace_id` (in the JSON
  body for `create`/`execute`, in the URL query for the others), propagated
  through the whole invoke chain for log correlation. 1-128 characters,
  restricted to `A-Z a-z 0-9 _ - . :` (covers UUIDs, hex, dotted names); when
  omitted the backend generates one. Note that an invalid format yields 500,
  not 400 (validation happens in the underlying call).
- **Status codes**:

  | Code | Trigger |
  |---|---|
  | 200 | Success (including business-level results such as a command exiting non-zero; see each endpoint) |
  | 400 | Request errors: missing parameters, invalid types/values, wrong `Content-Type`, etc. |
  | 403 | Request source is not a loopback address |
  | 404 | URL shape unregistered, or the `{id}` in the path has no matching sandbox (`DELETE` returns an idempotent 200 for a nonexistent `{id}`) |
  | 405 | Path exists but the HTTP method is unregistered; the response carries an `Allow` header (e.g. GET on `.../{id}/execute` returns `Allow: POST`); a nonexistent `{id}` takes precedence and returns 404 |
  | 413 | Request or response body exceeds the 512 MiB limit |
  | 500 | Underlying RPC/IO failure; `message` is the underlying cause prefixed with the operation (e.g. `execute failed: rpc timeout`) |
  | 503 | Concurrency exceeds 64 |

All error response bodies are JSON of the form `{"message": "..."}`.

### 1. Create sandbox: `POST /v1/sandbox/sandboxes`

```http
POST /v1/sandbox/sandboxes HTTP/1.1
Content-Type: application/json

{
  "image": "python:3.12-slim",
  "cpu": 1000,
  "memory": 2048,
  "sandbox_type": "supervisor",
  "ports": ["tcp:8080", "9090"],
  "upstream": "127.0.0.1:8000",
  "working_dir": "/sandbox",
  "env": {"PATH": "/usr/bin"},
  "idle_timeout": 600
}
```

| Parameter      | Type                     | Required | Default                  | Description                                                                                          |
|----------------|--------------------------|----------|--------------------------|------------------------------------------------------------------------------------------------------|
| `image`        | string                   | no       | none                     | Docker image name; effective only when `sandbox_type="docker"`                                        |
| `cpu`          | int                      | no       | none                     | CPU resource, millicores                                                                             |
| `memory`       | int                      | no       | none                     | Memory, MB                                                                                           |
| `sandbox_type` | string                   | no       | `""`                     | Sandbox executor type: `""`/`"supervisor"`/`"docker"`                                                |
| `ports`        | array\<string\>          | no       | none                     | Port forwarding list; each element is `"proto:port"` (e.g. `"tcp:8080"`) or `"port"`                  |
| `upstream`     | string                   | no       | none                     | Reverse tunnel upstream, a local service address `host:port`, e.g. `"127.0.0.1:8000"`                |
| `working_dir`  | string                   | no       | temp dir inside sandbox  | Working directory inside the sandbox                                                                  |
| `env`          | object\<string,string\>  | no       | inherit sandbox runtime  | Environment variables; when specified they apply wholesale (not merged with the default environment)  |
| `idle_timeout` | int                      | no       | `300`                    | Idle timeout in seconds; must be greater than 0                                                      |
| `user`         | string                   | no       | platform default `agentos` | Container run-as user, passed through as Docker's `Config.User`; accepts any format Docker accepts, e.g. `"1000"`, `"1000:1000"`, `"root"`; effective only when `sandbox_type="docker"` |
| `trace_id`     | string                   | no       | `""`                     | Trace ID, 1-128 characters restricted to `A-Z a-z 0-9 _ - . :`; generated by the backend when omitted |

**Success (200)** — create is synchronous and blocking: a 200 means the child
sandbox is ready (creation includes a readiness verification), no polling
required:

```json
{
  "instance_id": "34420000-0000-4000-886a-980025037460"
}
```

`instance_id` is the platform instance ID (UUID form); all subsequent
operations put it into the `{id}` segment of the URL path.

**Failure responses**:

- 400 — invalid parameter type/value, e.g. `ports` not an array of strings,
  `env` with non-string keys/values, `cpu`/`memory`/`idle_timeout` not
  integers, `idle_timeout <= 0`, `trace_id` not a string:

  ```json
  {"message": "idle_timeout must be greater than zero"}
  ```

- 500 — creation failed; `message` is `create sandbox failed: ` plus the
  underlying cause. Note that `ports` element format (`proto:port`) is only
  validated at creation time, so an invalid element (e.g. `"tcp:8080:9"`,
  `"tcp:abc"`) also yields 500, not 400:

  ```json
  {"message": "create sandbox failed: rpc timeout"}
  ```

### 2. Delete sandbox: `DELETE /v1/sandbox/sandboxes/{id}`

```http
DELETE /v1/sandbox/sandboxes/34420000-0000-4000-886a-980025037460 HTTP/1.1
```

| Parameter      | Type   | Required | Default | Description                                                |
|----------------|--------|----------|---------|-------------------------------------------------------------|
| `{id}` (path)  | string | yes      | —       | Sandbox instance ID, the `instance_id` returned by create   |

No request body.

**Success (200)**:

```json
{
  "success": true
}
```

**Idempotent delete (200)** — when `{id}` does not exist no underlying
terminate request is sent and the response is still 200; repeated deletes are
safe:

```json
{
  "success": false,
  "message": "sandbox not found"
}
```

**Failure (500)** — returned only when the underlying terminate RPC raises
(e.g. the platform-side instance was already reclaimed); `message` is the
underlying original cause:

```json
{
  "message": "instance not found"
}
```

### 3. Execute command: `POST /v1/sandbox/sandboxes/{id}/execute`

```http
POST /v1/sandbox/sandboxes/34420000-0000-4000-886a-980025037460/execute HTTP/1.1
Content-Type: application/json

{
  "command": ["ls", "-la"],
  "working_dir": "/tmp",
  "env": {"PATH": "/usr/bin"},
  "timeout": 30
}
```

| Parameter      | Type                      | Required | Default                | Description                                                                    |
|----------------|---------------------------|----------|------------------------|---------------------------------------------------------------------------------|
| `{id}` (path)  | string                    | yes      | —                      | Sandbox instance ID, the `instance_id` returned by create                        |
| `command`      | string \| array\<string\> | yes      | —                      | Command. A string runs via `/bin/sh -c` inside the sandbox; an array executes directly without a shell |
| `working_dir`  | string                    | no       | sandbox working dir    | Working directory for this command                                               |
| `env`          | object\<string,string\>   | no       | inherit sandbox env    | Command environment variables; when specified they **replace** the sandbox default environment wholesale (not merged) |
| `timeout`      | number                    | no       | no limit               | Timeout in seconds; must be greater than 0                                       |
| `trace_id`     | string                    | no       | generated by backend   | Trace ID, 1-128 characters restricted to `A-Z a-z 0-9 _ - . :`                    |

**Success (200)**:

```json
{
  "returncode": 0,
  "stdout": "total 0\ndrwxr-xr-x  2 root root  40 Aug 31 12:00 .\n",
  "stderr": ""
}
```

**A failed command is not an HTTP error** — non-zero exit, timeout, and
invalid command are all reported in a 200 response via `returncode`:

Command timeout (200, the command process is killed):

```json
{
  "returncode": -1,
  "stdout": "",
  "stderr": "Command timed out after 30 seconds"
}
```

Invalid command (200, e.g. an empty array or an array with non-string
elements; the command does not run):

```json
{
  "returncode": -1,
  "stdout": "",
  "stderr": "Error: cmd list cannot be empty"
}
```

**Failure responses**:

- 400 — missing `command`, or invalid type for
  `working_dir`/`env`/`timeout`/`trace_id`, or `timeout <= 0`:

  ```json
  {"message": "command is required"}
  ```

- 404 — no sandbox for `{id}`:

  ```json
  {"message": "sandbox 34420000-0000-4000-886a-980025037460 not found"}
  ```

- 500 — underlying RPC failure:

  ```json
  {"message": "execute failed: rpc timeout"}
  ```

### 4. Read file: `GET /v1/sandbox/sandboxes/{id}/files/read`

```http
GET /v1/sandbox/sandboxes/34420000-0000-4000-886a-980025037460/files/read?path=/sandbox/data.txt&mode=r HTTP/1.1
```

| Parameter      | Type  | Required | Default              | Description                                                       |
|----------------|-------|----------|----------------------|--------------------------------------------------------------------|
| `{id}` (path)  | string | yes     | —                    | Sandbox instance ID, the `instance_id` returned by create           |
| `path`         | query | yes      | —                    | Absolute path of the file inside the sandbox                       |
| `mode`         | query | no       | `rb`                 | Open mode: `rb` (binary) / `r` (text)                              |
| `trace_id`     | query | no       | generated by backend | Trace ID, 1-128 characters restricted to `A-Z a-z 0-9 _ - . :`      |

**Success (200), text mode** — with `mode=r`, `content` is the file text and
`content_encoding` is `text`:

```json
{
  "path": "/sandbox/data.txt",
  "mode": "r",
  "content": "hello sandbox",
  "content_encoding": "text"
}
```

**Success (200), binary mode** — with `mode=rb` (default), `content` is the
Base64-encoded file content and `content_encoding` is `base64`:

```json
{
  "path": "/sandbox/payload.bin",
  "mode": "rb",
  "content": "AAECAw==",
  "content_encoding": "base64"
}
```

Reading goes through RPC to native Python `open` inside the sandbox, so it
does not depend on shell tools in the sandbox and works with minimal images
(no tar/cat/sh).

**Failure responses**:

- 400 — missing `path`:

  ```json
  {"message": "path is required"}
  ```

- 404 — no sandbox for `{id}`:

  ```json
  {"message": "sandbox 34420000-0000-4000-886a-980025037460 not found"}
  ```

- 500 — file missing or unreadable inside the sandbox:

  ```json
  {"message": "read file failed: [Errno 2] No such file or directory: '/sandbox/data.txt'"}
  ```

### 5. Write file: `PUT /v1/sandbox/sandboxes/{id}/files/write`

`path`/`mode` go in the URL query; the file content is the request body (raw
content, not JSON).

Text write (`mode=w`, body is the raw text, must be UTF-8):

```http
PUT /v1/sandbox/sandboxes/34420000-0000-4000-886a-980025037460/files/write?path=/sandbox/output.txt&mode=w HTTP/1.1
Content-Type: text/plain

hello sandbox
```

Binary write (`mode=wb`, body is a Base64-encoded string):

```http
PUT /v1/sandbox/sandboxes/34420000-0000-4000-886a-980025037460/files/write?path=/sandbox/payload.bin&mode=wb HTTP/1.1
Content-Type: text/plain

AAECAw==
```

| Parameter      | Type    | Required | Default              | Description                                                                    |
|----------------|---------|----------|----------------------|---------------------------------------------------------------------------------|
| `{id}` (path)  | string  | yes      | —                    | Sandbox instance ID, the `instance_id` returned by create                        |
| `path`         | query   | yes      | —                    | Absolute path of the file inside the sandbox; parent directories are created     |
| `mode`         | query   | no       | `wb`                 | `wb` (write binary) / `w` (write text) / `a` (append text) / `ab` (append binary) |
| body           | string  | yes      | —                    | File content. When `mode` contains `b` the body must be a Base64 string, otherwise UTF-8 text |
| `trace_id`     | query   | no       | generated by backend | Trace ID, 1-128 characters restricted to `A-Z a-z 0-9 _ - . :`                   |

The body encoding is decided by `mode`; the caller declares nothing extra.
Note: an empty body is rejected (400), so an empty file cannot be written
through this endpoint.

**Success (200)**:

```json
{
  "success": true,
  "path": "/sandbox/output.txt"
}
```

**Failure responses**:

- 400 — missing `path`, empty body, missing `Content-Length`, invalid Base64
  body in a binary mode, or non-UTF-8 body in a text mode:

  ```json
  {"message": "invalid base64 content: Incorrect padding"}
  ```

- 404 — no sandbox for `{id}`:

  ```json
  {"message": "sandbox 34420000-0000-4000-886a-980025037460 not found"}
  ```

- 500 — write failed inside the sandbox (e.g. permission denied):

  ```json
  {"message": "write file failed: [Errno 13] Permission denied: '/sandbox/output.txt'"}
  ```

### 6. List directory: `GET /v1/sandbox/sandboxes/{id}/files/list`

```http
GET /v1/sandbox/sandboxes/34420000-0000-4000-886a-980025037460/files/list?path=/sandbox&recursive=true&max_depth=2&include_files=true&include_dirs=true HTTP/1.1
```

| Parameter       | Type           | Required | Default              | Description                                                                    |
|-----------------|----------------|----------|----------------------|---------------------------------------------------------------------------------|
| `{id}` (path)   | string         | yes      | —                    | Sandbox instance ID, the `instance_id` returned by create                        |
| `path`          | query          | yes      | —                    | Absolute path of the directory inside the sandbox                                |
| `recursive`     | query (bool)   | no       | `false`              | Whether to list subdirectories recursively; accepts only `true`/`false` (case-insensitive) |
| `include_files` | query (bool)   | no       | `true`               | Whether results include files                                                   |
| `include_dirs`  | query (bool)   | no       | `true`               | Whether results include directories                                             |
| `max_depth`     | query (int)    | no       | `20`                 | Maximum recursion depth, `>=0`; effective only with `recursive=true`, passing `0` is the same as omitting it |
| `trace_id`      | query          | no       | generated by backend | Trace ID, 1-128 characters restricted to `A-Z a-z 0-9 _ - . :`                   |

**Success (200)**:

```json
{
  "items": [
    {
      "name": "data.txt",
      "path": "/sandbox/data.txt",
      "size": 12,
      "is_directory": false,
      "modified_time": "2026-08-31T12:00:00.123456",
      "type": ".txt"
    },
    {
      "name": "logs",
      "path": "/sandbox/logs",
      "size": 0,
      "is_directory": true,
      "modified_time": "2026-08-31T11:30:00.456789",
      "type": null
    }
  ]
}
```

Fields of each entry in `items`:

- `name`: file/directory name (no path).
- `path`: absolute path inside the sandbox.
- `size`: size in bytes; always `0` for directories.
- `is_directory`: whether the entry is a directory.
- `modified_time`: last modification time — sandbox-local, microsecond
  precision, no timezone (format `YYYY-MM-DDTHH:MM:SS.ffffff`).
- `type`: file extension including the dot (e.g. `.txt`); `null` for
  directories and extensionless files.

At most 10000 entries are returned, silently truncated at the limit;
unreadable subdirectories are skipped.

**Failure responses**:

- 400 — missing `path`, `max_depth` not an integer or negative, or a boolean
  parameter that is not `true`/`false`:

  ```json
  {"message": "recursive must be a boolean ('true' or 'false')"}
  ```

- 404 — no sandbox for `{id}`:

  ```json
  {"message": "sandbox 34420000-0000-4000-886a-980025037460 not found"}
  ```

- 500 — directory does not exist inside the sandbox:

  ```json
  {"message": "list files failed: path not found: /sandbox"}
  ```

### 7. Search files: `GET /v1/sandbox/sandboxes/{id}/files/search`

```http
GET /v1/sandbox/sandboxes/34420000-0000-4000-886a-980025037460/files/search?path=/sandbox&pattern=*.txt&exclude_patterns=ignore*,*.bak HTTP/1.1
```

| Parameter          | Type            | Required | Default              | Description                                                                     |
|--------------------|-----------------|----------|----------------------|----------------------------------------------------------------------------------|
| `{id}` (path)      | string          | yes      | —                    | Sandbox instance ID, the `instance_id` returned by create                         |
| `path`             | query           | yes      | —                    | Absolute path of the search root inside the sandbox                               |
| `pattern`          | query           | yes      | —                    | Glob pattern (e.g. `*.txt`) matched against **file names**; only files are returned, never directories |
| `exclude_patterns` | query (array)   | no       | none                 | Exclusion patterns, comma-separated (e.g. `ignore*,*.bak`), also matched against file names |
| `trace_id`         | query           | no       | generated by backend | Trace ID, 1-128 characters restricted to `A-Z a-z 0-9 _ - . :`                    |

The search runs recursively under `path`; matching files are returned as
entries.

**Success (200)**:

```json
{
  "items": [
    {
      "name": "data.txt",
      "path": "/sandbox/data.txt",
      "size": 12,
      "is_directory": false,
      "modified_time": "2026-08-31T12:00:00.123456",
      "type": ".txt"
    }
  ]
}
```

Fields of each entry in `items`:

- `name`: file name (no path).
- `path`: absolute path inside the sandbox.
- `size`: size in bytes.
- `is_directory`: whether the entry is a directory (always `false` for this
  endpoint).
- `modified_time`: last modification time — sandbox-local, microsecond
  precision, no timezone (format `YYYY-MM-DDTHH:MM:SS.ffffff`).
- `type`: file extension including the dot (e.g. `.txt`); `null` for
  extensionless files.

**No match (200)** — no file matched, or the search root does not exist (not
an error); both return an empty list. Unreadable subdirectories are skipped:

```json
{
  "items": []
}
```

**Failure responses**:

- 400 — missing `path` or `pattern`:

  ```json
  {"message": "pattern is required"}
  ```

- 404 — no sandbox for `{id}`:

  ```json
  {"message": "sandbox 34420000-0000-4000-886a-980025037460 not found"}
  ```

- 500 — underlying RPC failure:

  ```json
  {"message": "search files failed: rpc timeout"}
  ```

### Lifecycle and reclamation

- `instance_id` is the platform instance ID (UUID form); functionsystem
  maintains the instance-to-physical-sandbox mapping internally, so callers
  never need to know the physical sandbox ID.
- create is synchronous and blocking: 200 means ready, 500 means failure, no
  polling.
- Multiple instances: several sandboxes may be created at once, operated
  independently and isolated by id; deleting one does not affect the others.
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
