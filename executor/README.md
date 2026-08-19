[中文](README.zh.md) | **English**

# agent-dx-executor

Platform-owned FaaS code package for custom-image Agent instances. It starts
the executor HTTP server and user processes from a standard YuanRong FaaS
initializer. The wheel is installed into the Agent base image and is not a
user-facing SDK.

It has no dependency on the agent-dx SDK or the `yr` package. The image still
must contain a compatible openYuanRong Python Runtime because that runtime
loads these entries as an ordinary FaaS code package.

Install the Executor into the Python environment used by the YuanRong Runtime.
It is image infrastructure and does not use the function's
`DELEGATE_DOWNLOAD`; that remains available for an optional user process code
package. Build the image with, for example:

```text
python -m pip install --no-deps agent_dx_executor-<version>-py3-none-any.whl
```

The package supports Python 3.9 and later without a minor-version-specific
dependency. Current primary runtime targets are Python 3.9 and Python 3.11.

FaaS entries:

```text
initializer: yr.agentexecutor.handler.initialize
handler:     yr.agentexecutor.handler.handle
pre-stop:    yr.agentexecutor.handler.pre_stop
```

At startup, the Executor initializes one process-local `SandboxInstance` in
the current function-instance container. Agent processes launched by the
Executor can access it through these loopback-only HTTP endpoints:

```text
POST /v1/sandbox/execute
POST /v1/sandbox/read_file
POST /v1/sandbox/write_file
POST /v1/sandbox/list_files
POST /v1/sandbox/search_files
```

Requests and responses use `application/json`. `execute` accepts `command`
and optional `working_dir`, `env`, and `timeout` fields; the aliases `cwd`,
`environment`, and `timeout_seconds` are also accepted. File content is sent
in `content`; binary content uses `content_encoding: "base64"` and text uses
`"text"`. List and search results are returned in an `items` field. This API
does not create a remote `@yr.instance` actor and does not depend on `yr`.

`execute` returns the command's `returncode`, `stdout`, and `stderr`. The
default timeout is 300 seconds when the request omits `timeout`. Commands run
in a new process group; on timeout the complete group is terminated and the
result has `returncode: -1`. The HTTP server processes at most 64 concurrent
requests and returns 503 when that limit is reached.

Sandbox JSON requests and responses each have an independent default limit of
512 MiB and do not use the Frontend file API's `max_file_size`. Binary content
is Base64-encoded inside JSON, so its effective request limit is approximately
three quarters of the JSON limit, minus the other JSON fields.

The Sandbox API is independent of the existing `/v1/files/upload`,
`/v1/files/download`, and `/v1/files/list` management-plane transfer API used
by the Frontend. Sandbox handlers do not reuse its `FileHandler`.

Bootstrap commands are read from `YR_RUNTIME_BOOTSTRAP_CMD` with the same
best-effort behavior as the original Python Runtime implementation: at most 64
argv arrays are considered, malformed entries and individual start failures
are logged and skipped.

Process stdin is connected to `/dev/null`. Stdout and stderr are combined into
the append-only `${GLOG_log_dir}/bootstrap_cmd_<index>.log`; when
`GLOG_log_dir` is unset it defaults to `/home/snuser/log/`, and when the log
cannot be opened both output streams fall back to `/dev/null`.

The FaaS `PRE_STOP_TIMEOUT` bounds shutdown. By default the Executor reserves
two seconds of that timeout for forced termination and final cleanup, and uses
the remaining time as the child-process SIGTERM grace period.
