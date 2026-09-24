# EXECD

Build with the root Cargo workspace: `cargo build --locked -p adx-execd --bin adx-execd`.

`adx-execd` serves Environment operations over HTTP: `/healthz`, `/invoke`, command watches, uploads and downloads. Configure `EXECD_HTTP_PORT` (default 50090) and optional `EXECD_HTTP_TOKEN`. Reverse tunnels use HTTP/WebSocket and are enabled with `EXECD_TUNNEL_WS_PORT` and `EXECD_TUNNEL_HTTP_PORT`.

When `ADX_ENVIRONMENT_ID` is configured, the reverse-tunnel WebSocket handshake
must carry the same `X-Sandbox-ID`. A mismatched client is rejected before it
can replace the active connection. Restore refreshes this identity for the new
listener generation. Binary file downloads close the HTTP connection after the
body, matching their response header; chunked tar downloads stay reusable.

The public Ingress → Relay → EXECD operations are defined in the
[`data-plane.yaml`](../../api/openapi/data-plane.yaml) OpenAPI contract. Generic
port forwarding and reverse tunnels carry application-defined protocols and do
not have fixed ADX request schemas.

`process.wait` reports an expired wait deadline as HTTP 200 with `status=running`
and `error_code=WAIT_TIMEOUT`; it does not stop the command. `process.kill`
returns HTTP 200 with `killed=false` for a missing or already exited command.
An actual signal failure remains an error.

Adxlet supplies `ADX_ENVIRONMENT_ID`, `ADX_RUNTIME_ID` and `ADX_OWNERSHIP_GENERATION` through sandboxd. Runtime cooperation uses the same HTTP listener: identity-aware status, checkpoint preparation and confirmed-unstarted abort. See the [HTTP contract](../../api/http/runtime-control.md).

Checkpoint restore validates the new execution identity, refreshes child environment and HTTP credentials, retires inherited connections, and rearms HTTP/tunnel listeners before reporting Running. Missing or stale restored identity leaves the runtime unavailable. Backend handoff uses `/proc/gvisor/checkpoint` or `ADX_CHECKPOINT_HANDOFF_FILE`; the restored environment comes from `/proc/gvisor/spec_environ` or `ADX_ENV_FILE`.

EXECD no longer compiles protobuf or connects to POSIX/RuntimeRPC services. User operations and control status use HTTP. Checkpoint artifact persistence and Environment lifecycle remain Adxlet responsibilities.

Run `cargo test --locked -p adx-execd`. `control_http.rs` starts the real runtime and uses the Adxlet HTTP client with a FIFO handoff fixture. It covers protocol/recovery behavior; it does not execute a real sandboxd checkpoint.

With checkpoint storage configured, Adxlet enables the workload-local
`POST /checkpoint` Unix HTTP listener at `/run/adx/execd.sock`. Override its directory
with `execd_env.ADX_EXECD_CONTROL_SOCKET_PATH` (AKernel uses `/run/akernel`). This keeps
the runtime running and produces a lifecycle-bound local recovery point for
reload/failover. A success response requires backend handoff and Adxlet's
persistent result acknowledgement. See the [runtime control contract](../../api/http/runtime-control.md#workload-local-checkpoint).
