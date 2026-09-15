# RRT

Build with the root Cargo workspace: `cargo build --locked -p rrt-daemon --bin rrt-runtime`.

`rrt-runtime` serves Instance operations over HTTP: `/healthz`, `/invoke`, command watches, uploads and downloads. Configure `RRT_HTTP_PORT` (default 50090) and optional `RRT_HTTP_TOKEN`. Reverse tunnels use HTTP/WebSocket and are enabled with `RRT_TUNNEL_WS_PORT` and `RRT_TUNNEL_HTTP_PORT`.

Node Manager supplies `ADX_INSTANCE_ID`, `ADX_RUNTIME_ID` and `ADX_OWNERSHIP_GENERATION` through sandboxd. Runtime cooperation uses the same HTTP listener: identity-aware status, checkpoint preparation and confirmed-unstarted abort. See the [HTTP contract](../../api/http/runtime-control.md).

Checkpoint restore validates the new execution identity, refreshes child environment and HTTP credentials, retires inherited connections, and rearms HTTP/tunnel listeners before reporting Running. Missing or stale restored identity leaves the runtime unavailable. Backend handoff uses `/proc/gvisor/checkpoint` or `ADX_CHECKPOINT_HANDOFF_FILE`; the restored environment comes from `/proc/gvisor/spec_environ` or `ADX_ENV_FILE`.

RRT no longer compiles protobuf or connects to POSIX/RuntimeRPC services. User operations and control status use HTTP. Checkpoint artifact persistence and Instance lifecycle remain Node Manager responsibilities.

Run `cargo test --locked -p rrt-daemon`. `control_http.rs` starts the real runtime and uses the Node Manager HTTP client with a FIFO handoff fixture. It covers protocol/recovery behavior; it does not execute a real sandboxd checkpoint.
