# Environment failure and data-path errors

This page describes the current Rust Gateway and Environment backend. The imported Go sandboxrouter, etcd failure read-through and retained FATAL/OOM JSON response implementation were removed during migration; they are not part of the current server contract.

## Current behavior

Coordinator publishes committed Running routes only for routable nodes. Pause, deletion and node invalidation withdraw routes. Ingress uses its in-memory Coordinator subscription; reconnect starts with a full snapshot. An established Ingress may keep cached routes while disconnected, but Relay still checks its local binding. Ingress restart has no disk route cache.

The Rust Ingress maps resolver/connection failures in `gateway/src/ingress/server.rs::error_response`:

| Condition | HTTP |
|---|---|
| Generic resolver reports unknown route (legacy/library mode) | 404 |
| Managed Ingress cache has no published route | 503 |
| Route synchronization not ready, resolver unavailable, draining or changed route | 503 |
| Resolver reports a non-running status or invalid connection metadata | 409 |
| Missing endpoint or other upstream connection error | 502 |
| Upstream connect timeout | 504 |
| Pool/admission pressure | 429 |
| Permission denial | 403 |

These error bodies are plain text. Authentication and individual endpoint validation have their own rejection paths. After response headers have been sent, a transport failure may close the stream instead of returning a new HTTP response.

Loss of a route does not by itself diagnose an OOM. The authenticated Environment
catalog includes `failure: {reason, exit_code, message}` for observed backend
termination. `reason: OomKilled` is sourced from sandboxd Wait before cleanup;
ordinary exit is `Exited`, absent backend is `Missing`. The SDK retains the
catalog reason in `get_info().failure` and can classify an exhausted direct call
as `SANDBOX_OOM` only after the Failed/OomKilled record is visible. An earlier
call or unavailable catalog retains the unknown-result error. User-command termination
retains `CommandResult.exit_signal`; the Sandbox may stay Running. EXECD does
not observe OOM, and a command signal alone is not an OOM classification.
See the [OOM contract](../../../platform/api/http/error-contract.md#oom-原因与-sdk).
There is no 410 OOM envelope or deleted-instance diagnostic retention.

## Recovery and SDK boundary

Coordinator heartbeats mark lost node executions invalid; the returning Adxlet reconciles and cleans them before admission. Valid shared checkpoint recovery is a separate coordinator path. Missing/local-only checkpoints cannot produce cross-node recovery. See [node failure](../../../docs/testing/node-failure-takeover.md).

The Python SDK retains its transport retry/error handling and surfaces response bodies. A 404 may trigger its compatibility invoke fallback, but the Rust API Server does not implement that legacy invocation transport: command and file operations need the Ingress → Relay → EXECD data path. Client retry behavior is not proof of automatic instance recovery.

Gateway tests (`gateway/tests/coordinator_routes.rs`, `mock_e2e.rs`), SDK transport tests and real basic [K8s acceptance](../../../docs/testing/2026-09-17-observability-k8s.md) cover different layers. The basic K8s run validates heartbeat failure and returning-node cleanup; it does not validate OOM classification or cross-host network partition isolation.
