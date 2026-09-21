# ADX API Server

Rust public management HTTP service, binary `adx-api-server`, in the root Cargo workspace. It owns API Key validation/cache, HTTP validation and compatibility responses, Capsule ownership caching, immutable-target retries, snapshot/key endpoints and Agent route forwarding. Lifecycle state and scheduling stay in Node Manager and Master.

Requests are converted directly to the generated Capsule RPC types. There is no Go adapter, function payload, generic Signal dispatch or legacy protobuf dependency. Public Sandbox paths and base64 JSON response envelopes remain compatible with the Sandbox SDK. Existing `functionProxyId` in a resume response is a public compatibility field containing the node ID.

## Modules

| Module | Responsibility |
|---|---|
| `contract.rs` | Typed public JSON validation, resources, environment and affinity conversion |
| `http.rs` | Routes, JSON/SSE responses, create replay handling, Agent streaming forwarding |
| `clients.rs` | mTLS RPC clients, Redis discovery, API Key validation and ownership reads |
| `ownership.rs` | Bounded LRU caches with fixed expiration |
| `operations.rs` | Pause/resume/delete/snapshot, pinned assignment/revision, durable-result validation |
| `edge.rs` | Default in-process Edge lifecycle; same service as the standalone binary |
| `config.rs`, `main.rs` | Configuration, HTTPS or literal-loopback HTTP, graceful shutdown |

```bash
cargo test --locked -p adx-api-server -j 2
cargo build --locked -p adx-api-server -j 2
./target/debug/adx-api-server --config /path/to/api.json
```

The unified deployment role is `api-server`. It embeds Edge by default while preserving separate API Server and Edge TLS identities and listeners. Set `edge_mode: standalone` to run `adx-edge-frontend` as a second process. Both modes use `gateway::edge::EdgeFrontendService`. Certificate identities trusted by Master and Node Manager use `api-server`. Certificates are supplied by deployment and loaded on process start. A TLS identity is still required for internal RPC when the public HTTP listener uses `loopback_http`.

The API always uses the verified tenant, not tenant fields supplied in a request. Existing-instance operations use the local ownership cache; misses query Master. A retry preserves the original assignment and lifecycle revision, and rejects a changed ownership generation. Only Published results are acknowledged as successful; SQLite Journaled results remain unavailable until publication.

Agent endpoints forward to `agent_address` when configured and preserve streaming responses. Agent resource authorization and business behavior belong to the Agent service. No Agent runtime is embedded here.

See [HTTP contract](docs/sandbox-lifecycle-api.md), [failure behavior](docs/sandbox-runtime-failure.md), [deployment](../../docs/testing/process-deployment.md), and [Rust migration status](../../docs/testing/rust-api-server.md).
