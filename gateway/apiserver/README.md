# ADX API Server

Rust public management HTTP service, binary `adx-apiserver`, in the root Cargo workspace. It owns API Key validation/cache, HTTP validation and compatibility responses, Environment ownership caching, immutable-target retries, snapshot/key endpoints and Agent route forwarding. Lifecycle state and scheduling stay in Adxlet and Coordinator.

Requests are converted directly to the generated Environment RPC types. There is no Go adapter, function payload, generic Signal dispatch or legacy protobuf dependency. Public Sandbox paths and base64 JSON response envelopes remain compatible with the Sandbox SDK. Existing `functionProxyId` in a resume response is a public compatibility field containing the node ID.

The API Server also owns the legacy-compatible scheduler HTTP paths while
Coordinator owns their state and decisions. `/global-scheduler/resources` reads the
watched in-memory node directory and renders the legacy `resource.fragment`
shape; `/api/sandbox/v1/resources` keeps the ADX `items` shape. Administrator-only
`/global-scheduler/scheduling_queue` reads the bounded, memory-only central
wait queue. `POST /global-scheduler/node/localschedulingstatus?node_id=...`
pauses new allocation to a node and `DELETE` removes that pause. Existing
Environments keep running. The pause is persisted by Coordinator and is not cleared by
Adxlet heartbeats; local pressure or reconciliation can still keep a
resumed node closed.
API Server performs the administrator check before either internal RPC. Coordinator
does not repeat user or component authorization so trusted internal controllers
can use the same operations directly.
Both resource shapes include CPU millicores, Memory/Disk MiB and whole-card
`GPU/<model>` or `NPU/<model>` capacity and allocatable counts.

## Modules

| Module | Responsibility |
|---|---|
| `contract.rs` | Typed public JSON validation, resources, environment and affinity conversion |
| `http.rs` | Routes, JSON/SSE responses, create replay handling, Agent streaming forwarding |
| `clients.rs` | mTLS RPC clients, Redis discovery, API Key validation and ownership reads |
| `ownership.rs` | Bounded LRU caches with fixed expiration |
| `operations.rs` | Pause/resume/delete/snapshot, pinned assignment/revision, durable-result validation |
| `ingress.rs` | Default in-process Ingress lifecycle; same service as the standalone binary |
| `config.rs`, `main.rs` | Configuration, HTTPS or literal-loopback HTTP, graceful shutdown |

```bash
cargo test --locked -p adx-apiserver -j 2
cargo build --locked -p adx-apiserver -j 2
./target/debug/adx-apiserver --config /path/to/api.json
```

The unified deployment role is `apiserver`. It embeds Ingress by default while preserving separate API Server and Ingress TLS identities and listeners. Set `ingress_mode: standalone` to run `adx-ingress` as a second process. Both modes use `gateway::ingress::IngressService`. Certificate identities trusted by Coordinator and Adxlet use `apiserver`. Certificates are supplied by deployment and loaded on process start. A TLS identity is still required for internal RPC when the public HTTP listener uses `loopback_http`.

The API always uses the verified tenant, not tenant fields supplied in a request. Existing-instance operations use the local ownership cache; misses query Coordinator. A retry preserves the original assignment and lifecycle revision, and rejects a changed ownership generation. Only Published results are acknowledged as successful; SQLite Journaled results remain unavailable until publication.

Agent endpoints forward to `agent_address` when configured and preserve streaming responses. Agent resource authorization and business behavior belong to the Agent service. No Agent runtime is embedded here.

See [HTTP contract](docs/sandbox-lifecycle-api.md), [failure behavior](docs/sandbox-runtime-failure.md), [deployment](../../docs/testing/process-deployment.md), and [Rust migration status](../../docs/testing/rust-apiserver.md).
