# Sandbox HTTP adapter and Agent compatibility routes

This module preserves the Sandbox HTTP paths and payloads and the nine `/api/agent` entrypoints. It is a library for an embedding service, not a standalone server.

## Retained code

- `internal/sandbox`: lifecycle/snapshot handlers, request validation, replay handling, existing wire encoding and behavioral tests.
- `internal/httpx`: response envelope, trace/header helpers and logging through the host's global zap logger.
- `internal/instancecache`: instance summaries used during ownership checks, duplicate-create checks and pause/delete handling. The host feeds accepted state updates through `ObserveInstance` and `ForgetInstance`.
- `backend`: explicit execution, authoritative instance-state, authentication and snapshot-directory dependencies. Each registered router carries its dependencies in the request context.
- `internal/gen`: generated protobuf code; generate from the repository root with `bash build/codegen/go.sh`.

The imported runtime SDK, function/Job helpers, scheduler proxy, IAM implementation, etcd watchers, Kubernetes adapters and Go data-path proxy are removed. The platform data path remains in the top-level Rust `gateway`.

## Host integration

`RegisterRoutes(router, dependencies)` validates all Sandbox dependencies before mounting any routes. The host supplies execution transport, instance-state reads, API Key verification, Master address resolution and a configured snapshot HTTP client. There is no default connection to an old runtime or metadata store. Missing dependencies fail registration.

API Key verification returns a tenant/admin identity. Incoming tenant headers are replaced with the verified tenant. Lifecycle and invocation handlers enforce instance ownership; the explicit admin role permits administrative operations. The host's verifier owns expiry and revocation policy.

Execution currently accepts the imported protobuf payloads (`Create`, `Invoke`, and lifecycle signal payloads). This is an interim HTTP compatibility adapter, **not the new Master/Node Manager protocol implementation**. Its small local encoding structures are not a runtime SDK. `controlbackend` now translates the supported create/delete path to Instance RPC, caches ownership and API key validation, and `cmd/adx-sandbox-api` supplies a configurable HTTPS entrypoint. Unsupported operations return explicit errors. See [service wiring and validation](../../../docs/testing/frontend-control.md).

## Agent entrypoints

`RegisterAgentRoutes(router, verify, agentHandler)` mounts these existing routes:

| Method | Path |
|---|---|
| POST | `/api/agent` |
| POST | `/api/agent/:instanceId/invoke` |
| DELETE | `/api/agent/:instanceId` |
| GET | `/api/agent` |
| GET | `/api/agent/:instanceId` |
| POST | `/api/agent/:instanceId/files/upload` |
| GET | `/api/agent/:instanceId/files/download` |
| GET | `/api/agent/:instanceId/files/list` |
| POST | `/api/agent/:instanceId/files/mkdir` |

The Agent layer supplies `agentHandler` (or a reverse proxy to that service). It receives the original method, path, query, body and the verified identity via `backend.IdentityFromContext`. It owns Agent resource authorization, business logic and response formatting. HTTP status, headers, binary bodies and streaming responses are passed through unchanged. A missing verifier or handler fails registration.

These route adapters preserve the HTTP entrypoints; the old Go Agent implementation's function registration, code deployment and runtime SDK execution chain has not been copied into this module. Agent CLI, SDK and Executor source remains under `agent/`; wiring them to the new backend is separate work.
