# Agent DX development

## Boundaries

- `agent/` owns Agent APIs, sessions and execution orchestration. Target platform access goes through the public Sandbox SDK.
- `platform/` owns Instance execution, the Sandbox API/SDK and RRT.
- `gateway/` owns shared entrypoints, routing and node forwarding; it does not own Instance lifecycle.
- Sandbox SDK uses adx-sandbox / adx_sandbox / ADX_ naming. Owned Agent namespaces use adx, Gateway commands use adx-, and config/headers use ADX. External runtime dependencies require functional replacement, not fabricated import renames. `docs/migration/sources.json` records exact provenance.
- Keep the Go HTTP adapter small: local request/response types, protocol encoding, and injected backend interfaces. Preserve Sandbox and Agent entrypoints. Do not reintroduce the removed runtime SDK, function/Job packages or metadata watchers to satisfy a helper import. Agent business logic belongs under `agent/`.

## Builds and tests

- Only `frontend_proxy_service.proto` is a retained legacy gRPC contract. Keep its necessary message dependencies confined to Frontend compatibility. Design new internal RPCs around Instance responsibilities; do not reuse old POSIX/function services. RRT operations and Node Manager runtime cooperation use HTTP; shared payloads live in adx-core runtime types.

- Root Cargo workspace; Go module at `platform/control-plane/sandbox-api`; Python packages build independently.
- Use Makefile/native package commands. `build/` contains tracked scripts; outputs go to `out/` or explicitly configured external caches.
- Run focused checks for changed components; report runtime/cluster validation separately.
- Delegate long builds/tests/packaging to a narrow-context subagent when available: exact repo, commands, concurrency, success criteria and log path. The worker does not edit source. Keep complete logs, poll every 60–180 seconds and return compact results.

## Commits

Use conventional commit subjects and a single CLA-compatible Signed-off-by trailer. Do not mix migration with unrelated behavior changes.
