# Local public SDK acceptance

> Historical local/driver integration record. Counts and unavailable-CI statements below refer to those batches. The current driver has seven groups; hosted K8s has since passed [Buildkite #21](2026-09-17-observability-k8s.md). Use [current instructions](../../build/e2e/README.md) for reproduction.

The local Linux ARM64 environment runs two isolated Docker nodes. Each node runs
real sandboxd (PR #56, `efc201531d7e2e9d69505da151eb66084b61eebf`), Node Manager
and Node Proxy. Node 1 also hosts Redis 7.2.5, Master with an embedded Shard,
Sandbox API and Edge. ADX processes are launched from the unified release package
by `adxctl`; sandboxd is independently hosted. The SDK is installed from a wheel.

Control requests enter Edge over verified TLS, then use the co-located API Server
loopback HTTP listener. API Server/Master/Node RPC and Edge/Node Proxy use mTLS.
Commands and files traverse Edge → Node Proxy → the real RRT inside a runc
instance. The test registry is local and separate from the platform data path.

Resource observations read each Linux container's cgroup CPU/memory limits and
filesystem free capacity, then reserve capacity for infrastructure. This test
observation producer is not the production sandboxd collector.

## Reproduction and evidence

Use [the installed SDK acceptance entrypoint](../../build/e2e/README.md).
Local setup, complete build/runtime logs and artifacts are in
`out/ci/local-e2e/`; `source-manifest.json` and `package/manifest.json` identify the
actual dirty-worktree build. The source baseline is
`1e49d86f2123173a8f5358182ca294fab9a9b1e4`; this is not an immutable release or
Buildkite result.

## Initial two-node acceptance results

| Check | Result | Evidence under `out/ci/local-e2e/` |
|---|---|---|
| Installed SDK, two real instances | passed: create, query, stdout/stderr/exit 7, 130,000-byte binary file, delete | `sdk-query.log`, `sdk-query/sdk-result.json` |
| Actual placement and persistent deletion | node1 + node2; both Deleted and resources released | `sdk-query/catalog-after-delete.json` |
| Physical runtime cleanup | both sandboxd lists empty after SDK deletion | `sandboxd-after-sdk-node{1,2}.txt` |
| Stop with live instances | two new running instances removed; both supervisors stopped; independent sandboxd empty | `stop-test.log`, `sandboxd-before-stop-node{1,2}.txt`, `sandboxd-after-stop-node{1,2}.txt` |
| Node Manager regression | 44 passed, 0 failed, 0 ignored | `sandboxd-adapter.log` |
| CLI regression | 7 passed | `redis-auth-cli.log` |
| Go regression and vet | 208 passed | `go-query-tests.jsonl`, `frontend-query.log` |
| Linux ARM64 release package integrity | passed | `package/manifest.json` |

The create durations recorded by this functional run are not a performance
benchmark. Earlier failed attempts remain in separate evidence directories.

## Backend-generated identity

The adapter leaves `StartRequest.sandbox_id` empty. It associates the returned
`StartResponse.id` with the platform execution ID. List, Stats and Delete use the
returned backend ID; RRT control and routing retain the platform execution ID.
The labels `adx.instance_id`, `adx.tenant_id`, `adx.runtime_id` and
`adx.generation` allow a new adapter process to recover that association.

Evidence for this change is separate under `out/ci/backend-generated-id/`.
`source-manifest.json` records the changed source and runtime binary hashes;
`green.log` records 46 passing Node Manager tests and the Linux ARM64 build.
The contract tests cover generated IDs, mapping recovery on restart, and cleanup
of an uncommitted runtime discovered by its labels.

The updated binary passed the real two-node SDK flow (`sdk/sdk-result.json`,
`e2e-resumed.log`). A second pair of live instances survived termination and
supervisor restart of both Node Managers: backend IDs were unchanged and the
installed SDK could query and execute commands on the original instances
(`restart-result.json`, `backend-before-node{1,2}.txt`,
`backend-after-node{1,2}.txt`). Subsequent `adxctl stop` removed both instances;
each independently hosted sandboxd reported an empty list (`restart.log`,
`sandboxd-after-stop-node{1,2}.txt`). This validates Node Manager process restart,
not cross-node recovery or a Buildkite run.

## Findings fixed during integration

- API Server has a loopback-only HTTP option for Edge's existing control forwarder.
  Internal RPC retains mTLS; public HTTPS remains the default.
- Managed Redis supports a password file and requires one for non-loopback binds.
- The sandboxd adapter leaves `StartRequest.sandbox_id` empty and records
  `StartResponse.id` as the backend ID for List/Stats/Delete. Platform execution
  IDs remain independent. Managed labels rebuild the mapping during inventory
  or cleanup after Node Manager restart.
- Explicit Start argument/authentication rejection permits cleanup; ambiguous
  transport failure still retains resources until reconciliation.
- API Server restores the SDK's filtered `GET /api/instances?instance_id=...`
  compatibility endpoint, including authentication and tenant ownership checks.

## Boundaries

The basic case uses `idle_timeout=0` and runc without a writable-layer quota.
The SDK default idle timeout is not connected yet. This does not validate
pause/resume, reusable snapshots, automatic recovery, XPU, tunnels, affinity or
performance. runc in this sandboxd revision rejects writable-layer quotas;
runsc/Firecracker require separate coverage.

On ARM64, sandboxd's image-index lookup defaults to AMD64, so this run uses a
single-platform ARM64 manifest. OCI work directories use tmpfs to avoid nested
Docker + OCI + runc OverlayFS depth limits. Nydus/distill_fs is not installed or
exercised; its periodic optional chunk-stat probe logs a missing-binary error.
These are environment/backend coverage limits, not passing claims.

All test instances, both test containers and the dedicated Docker network were cleaned up. Build caches, release artifacts and evidence remain for inspection; no commit or push was performed.

## Repository-owned E2E driver

The tracked `build/e2e/prepare.py` and `build/e2e/run.py` now reproduce the full
platform from a portable image bundle, without mounting the product checkout
into the nodes. On Linux ARM64, the bundle built from the verified package above
passed all five mandatory scenarios: SDK create/command/file/delete, API Key and
tenant isolation, full-capacity waiting followed by rescheduling, Node Manager
restart, and supervisor stop. Both test containers and the isolated network were
removed with no cleanup error.

Evidence is under `out/ci/e2e-driver/`: `bundle-v2/bundle.json` identifies the
package/backend/images, `acceptance-v3/result.json` and `junit.xml` record the
complete gate, and the scenario reports plus node logs record actual behavior.
The driver also has eight passing local tests covering integrity, CI commit and
architecture checks, mandatory scenarios, original-error preservation, cleanup
failure, and registry archive import (`final-unit.log`). Earlier failed registry
attempts retain separate results and also confirmed scoped cleanup.

`.buildkite/pipeline.yml` uses the same build/deployment entrypoints with an
artifact handoff. Its YAML/dependency/artifact structure and shell syntax were
checked locally. At that time a native amd64 Buildkite run had not been triggered; configuring
queues and digest-pinned runtime images and publishing a clean revision remain
necessary. The external backend build script and native release build were not
rerun in this driver validation; it reused the previously verified package and
pinned backend binaries, checking their hashes before image preparation.

## Kubernetes pipeline integration

Buildkite deployment now uses `build/e2e/kubernetes/run.py`: publish immutable
node/RRT image references, create an isolated target namespace, start two node
Pods and Services, run the shared SDK scenarios, collect diagnostics and verify
namespace cleanup. The local Docker driver remains a development regression tool.

Seventeen local contract tests passed (`out/ci/k8s-e2e/final-tests.log`), including
Secret projection, Service/Pod layout, immutable image handoff, namespace UID
ownership and cleanup after a failed stop. The shared fixture's local regression
also passed all five scenarios and cleaned its containers/network
(`out/ci/k8s-e2e/local-regression/result.json`). Kubernetes-specific Pod addressing
and backend startup waiting were checked in source/contract validation only.
At that time no target kubeconfig was available for a real cluster run; neither these tests
nor the prior Docker results establish Kubernetes or remote Buildkite acceptance.

The Kubernetes pipeline now directly reuses the existing CI `default/linux/amd64`
queue, Rust builder, sandbox packager/deployer, Agent Stack target-kubeconfig
mount and SWR secrets. ADX supplies its own namespace and component artifacts.
Compilation, image publication and Kubernetes acceptance are distinct mandatory
steps. The separate ADX deployer-image recipe was removed. Reuse/credential
contracts are included in `out/ci/k8s-e2e/reuse-final-tests.log`; this is source and
local contract evidence, not a hosted CI run.

## 2026-09-16 放置约束扩展

该批次基础门禁扩展为六组（当前为七组）。package-v17 已完成新一轮双节点真实验收，包含六项亲和／反亲和和节点偏好规则；结果、制品身份与正式 K8s 边界见 [本轮记录](2026-09-16-placement-e2e.md)。以上较早结果保留其当时的覆盖范围。
