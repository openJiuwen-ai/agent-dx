# ADX Kubernetes E2E pipeline

`.buildkite/pipeline.yml` builds the current revision, publishes its node and RRT
images, then deploys and tests ADX in the target Kubernetes cluster. The E2E step
runs as a Buildkite Agent Stack for Kubernetes job. It uses a mounted target
kubeconfig to create a unique `adx-e2e-*` namespace and deletes that namespace
when the run finishes.

## Deployment

| Resource in the test namespace | Processes |
|---|---|
| `node1` Pod | Redis, Master with embedded Shard, Sandbox API, Edge, Node Manager, Node Proxy, independently hosted sandboxd |
| `node2` Pod | Node Manager, Node Proxy, independently hosted sandboxd |
| `master` / `node2` Services | Service addresses for Redis, Master and Edge; nodes advertise their Pod IPs for RPC/forwarding |
| `adx-test-credentials` Secret | Short-lived test certificates and API/Redis keys |
| `adx-test-registry` Secret, when configured | Pod image pulls and sandboxd RRT image pulls |

Each node Pod runs the unified package through `adxctl`. sandboxd is started by
the test fixture, outside the product supervisor. Node containers are privileged
for nested runc/network/cgroup operations. They have private Pod networking,
3 CPU / 4 GiB limits, and emptyDir volumes for runtime state and evidence; the
image work directory uses a memory-backed emptyDir. Host PID/network namespaces,
hostPath volumes and host Docker sockets are not part of this deployment.
Preferred Pod anti-affinity spreads the two nodes when the cluster has capacity;
this does not establish physical-host fault isolation.

Kubernetes Pod Ready is only the first deployment check. Acceptance waits for
both ADX nodes to register, reconcile and become available/routable. Public SDK
requests from node1 enter Edge over TLS and traverse the real control/data paths.

## Existing CI infrastructure

All three steps use the existing `default` queue with `os=linux`, `arch=amd64`
and the Kubernetes plugin. Worker images follow the existing CI profiles:

| Step | Reused worker image |
|---|---|
| `platform-build` | `swr.cn-southwest-2.myhuaweicloud.com/yuanrong-dev/compile-ubuntu2004-rust:v20260826_rust1950_musl_x86_64` |
| `platform-images` | `swr.cn-southwest-2.myhuaweicloud.com/yuanrong-dev/sandbox-packager:v20260506_kubectl` |
| `platform-e2e` | `swr.cn-southwest-2.myhuaweicloud.com/yuanrong-dev/sandbox-deployer:v20260506_kubectl_py39` |

The builder reuses `/mnt/paas` with ADX cache subdirectories. The packager uses
its privileged Docker setup and an emptyDir at `/var/lib/docker`, starting only
its own daemon when required. The deployer uses the existing Agent Stack patch:
Secret `sandbox-target-kubeconfig` is mounted at
`/var/run/yr-k8s/target/kubeconfig` on `container-0`. Container names are left to
the controller so this patch continues to apply. `ADX_KUBE_CONTEXT` may select
a context from that file. The target kubeconfig needs permission to manage the
run's namespace, Pods, Services and Secrets and to exec/copy/read diagnostics.

Registry integration reuses `swr-pull-secret` and `swr-credentials` with the same
`SWR_DOCKER_CONFIG_JSON`, `SWR_USERNAME`, `SWR_PASSWORD` injection. The wrapper
`.buildkite/with_registry.py` writes a private temporary Docker config, passes it
to the packager/deployer, and removes it on exit. It forwards cancellation to
the child so namespace cleanup can finish. No credential values enter argv or
uploaded artifacts. ADX images default to the existing SWR organization under
`swr.cn-southwest-2.myhuaweicloud.com/yuanrong-dev/adx-e2e`.

ADX build inputs remain explicit: the root Rust toolchain, Go/protobuf/Python
build tools, matching `ADX_REDIS_SERVER` / `ADX_REDIS_CLI` 7.2.5 binaries, and
`ADX_E2E_RUNTIME_BASE` / `ADX_E2E_RRT_BASE` digest-pinned runtime bases. Reusing
worker images does not change the product's toolchain or dependency pins. The bootstrap prepares pinned Go and Redis dependencies in the worker cache.
`GOROOT` is set together with `PATH` so the worker's preinstalled Go cannot mix
standard libraries with the selected compiler. The bundled Redis uses the same
libc/plain transport build as local acceptance, avoiding a host OpenSSL ABI
dependency in the Ubuntu runtime image.

## Artifact handoff and acceptance

`platform-build` constructs all ADX binaries, the SDK and pinned sandboxd helpers
from the clean current commit. `platform-images` downloads the release archive and verifies its SHA256 before
restoring the complete directory tree and executable permissions. It downloads
the verified external backend artifacts,
creates the verified bundle and publishes the node/RRT images. `registry-images.json` records immutable digest
references, source image IDs and the checksum of `bundle.json`.

`platform-e2e` downloads only those JSON manifests and invokes
`build/e2e/kubernetes/run.py`. It verifies their relationship, commit, architecture
and immutable references. The target cluster pulls the build's images; neither
the deployer nor test Pods compile or substitute product binaries.

Mandatory scenarios are SDK create/query/command/file/delete, invalid key and
tenant isolation, administrator key management through Edge, capacity
exhaustion/release, six two-node placement rules,
heartbeat expiry with returning-node cleanup, Node Manager process restart and
supervisor stop with physical runtime cleanup. Missing scenarios, diagnostic or
cleanup failures prevent a pass. `result.json`, JUnit, Kubernetes resource/events
and per-Pod logs are uploaded. Secret bodies travel on stdin and are excluded
from manifests and evidence; generated API/Redis keys are redacted from collected
component logs.

Capacity checks also save Master/Node resource scrapes at allocated, queued and
released points. The unified supervisor runs with log rotation enabled in both
Pods. Stop checks decompress the closed gzip files, reject unfinished compression
and reported log I/O failures, and save `logging-node1.json` / `logging-node2.json`.
The E2E log includes `[METRICS PASS]` and `[LOGGING PASS]` evidence.

Cleanup validates namespace ownership label and UID, stops nodes in reverse
order while Master is still present, deletes the namespace and confirms absence.
A replaced/unowned namespace is never deleted. A host/job SIGKILL can interrupt
cleanup; the recorded run ID and namespace label identify only that run's
resources for recovery, and a missing result cannot count as passed.

Kubernetes deployment has contract tests; real-cluster execution requires the
existing CI target access and a committed ADX revision. Prior Docker-based local E2E
results remain local evidence, not Kubernetes acceptance. The local driver is
available separately for development reproduction in [build/e2e](../build/e2e/README.md).

Buildkite syntax follows the official [Agent Stack execution](https://buildkite.com/docs/agent/self-hosted/agent-stack-k8s/running-builds)
and [PodSpec configuration](https://buildkite.com/docs/agent/self-hosted/agent-stack-k8s/podspec).

构建会在复用的 worker 中按版本和 SHA256 准备 Go 1.25.5、Redis 7.2.5，用于外部 sandboxd 构建。未指定基础镜像时，打包步骤按仓库 Dockerfile 构建并发布测试基础镜像，再用 registry digest 构建节点与 RRT 镜像。`ADX_REDIS_SERVER` / `ADX_REDIS_CLI` 和 `ADX_E2E_RUNTIME_BASE` / `ADX_E2E_RRT_BASE` 可显式覆盖。

## Rust image and Cargo cache

The Rust worker uses the existing Rust 1.95.0 image pinned by registry digest.
CI selects its preinstalled `stable` toolchain and disables automatic toolchain
installation; bootstrap verifies the actual version against `rust-toolchain.toml`.
The same numeric pin applies to local builds.

`.buildkite/setup-cargo.sh` restores the image's rsproxy sparse source settings in
the persistent ADX Cargo home, including Git dependency caching.
It exports `CARGO_HOME` from `ADX_CARGO_HOME` inside the build process so image
profile initialization cannot silently redirect downloads back to `/root/.cargo`. Both registry/git
downloads and release compilation outputs survive job Pods under `/mnt/paas`.
The release target cache is separated by architecture and toolchain; the build
step is serialized so package assembly cannot copy another job's binaries.
An existing sccache from the shared worker cache is reused when available, and
Cargo cache locations/source selection are recorded in `bootstrap.log`.

For a pinned external sandboxd already built by CI, `ADX_BACKEND_ARTIFACT_BUILD`
can select the Buildkite build UUID containing its `platform-build` backend
artifacts. Revision, target, complete file set and every file checksum must pass
validation. ADX product binaries and SDK are still built from the current commit.
Omit the variable to rebuild the external runtime from its pinned source.

The image stage caches the pinned Ubuntu amd64 base in SWR. On a cache miss it
pulls the identical manifest from a configurable regional mirror and checks its
config/image digest before publishing. The runtime tools image is cached by its
Dockerfile/bootstrap recipe; all final image inputs use SWR digest references.

## Build page logs and artifact summary

Every stage streams stdout/stderr to the Buildkite job log and retains the same
output as `out/buildkite/logs/step-<stage>.log`. Compiler and image-build output
is grouped by phase. `pipefail` preserves command failures through `tee`.
Toolchain setup stays in the current shell so exported cache paths reach builds.

Each stage updates the `adx-build-summary` build annotation and uploads its
Markdown/JSON summary. Later stages extend the earlier summary with immutable
image references and Kubernetes results. The summary links the release archive,
SHA256, standalone manifest, SDK wheel, build logs, image provenance, result JSON
and JUnit. It records actual Pod/host placement and distinguishes same-host runs.
Failures still publish a summary and retain their original exit status.

## Firecracker checkpoint profile

`ADX_E2E_CHECKPOINT=1` adds the independent `platform-fc-e2e` job. It requires a
checksummed native Firecracker kit and an explicitly selected KVM-capable target
worker. The image step combines that kit with the same verified ADX release;
the test step runs the public SDK checkpoint, snapshot and node-fault cases inside
an isolated Kubernetes Pod and publishes deployment, per-case, JUnit and cleanup
evidence. See [runtime kit and invocation](../build/e2e/firecracker/README.md).
The existing basic E2E result alone does not count as this profile passing.

组件日志采集验收复用现有 Edge/Node Proxy 指标端点，并通过真实 OpenTelemetry Collector 接收结构化组件日志。stop 组包含后端 503、文件滚动与 Collector 重启，控制台输出 `[METRICS PASS]` / `[COLLECTION PASS]`；产物含 `gateway-metrics-node*.json`、`collection-node*.json`、`collected-logs.jsonl` 和 `collector-process.log`。部署及保证边界见 `docs/testing/log-collection.md`。Trace验收输出 `[TRACE PASS]`，保存 `traces-node*.json` 与 `collected-traces.jsonl`，检查完整创建链路和实例队列父子关系；配置见 `docs/testing/distributed-traces.md`。

### Collector 镜像同步

设置 `ADX_COLLECTOR_SYNC_ONLY=1` 触发独立 `collector-sync` 步骤，跳过编译、产品镜像和 K8s 验收。复用现有 SWR Secret，将 `build/observability/source.json` 锁定的 Linux AMD64 manifest 同步至 `ADX_E2E_IMAGE_REPOSITORY`。默认从 GHCR 拉取，可通过 `ADX_COLLECTOR_SYNC_SOURCE` 指定其他仓库，摘要保持不变。

步骤日志记录下载、推送和按原摘要回拉，产物位于 `out/buildkite/collector-sync/`，含 `result.json` 与镜像地址。同步成功后才更新正式构建的 Collector 默认地址；同步步骤通过不代表 K8s 验收通过。

正式镜像构建默认使用 `source.json` 中的 `ci_image`，即西南二区 SWR 的 Linux AMD64 固定摘要，已通过 [Buildkite #20](https://buildkite.com/agent-dx/agent-dx/builds/20) 推送及回拉验证。`ADX_COLLECTOR_IMAGE` 可覆盖完整引用；`ADX_COLLECTOR_MIRROR` 继续用于选择承载上游多架构索引的镜像站。本地构建默认仍使用上游多架构镜像。

`ADX_BACKEND_ARTIFACT_BUILD` 接受 Buildkite build UUID（API 返回的 `id`），不是页面上的递增构建编号。复用制品仍需匹配 sandboxd 提交、目标架构与完整文件摘要；ADX 产品每次从当前提交构建。
