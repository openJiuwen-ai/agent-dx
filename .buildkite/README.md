# ADX Buildkite pipelines

ADX uses four independent Buildkite pipelines backed by one repository:

| Buildkite pipeline | Configuration | Responsibility |
|---|---|---|
| `agent-dx` | `pipeline-package.yml` | Rust/platform checks, base package and optional OBS publication |
| `agent-dx-python-sdk` | `pipeline-sdk.yml` | Python SDK tests, wheel/sdist, clean install smoke and optional OBS/PyPI publication |
| `agent-dx-admin` | `pipeline-admin.yml` | `adxadmin` tests, wheel/sdist, clean install smoke and optional PyPI/TestPyPI publication |
| `agent-dx-full-test` | `pipeline-full.yml` | Compose exact base/SDK candidates and run the ten-group Kubernetes Full gate |

`.buildkite/pipeline.yml` only dispatches by `BUILDKITE_PIPELINE_SLUG`; it does
not contain product build or test jobs. Collector mirroring and target-node
preparation are isolated in `pipeline-maintenance.yml`. The Full E2E job uses a
mounted target kubeconfig to create a unique `adx-e2e-*` namespace and deletes
that namespace when the run finishes.

The current formal validation used commit
`a4798032e96a602bd58cc13c1312cd01e67effb3` across all three pipelines:
[base package #68](https://buildkite.com/agent-dx/agent-dx/builds/68),
[Python SDK #4](https://buildkite.com/agent-dx/agent-dx-python-sdk/builds/4), and
[Full Test #4](https://buildkite.com/agent-dx/agent-dx-full-test/builds/4).
The optimized base build completed in 3 minutes 30 seconds, compared with
7 minutes 11 seconds for the previous serial #65 build. Full passed all ten
groups on two physical workers with no missing checks or cleanup errors.
The `agent-dx-admin` configuration was added later and is not part of that
historical three-pipeline validation record.

Create a Buildkite pipeline named `agent-dx-admin` against the same repository
and keep the default configuration path `.buildkite/pipeline.yml`; the selector
dispatches that slug to `pipeline-admin.yml`. Set the pipeline default
`ADX_ADMIN_PYPI_UPLOAD=0`. A release build must be created from the exact
`adxadmin-v<version>` Git tag and explicitly override the upload variable to
`1`; set `ADX_ADMIN_PYPI_REPOSITORY=testpypi` for a rehearsal.

## Base package deployment mode

The base package ships `adxctl`, `adx-coordinator`, `adxlet`, `adx-apiserver`
and Redis. Ingress runs inside API Server; Relay runs inside adxlet. Separate
Ingress/Relay executables and the debug forwarder are not compiled or archived
by release steps. Execd and the SDK remain in the unified release archive.
Set `ADX_OBS_UPLOAD=1` when triggering the base pipeline to enable the dependent
`platform-obs` job. It verifies the assembled archive and build manifest before
uploading artifacts and publishing `out/buildkite/obs/manifest.json` and URLs.

## Optional PyPI publication

`admin-package` always runs the `adxadmin` unit and release tests, builds one
wheel and one sdist with `python -m build`, checks both with Twine, installs the
wheel in a source-free virtual environment and writes `admin-candidate.json`.
The candidate records the exact commit, Buildkite build ID and SHA256 of both
files.

`admin-pypi` is omitted unless the build explicitly sets
`ADX_ADMIN_PYPI_UPLOAD=1`. It accepts `ADX_ADMIN_PYPI_REPOSITORY=pypi` (the
default) or `testpypi`, and only publishes a build whose tag exactly matches
`adxadmin-v<package-version>`. The step consumes the candidate from
`admin-package`, never rebuilds it, and does not use `--skip-existing`. After
upload, it reads the selected index JSON API and verifies the exact filenames
and SHA256 values before writing `out/buildkite/admin-publish/publish.json`.

The existing `sdk-package` step follows the same candidate contract for
`adx-sandbox`. `sdk-pypi` is omitted unless `ADX_SDK_PYPI_UPLOAD=1` on an exact
`sdk-v<package-version>` tag. `ADX_SDK_PYPI_REPOSITORY` selects `pypi` (the
default) or `testpypi`; successful readback is written to
`out/buildkite/sdk-publish/publish.json`. OBS upload remains independent and is
still controlled by `ADX_OBS_UPLOAD`.

The Kubernetes execution namespace must contain this Secret:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: adx-pypi-credentials
type: Opaque
stringData:
  admin-pypi-token: pypi-...
  admin-testpypi-token: pypi-...
  sandbox-pypi-token: pypi-...
  sandbox-testpypi-token: pypi-...
```

The four entries are optional independently; a selected package and repository
fails closed when its token is absent. Buildkite passes the selected token as
`TWINE_PASSWORD`, with username `__token__`; credentials do not enter argv or
artifacts. Because PyPI does not currently list Buildkite as a Trusted
Publishing provider, this pipeline uses a project-scoped API token. The first
upload of a previously nonexistent project may require an account-scoped token;
rotate it immediately to a project-scoped token after the project exists.

Create or update the Secret from protected local files, avoiding token values
in shell history or command arguments:

```sh
umask 077
# Write each token to the corresponding file without committing the files.
kubectl -n <buildkite-agent-namespace> create secret generic adx-pypi-credentials \
  --from-file=admin-pypi-token=/secure/adxadmin-pypi.token \
  --from-file=admin-testpypi-token=/secure/adxadmin-testpypi.token \
  --from-file=sandbox-pypi-token=/secure/adx-sandbox-pypi.token \
  --from-file=sandbox-testpypi-token=/secure/adx-sandbox-testpypi.token \
  --dry-run=client -o yaml | kubectl apply -f -
```

## Deployment

| Resource in the test namespace | Processes |
|---|---|
| `node1` Pod | Redis, Coordinator with embedded Shard, Sandbox API, Ingress, Adxlet, Relay, independently hosted sandboxd |
| `node2` Pod | Adxlet, Relay, independently hosted sandboxd |
| `coordinator` / `node2` Services | Service addresses for Redis, Coordinator and Ingress; nodes advertise their Pod IPs for RPC/forwarding |
| `adx-test-credentials` Secret | Short-lived test certificates and API/Redis keys |
| `adx-test-registry` Secret, when configured | Pod image pulls and sandboxd EXECD image pulls |

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
requests from node1 enter Ingress over TLS and traverse the real control/data paths.

## Existing CI infrastructure

These are Buildkite execution-cluster resources. They are independent of the
two-worker target-cluster requirements documented in the
[Kubernetes E2E README](../build/e2e/kubernetes/README.md). The current Agent
Stack requests/limits are: each Platform/Gateway/EXECD/source-gate compiler
`4/8 CPU` and `8/16 GiB`, package assembly and OBS publication `1/2 CPU` and
`2/4 GiB`, image publish `8 CPU / 16 GiB`, and the E2E deployer `2/4 CPU` and
`4/8 GiB`. The target
kubeconfig selects a second cluster where the ADX Pods are deployed.

All product steps use the existing `default` queue with `os=linux`, `arch=amd64`
and the Kubernetes plugin. Worker images follow the existing CI profiles:

| Step | Reused worker image |
|---|---|
| `build-platform` / `build-gateway` / `build-execd` / `source-gate` | immutable `ci_image` digest in `build/images/build-environment.json` |
| `platform-build` / `platform-obs` | same immutable ADX build image |
| `sdk-package` / `platform-images` | `swr.cn-southwest-2.myhuaweicloud.com/yuanrong-dev/sandbox-packager:v20260506_kubectl` |
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
`ADX_E2E_RUNTIME_BASE` / `ADX_E2E_EXECD_BASE` digest-pinned runtime bases. Reusing
worker images does not change the product's toolchain or dependency pins. The
digest-pinned ADX build image already contains Rust 1.95.0, Go 1.25.5, Redis
7.2.5, musl, erofs-utils 1.8.10 and the Python build dependencies. Bootstrap
verifies these pins and fails instead of downloading or compiling missing tools.
`GOROOT` is set together with `PATH` so the worker's preinstalled Go cannot mix
standard libraries with the selected compiler. The bundled Redis uses the same
libc/plain transport build as local acceptance, avoiding a host OpenSSL ABI
dependency in the Ubuntu runtime image.

## Artifact handoff and acceptance

`build-platform`, `build-gateway` and `build-execd` compile in parallel with
separate target directories, while `source-gate` runs the source and unit-test
gate. Each compile step uploads a component archive and manifest. After all four
steps pass, `platform-build` verifies those immutable handoff artifacts and
assembles the ADX base package without recompiling them. `build-manifest.json`
binds the source commit, component manifests, release archive, backend bundle
and compatibility SDK copy by SHA256.

The assembly step downloads the pinned external sandboxd backend artifact selected by
`ADX_BACKEND_ARTIFACT_BUILD` and verifies its revision, target and complete file
digests. `sdk-package` independently tests the public SDK on Python 3.12, creates
one wheel and one sdist, installs the wheel in a source-free virtual environment,
and publishes `sdk-candidate.json` with commit, version and SHA256 values.

The current base archive still carries a convenience copy of the SDK wheel for
standalone installation compatibility. That copy is not the SDK candidate and
is never selected by Full. Full installs only the wheel named and hashed by the
independent `sdk-package` candidate.

The Full pipeline requires `ADX_BASE_PACKAGE_BUILD_ID` and `ADX_SDK_BUILD_ID`.
`platform-images` downloads both immutable candidates by Buildkite build UUID,
verifies their commits and digests, restores the base package tree and injects
the independently built SDK wheel into the test image. It then publishes the
node, EXECD and entrypoint-fixture images. `registry-images.json` records immutable digest
references, source image IDs and the checksum of `bundle.json`.

`platform-e2e` downloads only those JSON manifests and invokes
`build/e2e/kubernetes/run.py`. It verifies their relationship, commit, architecture
and immutable references. The target cluster pulls the build's images; neither
the deployer nor test Pods compile or substitute product binaries.

The independent Full pipeline always selects the `full` profile. It runs SDK
create/query/command/file/delete, API-key and tenant isolation, capacity,
two-node placement, local-first atomic ownership, the broad data-plane surface,
idle lifecycle reclamation, heartbeat expiry with returning-node cleanup, Node
Manager restart, and supervisor stop with physical runtime cleanup. Missing
selected scenarios, diagnostic or cleanup failures
prevent a pass. `result.json`, JUnit, Kubernetes resource/events and per-Pod logs
are uploaded. Secret bodies travel on stdin and are excluded from manifests and
evidence; generated API/Redis keys are redacted from collected component logs.

The Full pipeline sets `ADX_E2E_PROFILE=full`. Direct local driver runs may still
select `l0` or `k8s-basic` for bounded diagnosis. Full fails after scheduling
when both platform Pods land on the same
worker. `ADX_E2E_NODE_NAMES` may restrict eligible workers, but the recorded
actual placement remains the acceptance evidence.

To rerun acceptance against an already published immutable image bundle, set
both `ADX_E2E_ARTIFACT_BUILD` to the source Buildkite build UUID and
`ADX_E2E_ARTIFACT_COMMIT` to that bundle's 40-character product commit. The
release and image jobs are skipped; the E2E job downloads the source build's
`platform-images` manifests, verifies their bundle digest and product commit,
and deploys the recorded image digests. After the Pods become ready, the driver
copies the current checkout's E2E harness into both Pods and verifies every file
digest before preflight or service startup. This keeps the product binaries and
images fixed at the selected artifact commit while allowing a newer test commit
to repair or extend acceptance logic. `result.json` and `harness.json` record
both identities. A build number or an unpaired commit is rejected.

CI worker prerequisites are changed only by the explicit maintenance mode
`ADX_K8S_NODE_PREPARE_ONLY=1`. It also requires the exact comma-separated
`ADX_K8S_PREPARE_NODE_NAMES` and an immutable source image build through
`ADX_E2E_ARTIFACT_BUILD`. The maintenance Pod is pinned to each named node,
loads `br_netfilter`, writes the modules-load and sysctl configuration under the
host `/etc`, verifies `net.bridge.bridge-nf-call-iptables=1`, and then removes
its isolated namespace. Ordinary builds and E2E runs never mutate host settings.

Capacity checks also save Coordinator/Node resource scrapes at allocated, queued and
released points. The unified supervisor runs with log rotation enabled in both
Pods. Stop checks decompress the closed gzip files, reject unfinished compression
and reported log I/O failures, and save `logging-node1.json` / `logging-node2.json`.
The E2E log includes `[METRICS PASS]` and `[LOGGING PASS]` evidence.

Cleanup validates namespace ownership label and UID, stops nodes in reverse
order while Coordinator is still present, deletes the namespace and confirms absence.
A replaced/unowned namespace is never deleted. A host/job SIGKILL can interrupt
cleanup; the recorded run ID and namespace label identify only that run's
resources for recovery, and a missing result cannot count as passed.

Kubernetes deployment has contract tests; real-cluster execution requires the
existing CI target access and a committed ADX revision. Prior Docker-based local E2E
results remain local evidence, not Kubernetes acceptance. The local driver is
available separately for development reproduction in [build/e2e](../build/e2e/README.md).

Buildkite syntax follows the official [Agent Stack execution](https://buildkite.com/docs/agent/self-hosted/agent-stack-k8s/running-builds)
and [PodSpec configuration](https://buildkite.com/docs/agent/self-hosted/agent-stack-k8s/podspec).

构建会在复用的 worker 中按版本和 SHA256 准备 Go 1.25.5、Redis 7.2.5。基础流水线直接下载并校验固定的 sandboxd 后端产物，不访问 GitHub 重建；只有显式取消 `ADX_BACKEND_ARTIFACT_BUILD` 时才进入源码构建维护路径。未指定基础镜像时，打包步骤按仓库 Dockerfile 构建并发布测试基础镜像，再用 registry digest 构建节点与 EXECD 镜像。`ADX_REDIS_SERVER` / `ADX_REDIS_CLI` 和 `ADX_E2E_RUNTIME_BASE` / `ADX_E2E_EXECD_BASE` 可显式覆盖。

## ADX build image and Cargo cache

`build/images/build-environment.json` is the source of truth for the immutable
Linux AMD64 build-image digest. Regular package jobs run through
`.buildkite/run-build-container.sh` and never install build dependencies at job
time. Set `ADX_BUILD_IMAGE_SYNC_ONLY=1` on the base pipeline to dispatch the
maintenance job that builds the Ubuntu 20.04 recipe, pushes the immutable image,
verifies it by digest and records `out/buildkite/build-image/result.json`. A
mutable `buildcache` tag may seed BuildKit cache only; product jobs always use
the recorded digest.

`.buildkite/setup-cargo.sh` restores the image's rsproxy sparse source settings in
the persistent ADX Cargo home, including Git dependency caching.
It exports `CARGO_HOME` from `ADX_CARGO_HOME` inside the build process so image
profile initialization cannot silently redirect downloads back to `/root/.cargo`.
Registry and Git downloads survive job Pods under `/mnt/paas`. Platform,
Gateway, EXECD and source-gate use separate target directories by architecture and
toolchain, so they can run concurrently without copying another job's outputs.
Package assembly consumes only uploaded component archives; it does not read a
compiler job's Cargo target directory.
An existing sccache from the shared worker cache is reused when available, and
Cargo cache locations/source selection are recorded in `bootstrap.log`.

`ADX_BACKEND_ARTIFACT_BUILD` selects the Buildkite build UUID containing the
`platform-build` backend artifacts. The default gate uses the verified artifacts
from build `01a0ad6d-9629-4da8-903b-3f8bd1ddc992` (Buildkite #21). Revision,
target, complete file set and every file checksum must pass validation. ADX
product binaries and SDK are still built from the current commit. Unset the
variable only for an explicit external-runtime rebuild from its pinned source.

The image stage caches the pinned Ubuntu amd64 base in SWR. On a cache miss it
pulls the identical manifest from a configurable regional mirror and checks its
config/image digest before publishing. The runtime tools image is cached by its
Dockerfile/bootstrap recipe; all final image inputs use SWR digest references.

## Build page logs and artifact summary

Every stage streams stdout/stderr to the Buildkite job log and retains the same
output as `out/buildkite/logs/step-<stage>.log`. Compiler and image-build output
is grouped by phase. `pipefail` preserves command failures through `tee`.
Toolchain setup stays in the current shell so exported cache paths reach builds.

Package and Full stages update the `adx-build-summary` annotation and upload
their Markdown/JSON summaries. The package summary links the build manifest as
well as the release and backend artifacts. The SDK pipeline publishes its candidate,
JUnit and install-smoke evidence as independent artifacts. Full summaries link
image provenance, result JSON and JUnit, record actual Pod/host placement and
distinguish same-host runs.
Failures still publish a summary and retain their original exit status.

## OBS artifact publication

Set `ADX_OBS_UPLOAD=1` to add the `platform-obs` step after `platform-build`.
The step downloads the exact Buildkite artifacts produced by that build, verifies
the release archive, package manifest, build manifest and sandboxd backend bundle, and then
uploads them to Huawei Cloud OBS. It does not rebuild any product binary.

The Kubernetes worker reads `AK` and `SK` from the existing
`obs-credentials` Secret and exposes them only as `OBS_ACCESS_KEY_ID` and
`OBS_SECRET_ACCESS_KEY`. Credentials are not passed on argv and are not written
to Buildkite artifacts. The pipeline pins the destination to bucket
`openyuanrong` at `obs.cn-southwest-2.myhuaweicloud.com`; changing it requires a
reviewed pipeline update rather than a per-build override.

The default channel is `daily`:

```text
adx/daily/<UTC timestamp>-<commit12>/linux/amd64/<artifact>
```

Set `ADX_OBS_UPLOAD_CHANNEL=release` and either `ADX_RELEASE_VERSION` or a
Buildkite tag to publish under:

```text
adx/release/<version>/linux/amd64/<artifact>
```

Each upload publishes `manifest.json` with the source commit, Buildkite build
ID, object URL, size and SHA256 for every file. The uploader reads object
metadata back and rejects an absent object or a size mismatch. The same manifest
and a compact `urls.txt` are retained under `out/buildkite/obs/`; the manifest
URL is also stored as Buildkite metadata `obs-manifest-url`.

The base pipeline uploads the release archive, release manifest and verified
runc Runtime Pack. The SDK pipeline's `sdk-obs` step separately verifies and
uploads its wheel, sdist and `sdk-candidate.json`. Set `ADX_OBS_UPLOAD=1` on the
pipeline that owns the candidate. Neither upload result is a Full deployment
acceptance verdict; Full consumes the two exact Buildkite build UUIDs.

## Firecracker checkpoint profile

`ADX_E2E_CHECKPOINT=1` adds the independent `platform-fc-e2e` job. It requires a
checksummed native Firecracker kit and an explicitly selected KVM-capable target
worker. The image step combines that kit with the same verified ADX release;
the test step runs the public SDK checkpoint, snapshot and node-fault cases inside
an isolated Kubernetes Pod and publishes deployment, per-case, JUnit and cleanup
evidence. See [runtime kit and invocation](../build/e2e/firecracker/README.md).
The existing basic E2E result alone does not count as this profile passing.

组件日志采集验收复用现有 Ingress/Relay 指标端点，并通过真实 OpenTelemetry Collector 接收结构化组件日志。stop 组包含后端 503、文件滚动与 Collector 重启，控制台输出 `[METRICS PASS]` / `[COLLECTION PASS]`；产物含 `gateway-metrics-node*.json`、`collection-node*.json`、`collected-logs.jsonl` 和 `collector-process.log`。部署及保证边界见 `docs/testing/log-collection.md`。Trace验收输出 `[TRACE PASS]`，保存 `traces-node*.json` 与 `collected-traces.jsonl`，检查完整创建链路和实例队列父子关系；配置见 `docs/testing/distributed-traces.md`。

### Collector 镜像同步

设置 `ADX_COLLECTOR_SYNC_ONLY=1` 触发独立 `collector-sync` 步骤，跳过编译、产品镜像和 K8s 验收。复用现有 SWR Secret，将 `build/observability/source.json` 锁定的 Linux AMD64 manifest 同步至 `ADX_E2E_IMAGE_REPOSITORY`。默认从 GHCR 拉取，可通过 `ADX_COLLECTOR_SYNC_SOURCE` 指定其他仓库，摘要保持不变。

步骤日志记录下载、推送和按原摘要回拉，产物位于 `out/buildkite/collector-sync/`，含 `result.json` 与镜像地址。同步成功后才更新正式构建的 Collector 默认地址；同步步骤通过不代表 K8s 验收通过。

正式镜像构建默认使用 `source.json` 中的 `ci_image`，即西南二区 SWR 的 Linux AMD64 固定摘要，已通过 [Buildkite #20](https://buildkite.com/agent-dx/agent-dx/builds/20) 推送及回拉验证。`ADX_COLLECTOR_IMAGE` 可覆盖完整引用；`ADX_COLLECTOR_MIRROR` 继续用于选择承载上游多架构索引的镜像站。本地构建默认仍使用上游多架构镜像。

`ADX_BACKEND_ARTIFACT_BUILD` 接受 Buildkite build UUID（API 返回的 `id`），不是页面上的递增构建编号。复用制品仍需匹配 sandboxd 提交、目标架构与完整文件摘要；ADX 产品每次从当前提交构建。

The basic acceptance driver also runs an independent `local-first` case: restart API Server with `create_mode=local_first`, verify concurrent public SDK creation, real commands and deletion, require confirmed-claim evidence, then restore central mode. The default `k8s-basic` profile contains five bounded cases. The ten-case `full` and local `standalone` profiles carry installed-SDK `data-plane`, `lifecycle` and fault/restart/stop coverage. [Buildkite #30](https://buildkite.com/agent-dx/agent-dx/builds/30) passed the previous eight-case OCI profile; the current profile split requires fresh formal evidence.

### Local runtime payload tools

The immutable Ubuntu 20.04 ADX build image provides erofs-utils 1.8.10 built
from pinned commit `51b5939b5f783221310d25146e6a2019ba8129b6`.
Both `mkfs.erofs` and `fsck.erofs` are required; the distribution's 1.0 package
lacks the checker. The runtime payload stays uncompressed and disables inline
data; the tools are build dependencies and are not included in the release.
EROFS deployments preflight the packaged payload through a read-only loop
mount. Listing `erofs` in `/proc/filesystems` alone is insufficient because
some worker kernels register the driver but reject block-backed mounts. The
Kubernetes acceptance profile uses the digest-pinned OCI EXECD image instead and
therefore checks bridge networking without requiring EROFS; standalone tests
continue to exercise the packaged EROFS source.
