# ADX Buildkite pipelines

ADX uses four Buildkite pipelines backed by one repository:

| Buildkite pipeline | Configuration | Responsibility |
|---|---|---|
| `agent-dx` | `pipeline-package.yml` | Platform/Execd/SDK/adxadmin UT and packages, install smoke, real Kubernetes L0, default OBS upload and optional PyPI publication |
| `agent-dx-python-sdk` | `pipeline-sdk.yml` | Python SDK tests, wheel/sdist, clean install smoke and optional OBS/PyPI publication |
| `agent-dx-admin` | `pipeline-admin.yml` | Standalone adxadmin UT, wheel/sdist, install smoke and optional PyPI publication |
| `agent-dx-full-test` | `pipeline-full.yml` | Compose exact base/SDK candidates and run the eleven-group Kubernetes Full gate |

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
## Pipeline controls

Base builds publish verified candidates to OBS by default. PyPI publication remains opt-in.
Set these variables on a Buildkite build or in that pipeline's environment settings.

| Variable | Default | Applies to / responsibility |
|---|---|---|
| `ADX_OBS_UPLOAD` | base `1`, SDK `0` | `0` disables OBS upload for the selected pipeline |
| `ADX_OBS_UPLOAD_CHANNEL` | `daily` | Base/SDK: `daily` or `release` |
| `ADX_RELEASE_VERSION` | tag-derived | OBS release path version |
| `ADX_OBS_BUCKET` / `ADX_OBS_ENDPOINT` | `openyuanrong` / `obs.cn-southwest-2.myhuaweicloud.com` | OBS intermediate transport and final publication destination |
| `ADX_ARTIFACT_TRANSPORT` | `obs` | Base components: `obs` staging or explicit `buildkite` transport |
| `ADX_ADMIN_PYPI_UPLOAD` | `0` | Base/admin: `1` enables adxadmin publication, exact `adxadmin-v<version>` tag required |
| `ADX_ADMIN_PYPI_REPOSITORY` | `pypi` | Base/admin: `pypi` or `testpypi` |
| `ADX_SDK_PYPI_UPLOAD` | `0` | Base/SDK: `1` enables Sandbox SDK publication, exact `sdk-v<version>` tag required |
| `ADX_SDK_PYPI_REPOSITORY` | `pypi` | Base/SDK: `pypi` or `testpypi` |
| `ADX_BASE_PACKAGE_BUILD_ID` / `ADX_SDK_BUILD_ID` | required | Full: exact input candidates; Full never publishes Python packages |

Boolean controls accept only `0` or `1`. Component transport is independent of
final publication: `ADX_OBS_UPLOAD=0` still permits intermediate OBS transfers
when `ADX_ARTIFACT_TRANSPORT=obs`. To build without OBS, select `buildkite` and
set `ADX_OBS_UPLOAD=0`. Missing OBS credentials or corrupt files fail the job;
there is no silent transport fallback. Temporary artifacts use
`adx/ci/<build UUID>/<commit>/<component>/`, with commit/build/group and SHA256
checked before extraction. Configure bucket retention for this temporary prefix
separately from `adx/daily/` and `adx/release/`.

## Python test environment

The base pipeline's `admin-package` uses the same outer Docker runner as the SDK
pipeline, with the digest-pinned Python 3.12 image. It runs UT, builds wheel/sdist,
checks them with Twine and performs a clean installation smoke test. The outer
runner verifies the exact clean Git checkout. The Python image does not need Git.
Rust build tooling continues to use the builder's Python 3.9.

## Base package deployment mode

The base package ships `adxctl`, `adx-coordinator`, `adxlet`, `adx-apiserver`,
`adx-ingress`, `adx-relay` and Redis. Ingress runs inside API Server and Relay
inside adxlet by default; the two standalone binaries support explicit split
process deployments. The debug forwarder remains source-built. Execd and the
SDK remain in the unified release archive.
By default, `platform-build` uploads verified local outputs, including
adxadmin candidates. The same job publishes
`out/buildkite/obs/manifest.json` and URLs without downloading the assembled package again.
The independent `artifact-manifest` step reads this manifest and uploads
`out/buildkite/index.html` to Buildkite Artifacts with links, sizes and SHA256
for every OBS object. Set `ADX_OBS_UPLOAD=0` to skip final OBS publication;
the index then links to the Buildkite artifact list.

## Base artifact set and L0 gate

The base pipeline retains all these downloadable outputs:

- `adx-release.tar.gz`: installable package, including the tested SDK wheel.
- `adx-execd.tar.gz`: independent Execd binary, runtime EROFS filesystem and component manifest; a separate SHA256 file is provided.
- `backend.tar.gz`: pinned sandboxd/runc dependency bundle, distinct from Execd.
- `sdk/`: Sandbox SDK wheel, sdist, candidate and test evidence.
- `admin/`: adxadmin wheel, sdist, candidate and install evidence.

Execd also remains in the unified installer bundle. Its independent archive is
published as a final artifact, not just an intermediate component transfer.
OBS upload includes the independent Execd archive and both Python distributions.

After assembly, `platform-images` reuses the Full image composition code and
`platform-e2e` runs **`l0`** against a real isolated Kubernetes deployment:
create/query, command stdout/stderr/exit code, binary file roundtrip, deletion,
authentication/tenant boundaries, and namespace cleanup. Fault/restart suites
remain in the independent Full pipeline. Base PyPI steps depend on the successful
L0 job. OBS uploads are candidate publication during assembly: an uploaded URL
alone does not establish a passed L0 build. Check the final Buildkite verdict.
L0 cleanup stops services, verifies an empty backend and deletes the test namespace.
Collector outage/restart, complete trace and log rotation assertions remain in
the broader profiles; L0 does not require their fault-injection evidence.

L0 image composition downloads the base candidate once through OBS staging by
default. Full keeps explicit base/SDK build IDs and its existing Buildkite input
transport; `ADX_BASE_ARTIFACT_TRANSPORT=obs` can consume a new build's staged base
candidate while that temporary prefix is retained.

The independent SDK and admin pipelines are convenient ways to build just one
Python package. They share exactly the same package/test scripts with the base
pipeline; they are not prerequisites for a base build. For `agent-dx-admin`, use
this repository and `.buildkite/pipeline.yml` as the configuration entrypoint.

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
| `platform-build` | same immutable ADX build image |
| `admin-package` / `sdk-package` / `platform-images` | `swr.cn-southwest-2.myhuaweicloud.com/yuanrong-dev/sandbox-packager:v20260506_kubectl` |
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

`build-platform`, `build-gateway` and `build-execd` each run their tests before
compiling and packaging. Platform owns shared platform/common crate tests;
Gateway owns API Server, routing and Agent crate tests; Execd owns runtime tests.
The test partition is checked against Cargo workspace membership. Explicitly
ignored integration tests still require their dedicated environments; this is
not the Full E2E gate. `source-gate` owns fmt/Clippy and CI/release tooling checks.
`admin-package` and `sdk-package` produce tested Python artifacts in parallel.

The base SDK step reuses the independent SDK pipeline command and produces a
tested wheel/sdist candidate. After all six jobs pass, `platform-build` downloads each component archive once
through the selected transport, validates the component manifest, assembles the
release, and runs installation/help smoke checks in a temporary directory.
Optional OBS publication uses these local outputs directly. Buildkite still
retains final base/backend archives for downstream Full runs and manual downloads;
intermediate component archives use OBS by default. `build-manifest.json` binds
the commit, component manifests, base archive, backend bundle and tested SDK
copy. `admin-candidate.json` independently binds the same commit/build and Python artifacts.

The assembly step downloads the pinned external sandboxd backend artifact selected by
`ADX_BACKEND_ARTIFACT_BUILD` and verifies its revision, target and complete file
digests. `sdk-package` independently tests the public SDK on Python 3.12, creates
one wheel and one sdist, installs the wheel in a source-free virtual environment,
and publishes `sdk-candidate.json` with commit, version and SHA256 values.

The base archive includes the tested SDK wheel produced by its `sdk-package`
step. Full uses the explicitly selected SDK build candidate and installs only
the wheel named and hashed by the
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

For a bounded follow-up on one case, reuse an exact `ADX_E2E_ARTIFACT_BUILD`
and set `ADX_E2E_TARGET_CASE` to a case in the `full` profile. The case runner
still deploys and cleans up two physical workers but reports `profile: targeted`
with `source_profile` and `selected_case`. This result is diagnostic evidence,
not a Full gate pass. The normal pipeline leaves `ADX_E2E_TARGET_CASE` unset.

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
The build summary records the current Full pipeline checkout as `commit`, the
bundle's verified package commit as `product_commit`, and, when reusing images
from another build, that image build's checkout as `image_build_commit`. The
E2E report must identify the current harness and the same product commit as
the downloaded image bundle.

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

The base assembly job publishes final artifacts by default. Set
`ADX_OBS_UPLOAD=0` to disable this publication for a build.
The local release archive, package manifest, build manifest, admin candidate and
sandboxd backend bundle are verified before upload. No product binary is rebuilt.

Workers read `AK` and `SK` from the `obs-credentials` Secret into
`OBS_ACCESS_KEY_ID` and `OBS_SECRET_ACCESS_KEY`. The secret is optional at Pod
creation so Buildkite-only builds can run; selecting OBS without credentials fails
explicitly. Credentials are never passed on argv or written to artifacts.
`ADX_OBS_BUCKET` and `ADX_OBS_ENDPOINT` override the documented defaults.

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
The following `artifact-manifest` step validates the manifest's commit and
Buildkite build ID before generating `out/buildkite/index.html`. Its HTML is a
Buildkite artifact, with OBS links for each file and the manifest itself.

The base pipeline uploads the release archive, release manifest and verified
runc Runtime Pack. The SDK pipeline's `sdk-obs` step separately verifies and
uploads its wheel, sdist and `sdk-candidate.json`. The standalone SDK pipeline
still requires `ADX_OBS_UPLOAD=1`. Neither upload result is a Full deployment
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

The basic acceptance driver also runs an independent `local-first` case: restart API Server with `create_mode=local_first`, verify concurrent public SDK creation, real commands and deletion, require confirmed-claim evidence, then restore central mode. The default `k8s-basic` profile contains five bounded cases. The eleven-case `full` and local `standalone` profiles carry installed-SDK `data-plane`, `lifecycle` and fault/restart/stop coverage. [Buildkite #30](https://buildkite.com/agent-dx/agent-dx/builds/30) passed the previous eight-case OCI profile; the current profile split requires fresh formal evidence.

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

## Python dependency image

`ADX_PYTHON_IMAGE_SYNC_ONLY=1` selects the maintenance job that builds
`build/images/Dockerfile.python`. Its base image and SWR repository are recorded
in `build/images/python-environment.json`. The recipe downloads the pinned tools
and runtime dependencies from `python-requirements.txt` into an offline wheelhouse,
then verifies a clean, network-free installation before and after registry push.
The package and publish jobs use the verified digest recorded as `ci_image` in
that JSON. Update it and the pipeline pins together after rebuilding the image.

When the wheelhouse is present, `python-env.sh` checks its recipe against the
checkout and sets `PIP_NO_INDEX=1` and `PIP_FIND_LINKS`; a stale recipe fails with a
rebuild instruction. Package and install-smoke virtualenvs remain isolated.
The Docker runner mounts the same persistent pip directory that it passes as
`PIP_CACHE_DIR`; `PIP_INDEX_URL`, `PIP_EXTRA_INDEX_URL` and `PIP_DEFAULT_TIMEOUT`
are forwarded when configured, for runs using an image without a wheelhouse.
