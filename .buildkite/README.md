# ADX Kubernetes E2E pipeline

`.buildkite/pipeline.yml` builds the current revision, publishes its node and RRT
images, then deploys and tests ADX in the target Kubernetes cluster. The E2E step
runs as a Buildkite Agent Stack for Kubernetes job. It uses a mounted target
kubeconfig to create a unique `adx-e2e-*` namespace and deletes that namespace
when the run finishes.

## Deployment

| Resource in the test namespace | Processes |
|---|---|
| `node1` Pod | Redis, Master with embedded Domain, Sandbox API, Edge, Node Manager, Node Proxy, independently hosted sandboxd |
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
| `platform-build` | `swr.cn-southwest-2.myhuaweicloud.com/yuanrong-dev/compile-ubuntu2004-rust:v20260507_x86_64` |
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
worker images does not change the product's toolchain or dependency pins. These
inputs must be available in the CI job; the worker profiles have not been live
validated for this ADX build yet.

## Artifact handoff and acceptance

`platform-build` constructs all ADX binaries, the SDK and pinned sandboxd helpers
from the clean current commit. `platform-images` downloads these artifacts,
creates the verified bundle and publishes the node/RRT images. `registry-images.json` records immutable digest
references, source image IDs and the checksum of `bundle.json`.

`platform-e2e` downloads only those JSON manifests and invokes
`build/e2e/kubernetes/run.py`. It verifies their relationship, commit, architecture
and immutable references. The target cluster pulls the build's images; neither
the deployer nor test Pods compile or substitute product binaries.

Mandatory scenarios are SDK create/query/command/file/delete, invalid key and
tenant isolation, capacity exhaustion/release, Node Manager process restart and
supervisor stop with physical runtime cleanup. Missing scenarios, diagnostic or
cleanup failures prevent a pass. `result.json`, JUnit, Kubernetes resource/events
and per-Pod logs are uploaded. Secret bodies travel on stdin and are excluded
from manifests and evidence; generated API/Redis keys are redacted from collected
component logs.

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

构建会在复用的 worker 中按版本和 SHA256 准备 Go 1.25.5、Redis 7.2.5，并安装固定版本的 Go 协议生成器。未指定基础镜像时，打包步骤按仓库 Dockerfile 构建并发布测试基础镜像，再用 registry digest 构建节点与 RRT 镜像。`ADX_REDIS_SERVER` / `ADX_REDIS_CLI` 和 `ADX_E2E_RUNTIME_BASE` / `ADX_E2E_RRT_BASE` 可显式覆盖。
