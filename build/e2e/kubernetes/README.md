# Kubernetes process-deployment acceptance

The Kubernetes driver treats product artifacts and acceptance code as separate
inputs. Image digests and packaged binaries come from the verified bundle. Once
the Pods are ready, the current Git commit's runtime harness is copied to
`/opt/adx/e2e` on every platform Pod and verified against `harness.json` before
preflight, sandboxd setup, or ADX startup. This also applies when the bundle is
reused from an earlier Buildkite build.

The Buildkite gate uses `run.py` here. `manifest.py` defines two ADX node Pods,
Services, volume/credential references and node placement. The targeted Redis
PVC case adds a third Pod. The ADX Pods run processes from the unified release.
`publish_images.py` runs on the build worker
and hands immutable node, EXECD and entrypoint-fixture registry references to the
deployment worker.

```sh
# Build stage, after build/e2e/prepare.py. Docker registry authentication is
# configured externally on the builder.
python3 build/e2e/kubernetes/publish_images.py \
  --bundle out/buildkite/bundle --repository registry.example/team/adx-e2e

# Deployment stage. Optional --registry-auth is a Docker config JSON file;
# optional --context explicitly selects a context from the supplied kubeconfig.
python3 build/e2e/kubernetes/run.py \
  --bundle out/buildkite/bundle/bundle.json \
  --registry-images out/buildkite/bundle/registry-images.json \
  --kubeconfig /path/to/target-kubeconfig \
  --output out/e2e/kubernetes-run-001
```

Profiles are explicit:

- `--profile l0` runs the minimum public SDK and authentication closure.
- `--profile k8s-basic` is the default and runs five bounded groups: `sdk`,
  `auth`, `capacity`, `placement`, and `local-first`.
- `--profile full` runs all eleven functional, lifecycle and fault groups and additionally requires
  the two platform Pods to be placed on distinct physical workers. Actual Pod
  to worker placement is checked after scheduling and retained as evidence.

## Target cluster prerequisites

The target cluster is separate from the Kubernetes cluster that hosts the
Buildkite Agent Stack. For `--profile full`, provide at least two schedulable
Linux workers. A self-managed cluster normally therefore has one control-plane
machine plus two workers; a managed Kubernetes service only needs the two
workers described here.

Each of the two ADX Pods contains the platform container and one OpenTelemetry
Collector sidecar:

| Resource per worker | Kubernetes request | Kubernetes limit | Recommended node capacity |
|---|---:|---:|---:|
| CPU | 2 CPU + 100m | 3 CPU + 1 CPU | 4 vCPU |
| Memory | 2 GiB + 128 MiB | 4 GiB + 256 MiB | 8 GiB |
| Image work directory | 1 GiB memory-backed `emptyDir` | 1 GiB | included in the 8 GiB recommendation |
| State, evidence and image cache | disk-backed `emptyDir` | no manifest limit | at least 30 GiB free disk |

Both workers must have these resources available at the same time. Pod
anti-affinity is preferred rather than required; `full` validates the actual
placement and fails if Kubernetes puts both Pods on one host. Repeated
`--node-name` values or `ADX_E2E_NODE_NAMES` restrict the eligible pool but do
not replace the final placement check.

### Host OS and kernel capabilities

Target workers must be Linux and all published artifacts must match the selected
`amd64` or `arm64` architecture. The repository's current formal Buildkite path
uses `amd64`. The container user space is Ubuntu-based, but the worker host does
not have to be Ubuntu; a maintained Ubuntu, openEuler, Rocky or equivalent Linux
distribution is acceptable when it supplies the required kernel capabilities.

The base OCI profile requires:

- privileged Pods running as root; the namespace and cluster admission policy
  must allow them;
- cgroup v1 or v2 CPU, memory and cpuset visibility;
- `br_netfilter` with
  `/proc/sys/net/bridge/bridge-nf-call-iptables` equal to `1`;
- working network namespaces, bridge networking, iptables and nested runc;
- cross-worker Pod networking, cluster DNS and Service routing;
- a container runtime that permits the above operations inside the privileged
  platform Pod.

There is no numeric kernel-version or Kubernetes-version rejection in the
current driver. The enforced gate checks capabilities. Use a maintained
Kubernetes release and a 5.10/5.15-or-newer LTS host kernel as the operational
baseline. An older kernel is not accepted merely because its version parses;
the preflight must still pass. The OCI profile does not need EROFS. Standalone
EROFS validation separately requires the filesystem driver, loop devices and a
successful real read-only mount.

### Network, registry and kubeconfig

The CNI must carry Pod traffic between the two physical workers. The fixture
publishes Services for ports `6379`, `17000`, `17001`, `18443` and `8443`, and
uses Pod IPs for Adxlet and Relay addresses. Network policy or host
firewall rules must allow this test-namespace traffic. Workers need registry
access to all digest-pinned Node, EXECD and Collector images.

The kubeconfig identity must be allowed to create, read and delete the isolated
Namespace and to manage Pods, Services and Secrets within it. It also needs Pod
exec/copy and log access, plus permission to read Pods, namespace state and
Events for evidence collection and cleanup. The Redis PVC case additionally
requires PVC create/read/delete access. A job that cannot confirm namespace
deletion does not pass.

### Conditional profiles

The Firecracker profile uses a separate Pod and is not part of base `full`:

| Firecracker worker requirement | Value |
|---|---|
| Worker count | one explicitly selected KVM-capable Linux worker |
| Pod resources | request and limit both `4 CPU / 6 GiB` |
| Host device | `/dev/kvm` mounted as a character-device `hostPath` |
| KVM contract | ioctl API version 12 |
| Other | privileged Pod and architecture-matched Firecracker kit, release and EXECD image |

If base `full` and Firecracker run concurrently, use two 4C/8G ordinary workers
plus one 8C/16G KVM worker. A smaller two-worker cluster is possible only when
one worker has enough spare capacity for both the base and 4C/6G FC Pods.

GPU/NPU acceptance is not currently executable through this manifest. It will
require device-capable workers, matching drivers/firmware, sandboxd device
discovery and Pod device exposure. Network partition tests require controlled
fault injection; neither they nor persistent-volume recovery are implied by a
passing base `full` result.

The targeted `redis-pod-restart` case needs a dynamically provisioned
`ReadWriteOnce` StorageClass, a 1 GiB PVC and permission to manage PVCs. It
uses the cluster's unique default dynamic class unless
`--redis-storage-class` explicitly selects one. Class discovery happens before
the test namespace is created and requires read access to StorageClasses. It
starts Redis in a separate Pod using the packaged `redis-server` and a
secret-mounted ACL. It creates live instances on both ADX workers, replaces
only the Redis Pod, then requires the Pod UID to change while the PVC UID,
committed ownership, backend IDs and public SDK file/command behavior remain
intact. Redis uses AOF with `appendfsync always`; namespace cleanup also
deletes the PVC. This case does not run in the default `full` profile.

```sh
python3 build/e2e/kubernetes/run.py \
  --bundle out/buildkite/bundle/bundle.json \
  --registry-images out/buildkite/bundle/registry-images.json \
  --kubeconfig /path/to/target-kubeconfig \
  --profile full --case redis-pod-restart \
  --redis-storage-class fast-rwo \
  --output out/e2e/redis-pod-restart-001
```

In Buildkite, set `ADX_E2E_TARGET_CASE=redis-pod-restart` and optionally
`ADX_E2E_REDIS_STORAGE_CLASS` to override the cluster default, alongside the exact reused
artifact build and commit required for every targeted case. The case is not
verified on a real cluster until its JUnit result, Pod/PVC identities, Redis
AOF evidence and two-worker backend inventory are retained.

The optional `--profile full --case mixed-soak` runs a five-minute public SDK
load on both workers and records per-operation counts, latency percentiles,
errors and final physical cleanup. It does not run in the default Full gate.

Every invocation requires a new evidence directory and creates a unique test
namespace. Target images must be digest pinned. Registry TLS verification is
on; private registry credentials are mounted read-only and supplied to both
Kubernetes and sandboxd. No cluster selection or credential material is embedded
in the source tree.

Ten scenario groups are shared with the local driver; the selected profile
controls which groups are required. Kubernetes-specific
contract tests cover manifests, Secret projection, Service addresses, immutable
artifact handoff and namespace ownership/cleanup failures. These tests do not
execute a Kubernetes cluster. Pod readiness alone does not pass the platform
readiness gate, and deleting a namespace alone cannot satisfy the physical
instance-cleanup scenario.

State and artifact cache are ephemeral for this acceptance deployment. Redis
still writes AOF to its Pod-local state volume; persistence across Pod deletion
is outside these scenarios. Kubernetes requests/limits bound the node container;
the fixture reads its cgroup limits before advertising ADX capacity.

See [Buildkite setup](../../../.buildkite/README.md) for the reused CI workers, target kubeconfig mount and SWR secrets.

The Kubernetes profile uses the digest-pinned OCI runtime image, so the target
worker pool does not need EROFS loop-mount support. It must provide bridge
netfilter with `bridge-nf-call-iptables=1`; the fixture checks this before starting
sandboxd. EROFS deployments use a separate real mount preflight. Select eligible nodes with repeated `--node-name` arguments, or set
`ADX_E2E_NODE_NAMES` to comma-separated Kubernetes node names in Buildkite.
The scheduler still applies the Linux/architecture and resource requirements.
A one-node pool runs two isolated Pods on one host; it does not validate
cross-host networking. Actual host placement is retained in the acceptance logs.
Such a run can pass `k8s-basic`; it cannot pass `full`.

The E2E job log streams deployment commands and their output as they run, with
10-second progress notices for long waits. It shows resource creation, Pod/image
readiness, node prerequisites, sandboxd readiness, ADX registration/routes and
cleanup. Structured Kubernetes JSON is retained in numbered artifact logs;
readable Pod placement is printed in the job log. Secret payloads remain on stdin
and known credential values are redacted before writing or streaming output.

Each scenario emits RUN, PASS/FAIL and elapsed time. Its child-process output is
streamed without buffering, including SDK instance IDs, command/file assertions,
authentication checks, capacity wait/resume, placement rules with expected/actual
node assignments, and restart recovery checks.
`case-results.json` retains per-case outcomes and duration; JUnit lists every
scenario required by the selected profile, with unexecuted required scenarios
marked skipped and cleanup reported independently. A failed command or timeout
still fails the acceptance.

The `local-first` case switches only API Server into local-first mode, checks SDK concurrent create/execute/delete and confirmed local-claim evidence, then restores central mode. This case requires newly built artifacts; earlier seven-case runs do not validate it. [Buildkite #30](../../../docs/testing/2026-09-18-runtime-environment-k8s.md) passed the previous eight-case OCI profile. The current default basic profile is the bounded five-case gate; `data-plane`, `lifecycle`, `node-failure`, `sandboxd-restart`, `restart`, and `stop` run under `full` and local `standalone`.
