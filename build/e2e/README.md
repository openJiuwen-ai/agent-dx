# Public SDK acceptance

Buildkite deploys to **Kubernetes** through [kubernetes/run.py](kubernetes/README.md).
It creates a dedicated namespace with two node Pods and Services, using immutable
images from the build step. Each Pod runs the platform as processes.

## Fixed-package acceptance without Buildkite scheduling

For an initial AFS component closure, use the fixed AFS component package and
the existing `afs/tests/run.py` inside one Linux VM as described below. This
requires neither Kubernetes nor Docker. It qualifies component behavior only.
For the complete ADX product path, reuse the Kubernetes driver on a qualified
target or the separately qualified native Linux Docker path. Product compilation
is not part of deployment; the drivers do not create VMs or a Kubernetes cluster.

Freeze the clean product commit, package/SDK/backend identities, image digests
and test-source commit before running. The reusable registry path only needs
the small matching bundle manifests and registry access, rather than another
download/build of the product. AFS ON additionally needs its hashed test-asset
sidecar, and AFS Full needs the matching frozen contract manifest and image.
Keep credentials outside the checkout and results.

The [fixed-package Kubernetes procedure](kubernetes/README.md#fixed-package-direct-run)
lists the inputs, environment checks, offline admission, direct component and
SDK cases, required smoke/Full commands, and evidence/cleanup rules. Use
`--artifact-commit` to enforce the fixed clean product outside CI.
`--verify-only` checks input manifests without deployment; its `inputs.json`
is not a functional or environment qualification result.

The available commands and unit contracts do not by themselves establish a
successful direct-package deployment. Record the actual candidate, target and
selected cases for every run. Kubernetes evidence does not qualify the Docker,
SSH multi-VM, soak or performance paths.

### Non-K8s SDK and required Full (PK1)

The local Docker adapter reuses the existing two-node deployment, public
scenarios, SDK-on-AFS assertions and AFS component runner. Its input is the
original fixed bundle/registry/AFS manifests plus a verified offline image
export, not a newly compiled product. The SDK remains the wheel installed in
the fixed node image. On 2026-11-01, fixed Base236 product commit
`f93e2fb04f423c7004532f38f1f1ca34de0ab782` and test-source commit
`e786bc440786102b0046ef03d1c5dd88c23525ee` passed targeted SDK Full and
one combined public11/component24/SDK16 Full on an x86_64 Linux 6.8/ext4 VM,
with normal cleanup. The combined run took 470.942 seconds. This qualifies
that single-VM Docker functional scope; other candidates and environments
require their own runs. The canonical [strategy](../../docs/testing/test-strategy.md)
records the initial environment failures and evidence boundary.

The image-export step uses `export_images.py` on an authenticated preparation
host. It pulls only the three immutable references from the matching bundle,
verifies their configuration IDs and architecture, and produces
`images.tar.gz` plus `image-export.json`. The latter binds the original bundle
and registry SHA, exact image IDs/references and derived archive size/SHA.
Keep original manifests unchanged: this archive is a separate transport, whose
bytes need not equal the original `prepare.py` archive. Deliver the export via
a trusted artifact channel and verify its recorded digest before use. Registry
credentials stay on the preparation host. The native runtime host does not
need SWR credentials, Kubernetes tools or cloud-cluster access.

Use an available native Linux Docker VM with the candidate architecture,
Python 3.10+, OpenSSL, tar, Git, cgroup v2, the existing sandboxd runtime
prerequisites and `/dev/fuse`. AFS state/output must be native ext4 with at
least 8 GiB free; budget additional space for the image archive, Docker layers
and temporary runtime archive. At least 4 CPUs/12 GiB available memory are
recommended for the sequential functional run; this is not a performance
qualification. Public nodes retain their 3 CPU/4 GiB limits. Use the provider's
reservation/run ownership before starting and its normal return procedure.

The VM administrator must make the kernel prerequisites available before
deployment: EROFS with a real loop mount, bridge netfilter with
`bridge-nf-call-iptables=1`, and working legacy IPv4/IPv6 filter tables for
the fixed sandboxd network ACL. A privileged container does not supply missing
host kernel modules. On the qualified Ubuntu 6.8 VM, the installed `erofs`,
`br_netfilter` and `ip6table_filter` modules needed explicit loading; no host
firewall rules or persistent sysctl configuration were changed. The runner
preflight checks EROFS/bridge prerequisites; sandboxd enforces its ACL
prerequisites at startup. Neither configures the host automatically.

From a clean frozen test-source checkout, supply one fixed product commit and
fresh output paths.

For an extracted Git archive without `.git`, first verify the archive SHA and
its externally recorded full test-source commit, then set
`ADX_E2E_HARNESS_COMMIT` to that commit. Keep this separate from
`--artifact-commit`, which identifies the fixed product. Do not set Buildkite
identity variables to pretend that a direct run is a CI job.

```sh
# Both OwnerFs and DFS, eight actual SDK assertions per mode.
python3 build/e2e/run.py \
  --bundle /absolute/fixed-bundle/bundle.json \
  --registry-images /absolute/fixed-bundle/registry-images.json \
  --image-export /absolute/offline-images/image-export.json \
  --artifact-commit REPLACE_WITH_PRODUCT_COMMIT \
  --profile standalone --case afs-sdk-full --cgroupns host \
  --output /absolute/new-sdk-result

# One required gate: public eleven groups, AFS ci-full24, SDK two modes16.
python3 build/e2e/afs_gate.py \
  --bundle /absolute/fixed-bundle/bundle.json \
  --registry-images /absolute/fixed-bundle/registry-images.json \
  --image-export /absolute/offline-images/image-export.json \
  --artifact-commit REPLACE_WITH_PRODUCT_COMMIT \
  --profile full --cgroupns host --output /absolute/new-full-result
```

Use `--case afs-component-full` with the first runner command for an explicitly
targeted component Full. It reuses the registered 24 cases and frozen Rust
test ELF already delivered in the fixed image; it never runs Cargo. No case
flag on the native `standalone` runner still selects only the public eleven
groups; the gate is required to include all three parts. A targeted SDK or
component pass is not a combined Full pass.

Every role is a test-owned privileged Docker container. The adapter freezes
container/network IDs and addresses before worker access; replacement or
ownership mismatch blocks reuse and deletion. AFS data binds only fresh
owned ext4 directories. SDK data roles explicitly share their own container's
mount namespace with sandboxd; other workers create private mount namespaces.
Component Full additionally exposes the existing BDI cache-probe directory to
its M role. These containers share one VM kernel/storage and do not establish
cross-host placement or failure isolation.

Require the parent and every child report, exact required case counts, matching
product/harness/file identities and JUnit; public Full retains its six stop
observability receipts. Require normal AFS process/mount cleanup, SDK
Environment/backend cleanup and `docker-cleanup.json` for owned containers,
network and cgroups. Removing a container cannot replace normal AFS cleanup.
Keep one original result/log/identity set and the frozen input; after successful
verification remove this run's derived runtime tar, native state and redundant
transfer copies. Do not prune shared Docker images or other tasks' caches.

### Fixed-package specials (PK2 minimum scope)

The explicit Docker cases `afs-component-specials`, `afs-sdk-restart` and
`afs-sdk-pressure` extend fixed-package functional diagnostics. They are not
part of default smoke or Full. On 2026-11-01, fixed Base236 product `f93e2fb0`
and test-source `ec144088aa82544e3b8fd5f85c93cc75145234cb` passed one complete
three-group campaign on the same Linux6.8/x86_64/ext4 Docker VM, in 240.975
seconds: five component cases, eight SDK restart assertions per mode, and five
SDK pressure assertions per mode, including normal cleanup. The initial failed
campaign and the stricter qualification are recorded separately in the canonical
strategy. Other candidates and environments require their own runs.

`afs-component-specials` reuses five existing AFS runner cases: OwnerFs/DFS
normal restart, DFS bounded-sync progress through the declared 64 MiB queue
profile, required-replica recovery, and failed-close retirement. It uses the
same Meta/Node binaries and normal process/mount cleanup as Full.

The SDK restart case keeps one real Environment on each AFS data role. An
SDK-launched background workload performs checked, synced file I/O while the
second Adxlet is paused; the fixture then resumes it, restarts both Adxlets
through the existing fault helper, and requires new process/session identities,
unchanged external runtime identities, further mounted I/O progress and full
SDK content verification. Explicit Environment deletion precedes AFS unmount.
The background command acknowledges startup before fault injection, uses only
the fixed image's existing Perl core support, and preserves command diagnostics
before deletion. After both Adxlets are ready, take a new progress baseline and
require another increment; earlier activity cannot stand in for post-ready I/O.

The SDK pressure case runs four workers with eight independent 64 KiB files
each, checks every byte through the opposite AFS role, checks `EEXIST` for
exclusive creation, and removes the files and Environments. This is a bounded
functional workload: it does not prove disk exhaustion, fairness, memory SLA,
throughput, crash durability or long-running reliability.

Reuse the existing targeted-suite scheduler through its public native entry:

```sh
python3 build/e2e/targeted_suite.py \
  --bundle /absolute/fixed-bundle/bundle.json \
  --registry-images /absolute/fixed-bundle/registry-images.json \
  --image-export /absolute/offline-images/image-export.json \
  --artifact-commit REPLACE_WITH_PRODUCT_COMMIT \
  --case afs-component-specials --case afs-sdk-restart --case afs-sdk-pressure \
  --budget-seconds 3600 --case-timeout-seconds 900 --cgroupns host \
  --output /absolute/new-special-result
```

Each case receives a fresh deployment and output. The suite verifies the
frozen harness, exact required assertions and explicit normal cleanup receipts;
a zero exit or container removal cannot replace them. A product case failure
may be followed by another independent case only after cleanup is confirmed;
the campaign remains failed. Timeout, missing identity or unconfirmed cleanup
stops the campaign. To reproduce just one group, use the same fixed inputs
with `build/e2e/run.py --profile standalone --case CASE` and a fresh output.

The legacy standard-suite runner retains its ARM64 and full-environment
guards. This native x86_64 campaign does not run pjdfstest/LTP/FSx/random
compatibility suites or claim their execution counts.

### Fixed AFS component package on one Linux VM

Provide a Linux VM whose native architecture matches the component, Python
3.10+, OpenSSL, root or `sudo -n`, a working `/dev/fuse`, util-linux/fuse3 tools
and a short dedicated native ext4 data directory. Follow the existing
[AFS environment prerequisites](../../afs/tests/README.md#运行条件).
This path runs Meta and multiple Node processes plus real FUSE/bind mounts in
one VM; it does not install the complete ADX stack or exercise its SDK.

Freeze the following separately: the product component's complete commit and
target, trusted archive SHA256/build ID, the clean test-source commit and file
digests, selected case IDs and environment configuration. A Buildkite Base
candidate publishes the complete AFS component as
`out/buildkite/components/afs.tar.gz`; it contains `afs-meta`, `afs-node` and
their component `manifest.json`. This is an optional delivery artifact distinct
from the complete `adx-release.tar.gz`. Use the manifest-bound AFS binaries
inside the full release instead when validating that delivery form, and label
the actual package scope in results.

Run from the frozen test-source root inside the VM. These variables are
operator-provided paths/identities, not shared CI defaults:

```sh
AFS_ARCHIVE=/absolute/path/to/afs.tar.gz
AFS_ARCHIVE_SHA256=REPLACE_WITH_TRUSTED_ARCHIVE_SHA256
AFS_PRODUCT_COMMIT=REPLACE_WITH_40_CHARACTER_PRODUCT_COMMIT
AFS_TARGET=x86_64-unknown-linux-gnu  # use aarch64-unknown-linux-gnu for ARM64
AFS_INPUT=/absolute/path/to/new-fixed-inputs
AFS_DATA=/var/tmp/afsp001            # new, owned, short, native ext4
AFS_RESULTS=/absolute/path/to/new-component-result
export AFS_INPUT AFS_DATA

printf '%s  %s\n' "$AFS_ARCHIVE_SHA256" "$AFS_ARCHIVE" | sha256sum -c -
mkdir "$AFS_INPUT"
mkdir "$AFS_INPUT/component" "$AFS_INPUT/bin"
tar -xzf "$AFS_ARCHIVE" -C "$AFS_INPUT/component"
python3 build/release/component.py verify --component afs \
  --directory "$AFS_INPUT/component" \
  --commit "$AFS_PRODUCT_COMMIT" --target "$AFS_TARGET"

# Adapt the verified component identity to the existing runner input contract.
python3 - <<'PY'
import json, os, subprocess
from pathlib import Path
root = Path(os.environ['AFS_INPUT']).resolve()
manifest = json.loads((root / 'component/manifest.json').read_text())
harness = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
identity = {'schema_version': 1, 'commit': manifest['commit'], 'target': manifest['target'],
            'binaries': manifest['files'], 'harness_commit': harness,
            'package_scope': 'verified AFS component package'}
for name in ('afs-meta', 'afs-node'):
    (root / 'bin' / name).symlink_to(root / 'component' / name)
(root / 'bin/artifact-identity.json').write_text(json.dumps(identity, indent=2) + '\n')
config = json.loads(Path('afs/tests/fixtures/singlevm.example.json').read_text())
config['hosts']['guest']['base'] = os.environ['AFS_DATA']
(root / 'singlevm.json').write_text(json.dumps(config, indent=2) + '\n')
PY

# Create only the new dedicated data directory, then qualify the selected scope.
sudo -n mkdir -m 0755 "$AFS_DATA"
python3 afs/tests/run.py check-env --env "$AFS_INPUT/singlevm.json" \
  --binary-dir "$AFS_INPUT/bin" \
  --case ownerfs.basic.read_write --case ownerfs.permissions.deny_other_uid \
  --case dfs.basic.namespace --case dfs.basic.one_writer_many_readers
python3 afs/tests/run.py run --env "$AFS_INPUT/singlevm.json" \
  --binary-dir "$AFS_INPUT/bin" --work-root "$AFS_RESULTS" \
  --case ownerfs.basic.read_write --case ownerfs.permissions.deny_other_uid \
  --case dfs.basic.namespace --case dfs.basic.one_writer_many_readers
```

The input/output directories and data base must be new and owned by this run.
Check extraction tools, archive member ownership and the trusted digest before
using a package; do not derive its trusted SHA from the just-downloaded bytes.
Do not change the sample UID/GID to a real user's identity merely to pass a
permission check. A managed environment may supply an equivalent private
mount/namespace transport, but its data must still be native ext4 and its
actual isolation/resource limits recorded. An AFS UDS path must fit Linux's
108-byte limit; a long management path is not a product failure.

Require all four selected cases to execute and pass with matching binary/source
identities, zero cleanup errors and normal process/unmount receipts. Preserve
`result.json`, per-case `identity.json`, `environment.json`, `checks.json`,
`waits.json`, `cleanup.json` and raw logs. `check-env` exit 2 means BLOCKED; READY/exit 0 is environment admission only,
not a functional pass. This selection excludes the Rust contract ELF, complete local
suite, ADX SDK/Full, cross-VM fault isolation, soak and performance. The existing
AFS runner can select wider suites when their inputs and environment qualify;
do not claim those scopes from this four-case run.

Before returning the VM, verify/export necessary results, normally stop/unmount
all owned processes and mounts, and remove verified redundant execution copies
and generated data. Retain one frozen package/identity and original failure
evidence. Follow the VM provider's normal close/release procedure; do not clean
other projects or shared caches.

## Activator deployment acceptance

The native Linux Docker driver supports complete Activator deployment acceptance in
both independent and embedded modes. Add `--activator` to the `prepare.py` command
above to include a digest-verified inherited HTTP workload. Use a newly built
package containing `bin/adx-activator`, API Server, Coordinator, adxlet, Execd and
Redis, plus the pinned sandboxd backend. Then run both modes:

```sh
make activator-e2e BUNDLE=/absolute/path/to/bundle OUT=/absolute/path/to/new-results
```

Set `E2E_CGROUPNS=host` when the Docker host requires the host cgroup namespace
(the same option as `run.py --cgroupns host`).

Each mode gets its own isolated two-node deployment and writes `result.json`,
`activator-result.json`, JUnit and component logs. Both public HTTPS Agent
entrypoints share real Redis. Independent mode starts one `adx-activator` process
per node and exercises both private control endpoints; embedded mode runs one
Activator inside each API Server. The tests publish a Template, reject foreign
traffic before side effects, send concurrent cold HTTP traffic, compare durable
AgentBinding and Platform identities, inspect the real sandboxd label/inventory,
run installed SDK commands and binary file operations through Execd, refresh the
binding cache, restart both Activators, delete and recreate the binding, and
require empty backend inventories and no remaining bindings/Environments.

The independent restart sends SIGTERM to the Activator; the embedded restart
restarts its API Server process through the supervisor. Stable binding generation,
Environment ID, runtime identity, backend inventory and workload boot ID must all
survive. These are logical two-node tests on one Docker host; they do not establish
cross-host or Kubernetes acceptance. Individual modes can be run with
`run.py --profile standalone --case activator-standalone` or
`--case activator-embedded`. They are additional targeted groups and do not replace
the eleven-group standalone profile.

## Installed example reproduction

[Installed example acceptance](example/README.md) runs the shipped complete
deployment configuration on a dedicated Linux KVM host, checks public SDK
operations and shutdown cleanup, and preserves configuration hashes.

## Local Docker reproduction

For local reproduction, `prepare.py` turns a verified ADX release package and pinned external backend
and an independently built SDK candidate into portable runtime images. `run.py` loads and verifies that bundle, starts
Redis, Coordinator/ShardScheduler, API Server with embedded Ingress and two adxlets with embedded Relays, and
independently starts real sandboxd on each node. Business tests use the public
SDK installed into the image from the independent wheel; no product source checkout
is mounted into the test nodes.

```sh
# Native Linux build stage; reuse an existing clean pinned checkout with --source.
python3 build/e2e/build_backend.py \
  --redis-cli /path/to/redis-cli --jobs 2 --output out/e2e/backend
python3 build/e2e/prepare.py \
  --package out/release/package --backend out/e2e/backend \
  --sdk-wheel out/sdk/adx_environment-0.1.0-py3-none-any.whl \
  --sdk-candidate out/sdk/sdk-candidate.json \
  --runtime-base registry.example/adx-e2e-tools@sha256:DIGEST \
  --execd-base ubuntu@sha256:DIGEST --output out/e2e/bundle

# Deployment stage on a native architecture matching the package.
python3 build/e2e/run.py --bundle out/e2e/bundle --output out/e2e/run-001
```

The Docker driver defaults to `--cgroupns private`. On a dedicated native Linux
host where sandboxd cannot enable cgroup v2 controllers in a private namespace
(`cgroup.subtree_control: device or resource busy`), use `--cgroupns host`.
Run the driver on the Docker host with permission to create and remove cgroups.
Resource observations still read each container's limits through
`/proc/self/cgroup`; they do not advertise the host's entire capacity. Each run
uses a unique sandboxd cgroup root per node. Cleanup removes those empty groups
after the test containers exit and reports failure if they remain occupied.

Use `--profile l0` for the minimum public API/SDK and authentication closure.
The default `--profile standalone` runs all eleven single-host logical two-node
groups. A group is a deployment and cleanup boundary, not one functional test.
The SDK, data-plane, lifecycle and placement groups emit stable functional
subcases, and JUnit reports those subcases individually. Both profiles write
`required_checks`, per-case results and case-level JUnit; a case that was not
reached is visible as skipped and prevents the JSON result from passing. The
public API inventory and uncovered conditional features are tracked in
[SDK E2E coverage](../../docs/testing/sdk-e2e-coverage.md).
For workload probes, add `--workload-probes` to `prepare.py`, then run
`run.py --profile standalone --case workload-probes` against that bundle.
Run it again with `ADX_E2E_RUNTIME_CONTROL=1` to exercise the Execd control
stream; the default exercises HTTP status polling. The managed Python workload
checks delayed HTTP readiness, TCP readiness, exec environment/working directory,
terminal readiness errors with resource cleanup, health failure thresholds,
bounded cold restart with a new backend, and idle deletion while probes run.
Stream mode also requires and saves Execd's successful control registration
from the host runtime log for each returned environment.
The result is a targeted case with seven functional subcases; it does not
replace the complete standalone profile. The fixture archive is hash-verified
with the release bundle, and business requests use its installed SDK wheel.
For a focused diagnostic, add `--case stop` (or another case from the selected
profile). The result is labeled `profile: targeted`, records `source_profile`
and `selected_case`, and cannot be cited as a complete profile pass.
The `lifecycle` group also starts a 120-second background command from a
separate SDK process, lets that client exit, and requires the six-second idle
policy to delete the instance before the command can finish naturally.

All output directories must be new. Local runs allow dirty packages and image
tags, recording their actual identities. Buildkite requires the current clean
commit and digest-pinned base images. The local and Kubernetes drivers share business scenarios and gate semantics;
[agent prerequisites and pipeline](../../.buildkite/README.md) describe setup.
`build/images/Dockerfile.e2e-runtime` supplies the tested Ubuntu runtime tools.
Local Docker reproduction requires access to the same bind-mounted paths as the Docker daemon. The Kubernetes deployer uses the supplied kubeconfig and registry references.

## Assertions

- `sdk`: Linux release packages first verify the configured EROFS or OCI runtime,
  runtime-only override and a plain custom image with the read-only EXECD bootstrap
  mount. The Kubernetes profile uses OCI; local process acceptance uses EROFS. Then
  two real instances across two nodes verify query, stdout/stderr/exit code,
  binary file round-trip, explicit deletion, Redis terminal state and released
  resources, and empty sandboxd inventories.
- `data-plane`: query both schedulable nodes through the installed SDK, create on
  a selected node, reattach by public instance ID (Environment ID), and exercise foreground/background
  commands, both handle and collection stdin/EOF, sync and async waits, stable
  command replay/conflict, typed not-found/timeout results, both kill entry
  points, filesystem text/binary/depth/directory copy, stateful Shell, interactive
  PTY input/EOF/resize/state, default TLS+Token forwarded-port traffic,
  authenticated Host-subdomain port forwarding, and a
  per-Environment TLS-only forwarded-port policy, and an SDK reverse-tunnel upstream
  round trip.
- `lifecycle`: close a detached handle, reattach to the same running Environment,
  delete it explicitly, verify ordinary `close()` preserves the remote Environment,
  verify context-manager deletion, then require an idle-timeout Environment to be
  reclaimed without a client-side delete.
- `auth`: invalid key and another tenant cannot read or delete the instance;
  the owner's instance remains running. An administrator creates, lists and
  revokes a tenant key through HTTPS Ingress; tenant management requests are denied,
  and revocation takes effect within the configured authentication cache budget.
- `capacity`: fill both nodes' advertised CPU capacity, verify another create
  waits, then release capacity and require that request to become executable.
- `placement`: use the public SDK on two nodes to verify environment affinity OR,
  instance anti-affinity, weighted and ordered node preferences, node ID
  constraints on every OR branch, and reverse instance anti-affinity. Require
  both nodes to report their live `runc`-only sandboxd runtime inventories;
  requesting `runsc` on node1 must expire in the central scheduling queue
  without an assignment or held resources. Verify actual assignments, execute
  a command and check physical cleanup. A separate heterogeneous-runtime E2E
  requires nodes with different sandboxd inventories.
- `local-first`: restart API Server with `create_mode: "local_first"`, verify
  entry-node rotation, concurrent same-name creation converging to one Environment,
  confirmed results reused with their original specifications, real EXECD commands, and physical cleanup.
  Require Coordinator local-claim logs, then restore the central deployment mode.
- `node-failure`: pin one running backend to each node, then suspend node2
  Adxlet heartbeats while its runtime remains independently hosted. Require
  persisted invalidation, resume the same process,
  require backend cleanup before readiness, and prove node1 remains executable.
- `sandboxd-restart`: pin one backend to each node, crash each independently
  hosted sandboxd daemon with `SIGKILL` and restart it. Require unchanged
  backend IDs, sandboxd `SERVING` health, restored node admission, and readable instance files,
  then delete the instances and verify resource release.
- `restart`: pin one backend to each node, terminate only Adxlet processes,
  wait for fresh node sessions
  and completed reconciliation, prove backend IDs are unchanged, then query and
  execute on the original instances.
- `redis-restart` (targeted fault case): pin one backend to each node, confirm
  AOF is enabled, crash supervised Redis with `SIGKILL`, and verify restarted
  Redis retains both persisted assignments and generations. Require unchanged
  backend IDs, readable instance files, executable commands and final release.
  Select it explicitly with `--profile full --case redis-restart`; it does not
  extend the default eleven-group Full gate.
- `redis-pod-restart` (Kubernetes-only targeted fault case): run AOF Redis in
  its own Pod backed by a PVC, keep one backend on each worker, replace the
  Redis Pod and verify the same PVC, assignments, generations and backend IDs.
  Public SDK file and command access must recover before final deletion. This
  uses a unique default dynamic StorageClass or `--redis-storage-class`; see
  `kubernetes/README.md`. The local Docker
  driver does not offer this case.
- `coordinator-restart` (targeted fault case): keep one live backend on each
  node while the supervised Coordinator is killed and restarted. Require a new
  Redis epoch, unchanged committed ownership and backend IDs, both nodes
  routable, and public SDK file/command access before final deletion. Select it
  with `--profile full --case coordinator-restart`.
- `apiserver-restart` and `ingress-restart` (targeted fault cases): restart each
  supervised public-entry role separately while two instances remain live.
  The Ingress case explicitly renders `ingress_mode: standalone`; the default
  fixture keeps Ingress embedded in API Server.
  Require an unchanged Coordinator epoch, persisted ownership and backend IDs,
  restored HTTPS API, public SDK file/command access and final resource release.
- `runtime-affinity` (targeted heterogeneous-runtime case): requires one live
  node to advertise `runsc` through sandboxd and the other live node not to.
  Create with the public SDK without specifying a node, then check the
  persisted assignment, runtime class, command execution and resource release.
  The default two-node fixture configures only `runc`. To build a heterogeneous
  bundle, pass `--runsc-bin /path/to/native/runsc` to `prepare.py`; its ELF
  architecture is checked against the release and its SHA256 is recorded in
  `bundle.json`. In Buildkite, set `ADX_E2E_RUNSC_BIN` and the independently
  checked `ADX_E2E_RUNSC_SHA256`, set an HTTPS `ADX_E2E_RUNSC_URL` and its
  pinned `ADX_E2E_RUNSC_SHA512`, or set an immutable
  `ADX_E2E_RUNSC_IMAGE=registry/repository@sha256:<digest>` and the binary's
  `ADX_E2E_RUNSC_SHA512`. The image path extracts `/runsc` from a regional OCI
  image, verifies the binary, and avoids downloading it from GitHub on each CI
  worker. Set exactly one of the three sources. Select
  `--profile full --case runtime-affinity`; the driver enables `runsc` only
  on node2 and requires live inventory, unpinned SDK creation, physical
  placement, command execution and cleanup. The OCI test image must support
  the selected `runsc` binary. This case remains unverified until it passes
  on an actual heterogeneous deployment.
  For a targeted Buildkite run with newly composed images, set the exact
  `ADX_BASE_PACKAGE_BUILD_ID`, `ADX_SDK_BUILD_ID` and
  `ADX_E2E_ARTIFACT_COMMIT` of the verified product alongside
  `ADX_E2E_TARGET_CASE=runtime-affinity`; the harness commit may differ from
  the product commit, and both identities are recorded.
- `mixed-soak` (targeted stability case): keep one public-SDK Environment active on
  each node while a third worker alternates create/command/file/delete between
  nodes for 300 seconds. Two concurrent workers exercise command and binary
  file round trips on the retained Environments. Require at least 40 commands and
  file round trips, five creations and deletions, zero operation errors, and
  physical backend cleanup on both nodes. Read back the authoritative node
  assignment for every created Environment and record verified counts per node.
  Record operation counts and p50/p95/p99/max latency in
  `mixed-soak-result.json`. Select it with
  `--profile full --case mixed-soak` or `--profile standalone --case mixed-soak`;
  it is excluded from the fast default gate.
- `idle-active` (targeted activity case): hold a foreground SDK command request
  open for 12 seconds with a six-second idle timeout. Verify that the instance
  and its allocation survive the active request, then close the client and
  require idle reclamation and resource release. Run with
  `--profile full --case idle-active`; it is outside the default basic gate.
- `relay-standalone` (targeted process-layout case): render a separate
  `adx-relay` service on both nodes and confirm each Relay has its own PID and
  a ready health endpoint. Then run the same public SDK command, file, port
  forwarding and reverse-tunnel checks as the embedded Relay data-plane case.
  Select it with `--profile full --case relay-standalone`; it requires a bundle
  containing `adx-relay`.
- `runtime-exit` (targeted sandboxd fault case): delete a real runc backend
  beneath a live SDK Environment. Verify that the default Never policy becomes
  Failed without a replacement, then verify a policy with two retries creates
  two distinct runtime identities and reaches Failed after the third loss.
  Check the persisted assignment, retry count, physical backend inventory,
  public SDK command access and final cleanup. Select with
  `--profile full --case runtime-exit`.
- `sandboxd-runtime-loss` (targeted combined daemon/backend fault): create one
  pinned Environment, pause adxlet briefly, remove the runtime through sandboxd,
  restart the sandboxd daemon, then resume adxlet. Repeat for the default Never
  policy and a two-attempt restart policy. Require Never to reach Failed with
  resources released, and the restart policy to run a new backend under the
  original assignment. Verify a public SDK command, final deletion and empty
  physical backend inventory. Select with
  `--profile full --case sandboxd-runtime-loss`.
- `coordinator-adxlet-restart` (targeted combined process fault): retain one
  Environment per node, then kill Coordinator and node2 adxlet concurrently under
  their supervisor. Require a new Coordinator epoch and adxlet session, unchanged
  Redis ownership and physical backend identities, restored public SDK command
  and file access, and final deletion. Select with
  `--profile full --case coordinator-adxlet-restart`.
- `sqlite-fallback` (targeted Coordinator outage case): enable the adxlet
  degradation journal only for this fixture, create a retained and an idle
  Environment, then suspend Coordinator while managed Redis remains available.
  Require the idle backend to be deleted with a durable SQLite pending record
  while Redis still has the old Running result. Resume Coordinator before the
  extended heartbeat deadline; require journal replay, Redis Deleted state,
  an empty pending table and the retained Environment serving a new SDK command
  with its original backend. Select with `--profile full --case sqlite-fallback`.
- `create-identities` (targeted concurrency/restart case): send twelve concurrent
  creates through independent HTTPS connections with one Request ID and different names
  and specifications. Require one confirmed Environment and one physical backend.
  A new Request ID with the winning Environment name must reuse its original
  specification. Restart Coordinator and API Server, then replay both
  request identities with new candidate names; require the same execution and
  a working SDK command before physical cleanup. Select with
  `--profile standalone --case create-identities`.
- `create-response-cut` (targeted lost-acknowledgment case): a TLS proxy reads
  the first successful create final event from the real Ingress, verifies the
  Redis assignment and physical sandboxd backend, then closes the downstream
  connection without delivering that event. The installed Environment SDK must
  retry with the same request ID and name; the final generation and backend
  must remain unchanged, and the instance must execute and delete normally.
  Select with `--profile full --case create-response-cut`.
- `create-unknown-query` (targeted transient-404 case): a TLS proxy drops the
  first create response before forwarding its write. The installed SDK keeps
  the original request ID and name while an independent public SDK lookup
  receives 404. The proxy then completes the original write and releases the
  retry; require one Redis assignment, one physical backend, a working command
  and full cleanup. Select with `--profile full --case create-unknown-query`.
- `command-response-cut` (targeted unknown command submission): after creating
  a real runc Environment, a TLS proxy forwards each `process.start` to Execd and
  discards its successful response. The installed SDK must return
  `CommandSubmissionError` with the stable command and request IDs. A new SDK
  client recovers the result by command ID; a marker written by the command
  must appear exactly once. Redis assignment, physical backend and final
  cleanup are checked. Select with `--profile full --case command-response-cut`.
- `command-watch-unavailable` (targeted command observation outage): a TLS proxy
  rejects only the command Watch handshake while normal HTTP queries remain
  available. The installed SDK starts one real background command and returns
  `CommandUnavailable` after its reconnect budget; a fresh healthy SDK client
  finds and terminates that same command. Redis generation, physical backend
  and final cleanup are checked. Select with
  `--profile full --case command-watch-unavailable`.
- `command-watch-query-unavailable` (targeted observation and query outage):
  after the real command starts, the TLS proxy rejects both Watch and
  `process.get`. The SDK retains the stable command identity while observation
  fails; a fresh healthy client finds and terminates the same command. Redis
  generation, physical backend and cleanup are checked. Select with
  `--profile full --case command-watch-query-unavailable`.
- `command-unsupported-feature` (targeted capability negotiation): a TLS proxy
  removes the Watch capability from Execd's real capability response. The
  installed SDK must return `UnsupportedFeature` before sending `process.start`.
  A fresh healthy client then starts the same command ID exactly once, and
  Redis assignment, backend and cleanup are checked. Select with
  `--profile full --case command-unsupported-feature`.
- `command-expiry` (expired-result contract):
  node1's Execd uses a one-second command-result TTL. The installed SDK must
  distinguish a never-seen command (`CommandNotFound`) from a completed command
  whose record expired (`CommandExpired`), then prove the Environment still runs and
  cleans up. Execd retains a bounded expired-ID tombstone and reports
  `COMMAND_EXPIRED` for this case. The cn-north-4 two-worker targeted run on
  2026-09-25 passed; select with `--profile full --case command-expiry`.
- `sqlite-node-restart` (targeted compound outage): after an idle deletion is
  durably pending in node1's SQLite journal while Coordinator is suspended,
  restart node1's Adxlet before resuming Coordinator. Require a new process
  with the same live backend and pending delete, then replay the journal and
  check the public SDK, Redis and physical cleanup. Select with
  `--profile full --case sqlite-node-restart`.
- `command-registry-capacity` (targeted Execd admission limit): only node1's
  Execd receives a one-record registry limit. Keep one background command
  running, require a second stable command ID to return `ResourceExhausted`,
  then terminate the holder and retry the rejected ID. Its side effect must
  occur exactly once; Redis assignment, backend and physical cleanup are
  checked. Select with `--profile full --case command-registry-capacity`.
- `upload-response-cut` (targeted resumable file transfer): a TLS proxy forwards
  the first binary upload chunk to Execd, waits for its committed offset, then
  discards the response. The installed SDK must query upload status and continue
  with the same upload ID from that offset. A fresh SDK client downloads the
  committed file and checks its SHA256; Redis assignment, physical backend and
  final cleanup are checked. Select with `--profile full --case upload-response-cut`.
- `download-response-cut` (targeted ranged download): an existing file is
  downloaded through a TLS proxy that returns only the first part of a 200
  response, then closes the connection. The installed SDK must preserve its
  `.part` file and issue a Range request for the remaining bytes. The completed
  SHA256, Redis assignment, physical backend and cleanup are checked. Select
  with `--profile full --case download-response-cut`.
- `schedule-deadline` (targeted center-queue timeout): require both workers to
  advertise only runc, request runsc on node1 with a three-second schedule
  timeout, observe the request in the admin queue, and require an outcome-unknown
  same-operation error after the queue deadline. The request must leave the
  queue without an assignment or held resources; both workers must have empty
  physical backend inventories. Select with `--profile full --case schedule-deadline`.
- `resource-stale` (targeted observation-expiry case): pause only node1's
  resource observer so its 10-second capacity sample expires while adxlet,
  Coordinator and existing backends continue. Require node1 to retain admission
  and its session after verified sample expiry. A node1-pinned SDK create must
  execute with the previous capacity while preserving existing allocations.
  Release it, resume observation, verify a fresh sample in the same session,
  create another instance, and physically clean up all backends. Select with `--profile full --case resource-stale`.
- `network-partition` (targeted two-worker fault): block node2's TCP traffic to
  Coordinator with a container-local firewall rule while keeping sandboxd and
  the Relay running. Require heartbeat invalidation, public Ingress route
  rejection for the failed instance, and continued SDK command and create on
  node1. Verify the firewall counter recorded blocked packets, remove the
  rule, then require node2 to clean its old backend before final deletion.
  Select with `--profile full --case network-partition`.
- `reconcile-crash` (targeted process fault): expire node2 while its sandboxd
  backend remains, then stop that backend's runc init so physical Delete waits
  after sending TERM. Kill Adxlet only after the pending signal proves cleanup
  began and the node remains closed to admission. Resume the init process;
  the replacement Adxlet must use a new session, remove the stale backend and
  reopen admission. Verify node1 still executes and delete both records.
  Select with `--profile full --case reconcile-crash`; this requires Linux
  `/proc` and the pinned runc backend, and is outside the default Full gate.
- `stop`: independently create a live instance pinned to each node, verify both
  backend inventories are occupied and exercise each forwarded port. A separate
  instance on each node is deleted to emit the required lifecycle log and trace. It also
  generates the Collector outage/recovery evidence required by its own metrics
  and log assertions. Then it stops each product supervisor, requires physical
  deletion, verifies independently hosted sandboxd still answers, and stops it.
  This case has no dependency on earlier SDK, data-plane or restart cases.

The runner produces `result.json`, `junit.xml`, package/image identity, SDK
results, node catalogs and component logs. Generated keys/certificates use a
private temporary directory outside uploaded evidence. Deployment failures and
cancellation still enter scoped cleanup. Original failures are retained;
cleanup failure or an unexecuted scenario prevents a pass. Images and bundles
are retained as build artifacts/cache; test containers and network are removed.

## Coverage boundary

The basic suite uses runc and no writable-layer quota. Most cases disable idle
reclamation; the dedicated `lifecycle` case enables a six-second timeout. It does
not validate pause/resume, snapshots, S3 rootfs/mounts, entrypoint inheritance,
failover, runtime network replacement, prolonged Coordinator outage, cross-node recovery, XPU,
mixed-load scheduling or performance. Those runtime-specific
contracts are assigned to the Firecracker profile. Resource observations read the node's cgroup
limits and filesystem with infrastructure reservations; this fixture is not
the production sandboxd collector.

sandboxd is locked to PR #56 through `third_party/sandboxd/source.json`. A
single-platform EXECD manifest is published within the test network (the pinned
backend's image-index lookup defaults to AMD64). OCI storage is a child of a
tmpfs mount to avoid nested OverlayFS depth and allow sandboxd to reset its
image directory. No host registry port or global Docker configuration is needed.

The SDK-only script remains available for an already provisioned deployment:

```sh
python build/e2e/sdk_smoke.py --endpoint 127.0.0.1:8443 \
  --token-file /run/adx-test/api-key --ca /run/adx-test/tls/ca.pem \
  --image registry.example/adx-execd@sha256:DIGEST --output out/e2e/sdk
```

Running this script alone does not establish the full acceptance result.

The formal three-machine inventory and completed-result contract is documented
under [multivm](multivm/README.md). The contract verifier does not provision VMs
or turn historical data-plane scripts into a control-plane acceptance result.

The capacity group also scrapes the live Coordinator and both Adxlet metrics endpoints. It checks two running instances and 4000 allocated CPU millis, one queued request while full, and zero instances/reservations after deletion. Per-node CPU, memory and disk allocation gauges must agree across Coordinator and Adxlet. Raw scrapes are retained as `metrics-allocated.json`, `metrics-queued.json` and `metrics-released.json`.

Component log-collection acceptance reuses the Ingress/Relay metrics endpoints
and receives structured component logs through a real OpenTelemetry Collector.
The stop group covers backend HTTP 503 responses, file rotation, and Collector
restart. It prints `[METRICS PASS]` / `[COLLECTION PASS]` and saves
`gateway-metrics-node*.json`, `collection-node*.json`, `collected-logs.jsonl`,
and `collector-process.log`. See the [log-collection contract](../../docs/testing/log-collection.md)
for deployment and guarantee boundaries. Trace acceptance prints `[TRACE PASS]`
and saves `traces-node*.json` and `collected-traces.jsonl`. Formal results are
recorded in [Buildkite #21](../../docs/testing/2026-09-17-observability-k8s.md).

## Early backend timeout regression

`backend_start_timeout.py` exercises a standalone public API with real Redis, API Server, Coordinator, adxlet and sandboxd. It requires an idle disposable Linux container labelled `adx.e2e.backend-timeout=true`, native runc and a reachable OCI image. The test creates a TLS handshake blackhole for image preparation and blocks one runc `run` per runtime case. Other runc operations use the original executable; a parent-death signal bounds the blocked process to its shim. The runner restores the executable checksum in `finally`; the owner must remove the disposable container even if the runner is interrupted.

```sh
python3 build/e2e/backend_start_timeout.py \
  --container "$CONTAINER" --api-url https://127.0.0.1:40446 \
  --token-file "$DATA/token" --image "$RUNC_IMAGE" \
  --budget 180 --evidence "$OUT/early-backend-timeouts"
```

Both image and runtime internal timers must fire at least ten seconds before the overall create budget. JSON and SSE must return `504 / DEADLINE_EXCEEDED / never / terminal` with the precise backend stage, and both original-request replay and authoritative failed-record replay must retain the exact message. The same real registry also returns missing-image, permission-denied, authentication-failure and rate-limit responses; JSON/SSE must retain their 404/403/401/429 categories and original message. Healthy create/delete calls before and after the faults verify runtime recovery. Evidence includes each response and an aggregate JSON verdict. This test injects faults only into the labelled disposable container.
