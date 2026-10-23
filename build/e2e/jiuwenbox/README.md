# Jiuwenbox Linux runtime acceptance

## Accepted prebuilt artifacts

[prebuilt-lock.json](prebuilt-lock.json) pins the accepted Linux x86_64 runtime
set, its source URLs, archive/member SHA256 values, Conch SDK, packaged guest
kernel/initrd, StratoVirt and helper identity. The original archives and selected
packages are retained under `/root/adx-jbx-prebuilt-20261019/pinned/sha256/` on the
acceptance host; `cache_path` is relative to its `pinned/` directory. Verify SHA256
before installing a cached file. The upstream `last_successful_build` URL remains
a rolling channel and must match the lock before use.

The JiuwenSwarm wheel's version is `1`; Conch RPM is
`0.1.0-10.oe2403sp4`, with SDK `0.2.0`. The lock also records the VMM's reported
version because it differs from its RPM label. This set uses StratoVirt and the
RPM-provided guest kernel/initrd. It was extracted into an isolated Ubuntu 24.04
fixture, rather than installed globally through RPM scriptlets. Jiuwenbox and
Conch contents remain unchanged.

The fixture root supplies `backend-fixture.json` with `conch_backend` set to
`stratovirt` and `conch_template_selector` set to `id`; template creation resolves
the immutable content ID into ADX's `template_id` image binding. Runtime paths
under `conch/` and `jiuwenbox/src/` point to the extracted RPM and installed wheel.
The lock records the ADX package/source manifests, installed public SDK wheel,
independent Execd EROFS, and Conch CLI/daemon SHA256 values.

## Source-built fixture

This driver exercises ADX's production Jiuwenbox RuntimeDriver and Linux host
operations against separately installed, unmodified Jiuwenbox and Conch. It
requires a dedicated root Linux KVM host and reserves the existing backends'
fixed paths `/var/run/conch`, `/etc/conch/cni/net.d`, and `/root/.jiuwenbox`.
It refuses preexisting service/state/configuration unless a previous run under
this same artifact root owns it. Use a fresh `--run` name for each attempt.

The prepared artifact root must contain:

- `jiuwenbox/`: develop `9e0569f4fd047e510f4ee4ff28c26c54aaec5066`.
- `conch/`: master `a9a756d86e4e48305c4a19cc3a63bd53ae972659`, with
  `bin/conch`, `bin/conchd`, and `build-artifacts/conch-init-initramfs.cpio.gz` built.
- `venv/`: Python 3.11+ with both packages installed from these sources.
- `bin/cloud-hypervisor`: Conch's v52.0-conch build.
- `tools/bin/virtiofsd`: version 1.13.3; `tools/bin/mkfs.erofs`: version 1.9+.
- `artifacts/vmlinux`: a guest kernel with EROFS, overlayfs, virtiofs/FUSE,
  virtio-pmem, networking, and virtio-vsock built in; x86 kernels also need PVH.
- `artifacts/execd.erofs`: the static Execd plus BusyBox artifact built with
  `build/runtime/rootfs.py`, and its source binary at
  `target/x86_64-unknown-linux-musl/release/adx-execd`.
- `target/debug/examples/jiuwenbox_acceptance`: built with
  `cargo build --locked -p adxlet --example jiuwenbox_acceptance`.

Install bubblewrap, cgroup v2, loop/EROFS support, iproute2, iptables,
bridge netfilter, and bridge/host-local CNI plugins. Existing host routes must
not overlap the fixture's `10.83.0.0/24`. Template creation pulls Ubuntu 24.04;
the resulting immutable template ID is retained in `artifacts/template-ready-<kernel-digest-prefix>`.
Execd is supplied only by the separate EROFS mount.

```sh
sudo /absolute/artifact-root/venv/bin/python -u build/e2e/jiuwenbox/accept.py \
  --root /absolute/artifact-root --run run001
```

Each backend covers create/readiness, independent EROFS host mount, guest binary
checksum and read-only access, delete, driver process SIGKILL/reopen, Jiuwenbox
SIGKILL/restart, and a real create whose successful HTTP response is delayed
beyond the caller deadline. The latter verifies that delete waits for the
late create and Execd is not started afterward. Conch additionally covers
conchd SIGKILL/restart. Fault injection is in a test UDS proxy, with external
backend source unchanged.

Per-case `audit.json` checks native absence, backend processes, Execd identity,
supervisor cgroups/netns/veth, and the retained read-only Execd EROFS mount.
The fixture also rejects leftover ADX host-filter chains from older runs. After stopping services, `host-audit.json` additionally
checks Conch's pooled namespaces and bridge ports. Logs, failed attempts,
configuration, and `results.json` remain under `runs/<run>/`. Later attempts
run an explicit empty-authority reconciliation on the dedicated test backends before starting new cases.

This is runtime integration acceptance. It does not exercise the public SDK,
Coordinator scheduling, Gateway routing, complete adxlet process deployment,
multiple nodes, or checkpoint/restore. See the
[configuration and runtime contract](../../../docs/testing/jiuwenbox-runtime.md).

## Complete single-host deployment and public SDK

`full_sdk.py` reuses the real backend fixtures above and starts Redis,
Coordinator, adxlet with embedded Relay, API Server, and standalone Ingress
through `adxctl validate`, `render`, and `run`. Supply a coherent `package-sdk/`
assembled and verified by `build/release/package.py`,
including the pinned Redis 7.2.5 binaries, SDK wheel and independent Execd EROFS.
Install that wheel in `sdk-venv/` before running the fixture.

```sh
sudo /absolute/artifact-root/venv/bin/python -u build/e2e/jiuwenbox/full_sdk.py \
  --root /absolute/artifact-root --run sdk001
```

Use a fresh run name. The fixture reserves loopback ports 26379, 27000–27001,
27090–27092, 28444, 28888, 29444, 30443, and 30880–30881, in addition to the
backend paths above. It generates short-lived test certificates and private
API/Redis keys. Treat deployment YAML, rendered Redis configuration and keys
as private; exclude them from shared evidence bundles.

Business operations run in a separate Python process with `-I`, importing only
the installed `adx-environment` wheel. Both runtime classes cover create,
stdout/stderr/nonzero exit, binary and UTF-8 file round-trips, guest Execd
checksum, read-only bootstrap, and public deletion/absence. The fixture kills
individual adxlet, Coordinator, API Server and Ingress processes; `adxctl`
restarts them, and the same SDK handles must continue working. It also kills
Jiuwenbox and conchd, checking bounded cold restart with new execution IDs.

The test policy binds a fixture-owned host directory at `/workspace` for both
backends. Files in that volume must survive runtime replacement. This is
operator-provided storage configuration: the public SDK's `Mount` currently
supports read-only image/S3 sources, and cannot declare writable Local mounts.
The fixture does not assert that a disposable root filesystem survives.
Backend source code remains unchanged; only isolated policy/service configs
are generated. All business actions, including cleanup, use public SDK calls;
Redis and native backend reads supply independent lifecycle evidence.

`results.json`, per-restart records, `sdk-installation.json`, component logs,
`runtime/audit.json`, and `host-audit.json` remain under the run directory.
The fixture stops the ADX deployment and external backend processes after the
run. This remains single-host acceptance; it does not establish multi-node,
resource-isolation or checkpoint/restore support.


## Redis fault and host reboot acceptance

`redis_recovery.py --root <artifact-root> --run <new-name>` uses the same complete
package and public SDK to test Redis SIGKILL/AOF replay, a 35-second SIGSTOP
outage, and Coordinator restart after Redis recovery. It requires unchanged
Environment/runtime IDs, volume bytes and file hashes, followed by physical
resource cleanup. It does not delete Redis data or test loss of the AOF.

`host_recovery.py` has three explicit phases on a dedicated Linux host or VM:

```sh
<artifact-root>/venv/bin/python build/e2e/jiuwenbox/host_recovery.py prepare \
  --root <artifact-root> --run reboot001
# Reboot only that dedicated host, then reconnect to it.
<artifact-root>/venv/bin/python build/e2e/jiuwenbox/host_recovery.py check \
  --root <artifact-root> --run reboot001
<artifact-root>/venv/bin/python build/e2e/jiuwenbox/host_recovery.py cleanup \
  --root <artifact-root> --run reboot001
```

The prepare phase creates root-owned, uniquely named systemd units for ADX and
both external services, enables boot startup, creates Environments and writes
`host-state.json`. It detaches SDK clients without deletion. This fixture's
ADX unit uses SIGKILL on service interruption to retain desired state; explicit
`adxctl stop` still drains/deletes Environments. The recorded fault uses an
immediate forced OS reboot, so it does not run a graceful deployment drain.
Do not reboot a shared development host: the recorded run uses a dedicated
Linux VM with working nested KVM, hosting the actual Conch guest VM inside it.

The check phase requires a changed kernel boot ID, original Environment IDs and
assignment, new runtime IDs with one additional cold restart attempt, installed
SDK `Environment.from_id()` command/file access, preserved workspace hashes,
EROFS checks and physical deletion. On failure it retains service state and
`host-result.json` for diagnosis. The cleanup phase deletes test Environments
through public SDK calls, stops/disables/removes the recorded units, and audits
resources. No checkpoint/restore is used; this is not power-loss or disk-loss
acceptance.

The acceptance bridge now accepts an explicit `reconcile` action with a complete
`catalog` array. The fixture supplies this test-owned authority for driver recovery;
the production ADXlet obtains it from the Coordinator. Resource audits query native
inventory and expected test identities, not a local ownership database. Shared Execd
EROFS mounts and their loop devices remain installed after environment deletion and
are reported separately from orphan resources. Kernel/VM/public SDK acceptance must
be rerun for these semantics; earlier reports describe the previous implementation.


## Supervisor SDK smoke

`prepare_supervisor.py` installs the pinned original JiuwenSwarm wheel into a
fresh server venv. Its content-addressed cache verifies the source archive and
wheel by SHA256 and size. `smoke.py` verifies the ADX package, creates a private
fixture, installs its public SDK wheel and runs eight supervisor cases.

Use a dedicated privileged Linux x86_64 host or container with bubblewrap,
iproute2, iptables, busybox, util-linux, OpenSSL, cgroup v1/v2 and loop/EROFS
support. A container must expose the cgroup hierarchy and loop devices.
Prepare `/etc/pki`, which the pinned default policy references, before running.
The fixture checks existing host state before creating its ownership marker.

```sh
PIP_INDEX_URL=https://repo.huaweicloud.com/repository/pypi/simple/ \
PIP_CACHE_DIR=/absolute/pinned-cache/pip \
python3 build/e2e/jiuwenbox/prepare_supervisor.py \
  --cache /absolute/pinned-cache --output /absolute/new-backend-root
sudo env PIP_INDEX_URL=https://repo.huaweicloud.com/repository/pypi/simple/ \
  PIP_CACHE_DIR=/absolute/pinned-cache/pip \
  /absolute/new-backend-root/venv/bin/python -u build/e2e/jiuwenbox/smoke.py \
  --backend-root /absolute/new-backend-root \
  --package /absolute/current-release-package \
  --output /absolute/new-smoke-output \
  --commit <current-40-character-clean-commit>
```

`--commit` validates the expected clean package identity. Omit it for explicit
local testing of an uncommitted package; the result records that dirty identity.
The SDK runs in Python isolated mode and its files are checked against the wheel.

The eight required cases cover supervisor creation, exec, binary/UTF-8 files,
independent read-only EROFS, five process recovery scenarios, public deletion and
native audit. A pass requires unchanged Jiuwenbox files and successful fixture
teardown. EROFS remains mounted after Environment deletion until that teardown.
JUnit, summary and sanitized logs are written to `--output`; credentials,
rendered configs and venvs remain in the adjacent private directory on failure.

`full_sdk.py` defaults to the complete supervisor/Conch acceptance. Select
`--runtime supervisor` for the supervisor suite. The runtime, host recovery and
Redis recovery drivers retain their existing default scope.
