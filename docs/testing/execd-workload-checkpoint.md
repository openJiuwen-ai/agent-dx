# Workload checkpoint validation

`POST /checkpoint` on the Execd Unix socket creates a local recovery point while
keeping the original runtime alive. It is independent of the reusable Snapshot
API and the stopping pause operation.

Focused regression commands:

```sh
cargo test -p adx-execd --test control --test control_http
cargo test -p adxlet --test pause_resume --test sandboxd_rpc
```

The Execd tests use a real process, TCP control requests, Unix socket requests and
a FIFO backend handoff fixture. They cover concurrent request rejection, waiting
for durable acknowledgement after handoff, stale/repeated acknowledgements,
error replies and listener rearming after restore. Controller tests cover pending
request retirement when execution identity changes.

adxlet tests cover `leave_running=true`, stable execution/route/allocation,
local artifact registration, commit retry without repeated capture, failure
without false success, restart cleanup of an uncommitted capture, and use of the
new point by failover.

## Verified evidence (2026-09-22)

- Execd control/HTTP: 12 passed; adxlet pause/resume and sandboxd RPC: 46 passed.
- Workspace Clippy (all targets/features, warnings denied), formatting and docs checks passed.
- AKernel SDK: 253 passed; Ruff, mypy and deployment-script checks passed.
- Linux x86_64 standalone with AKernel's gVisor backend: 6 integration cases passed,
  1 OCI case skipped because no custom image was specified.
- `test_internal_checkpoint_reload_and_reverse_tunnel` exercised the actual Unix
  request, live-source continuation, reload, checkpoint-era file contents and
  restored reverse-tunnel traffic. Task containers were removed afterward.

The validation image was `akernel-adx-validation:execd-checkpoint`, ID
`sha256:6a9dee3bf0e3c7e74f655811ea8ebda0f2eadfab216ea1ae9c064264ad171434`.
It layered this change's debug-built, stripped adxlet and Execd over the
AKernel validation image based on Buildkite #71. AKernel's existing EROFS rootfs
was repacked with the new Execd; sandboxd was unchanged. This is not a released
ADX package, and AKernel's checked-in #71 artifact pin still needs a later release
update. The first run used gVisor; the Firecracker follow-up below used the same
ADX binaries. Kubernetes was not run.

Logs are kept under `out/ci/execd-checkpoint/`; the real-run log is
`akernel-e2e.log`. The remote original is
`/var/log/akernel-execd-checkpoint-e2e-final-20260922.log`.


## Firecracker follow-up (2026-09-22)

The AKernel integration suite also passed with `AKERNEL_TEST_RUNTIME=firecracker`:
6 passed, 1 custom OCI/Nydus case skipped, zero failures/errors, 73.973 seconds.
It exercised workload Unix checkpoint, source continuation, reload, restored file
contents and reverse-tunnel reconnection, plus command/filesystem/PTY operations.

Environment: Linux x86_64 `6.8.0-137-generic`, KVM API 12, Firecracker bundle
`v1.16.1-akernel.3`, guest kernel `6.1.177`, virtiofsd `1.14.0`.
The optional FC binaries/kernel were added from an existing AKernel image after
checking the pinned manifest hashes; the guest initrd was built from sandboxd
`7d2af7f52eeaae5203fc9f4fe3dadffc357eac3c`. The daemon and ADX binaries were unchanged.
Image: `akernel-adx-validation:execd-checkpoint-fc`, ID
`sha256:e5e653a06555cac24d490550b8aad770df3b1d7ba1984a26b7da818f5b9243d9`.

Remote log: `/var/log/akernel-adx-fc-e2e-20260922.log`;
local copy: `out/ci/execd-checkpoint/firecracker-e2e.log`.
The task container was removed. This is standalone validation of a test image;
custom OCI, cross-node cloning and Kubernetes are outside this run's evidence.
