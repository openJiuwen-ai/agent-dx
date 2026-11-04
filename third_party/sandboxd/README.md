# sandboxd protocol

- Repository: https://github.com/inclusionAI/sandboxd
- Pinned head: `b5dab2958183faa8bcf04955eef7b0ae1c9b10ef`
- ADX builds this revision without downstream source patches. Rootfs defaults,
  bootstrap mounts and managed-file paths are adapted by the control plane to
  the published sandboxd protocol.
- Source path: `api/runtime/v1/sandbox-api.proto`
- License: Apache-2.0; source and license copied verbatim.

The pinned protocol supplies `StartResponse.sandbox_ip` as field 4 and the
Checkpoint RPC. Unknown Start outcomes converge through periodic inventory
and physical-ID Delete, including instances that appear after an empty scan. Adxlet generates its sandboxd client from this file.
Build and E2E inputs must use this same revision, rather than an independently
maintained control-plane copy of the backend API.

## CI binary bundle

[`artifacts.json`](artifacts.json) pins OBS bundles for
`x86_64-unknown-linux-gnu` and `aarch64-unknown-linux-gnu`, using the same sandboxd
revision as `source.json`.
The archive contains sandboxd, sbox, runc, runc-shim, sandbox-logger, redis-cli
and their backend manifest. It contains no ADX platform or Execd binaries.

The current AMD64 executables require glibc 2.34 or newer. Firecracker uses
`mount_setattr` for recursive virtiofs export protection, requiring Linux 5.12
or a kernel with that feature backported. An available `/dev/kvm` alone does
not establish host compatibility.

`build/e2e/download_backend.py` checks the archive byte count and SHA256, then
validates the pinned sandboxd revision, declared target, complete file set,
per-file SHA256 and each executable's ELF machine. It rejects unsafe or duplicate
tar entries and installs the directory only after verification. Download or
verification failure stops package assembly.

Both bundles are built from the clean pinned sandboxd revision using
`build/e2e/build_backend.py`. Updating this dependency requires matching source
and verified archive pins. To include the backend in the ADX release, build with
`ADX_WITH_SANDBOXD=1 make platform-release`. This adds the backend to
`runtime/sandboxd/`, includes gVisor runsc release 20260817.0,
and ships the systemd unit and example configuration described
in the [standalone deployment guide](../../docs/deployment/standalone.md#使用随包的-sandboxd).

For native cleanup qualification, point `ADX_SANDBOXD_TEST_SOCKET` at a dedicated,
empty backend and set `ADX_SANDBOXD_TEST_IMAGE` to a reachable image. Optional
`ADX_SANDBOXD_TEST_RUNTIME` selects runsc instead of runc;
`ADX_SANDBOXD_TEST_ROOTFS` selects a backend-local rootfs. Run
`cargo test -p adxlet --test sandboxd_main_reconciliation -- --ignored --nocapture`.
The fixture injects lost responses and late delivery through a gRPC proxy,
reconstructs the node/driver to lose caller state, verifies periodic reclamation
and preservation of an unrelated sandbox, then checks an empty backend.
