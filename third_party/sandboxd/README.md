# sandboxd protocol

- Repository: https://github.com/inclusionAI/sandboxd
- Selected change: https://github.com/inclusionAI/sandboxd/pull/56
- Pinned head: `efc201531d7e2e9d69505da151eb66084b61eebf`
- ADX applies the tracked trusted-guest-metadata and Firecracker realtime-restore
  patches to this revision. On restore, sandboxd sends a fresh trusted host realtime
  timestamp to the guest PID 1, which sets CLOCK_REALTIME before it releases the
  checkpoint handoff. The restore-network-fence patch takes the target TAP down before
  loading a Firecracker snapshot and only activates it after the guest accepts its new
  network identity; failed handoff paths keep it isolated. The timestamp change requires
  a newly built initrd and fresh checkpoint; it does not make older snapshot artifacts
  compatible. Rootfs defaults, bootstrap mounts and managed-file paths are adapted by the
  control plane to the published sandboxd protocol.
- Source path: `api/runtime/v1/sandbox-api.proto`
- License: Apache-2.0; source and license copied verbatim.

The pinned protocol supplies `StartResponse.sandbox_ip` as field 4 and includes
the Checkpoint RPC. Adxlet generates its sandboxd client from this file.
Build and E2E inputs must use this same revision, rather than an independently
maintained control-plane copy of the backend API.

## CI binary bundle

[`artifacts.json`](artifacts.json) pins the OBS bundle for
`x86_64-unknown-linux-gnu`, using the same sandboxd revision as `source.json`.
The archive contains sandboxd, sbox, runc, runc-shim, sandbox-logger, redis-cli
and their backend manifest. It contains no ADX platform or Execd binaries.

`build/e2e/download_backend.py` checks the archive byte count and SHA256, then
validates the pinned sandboxd revision, declared target, complete file set,
per-file SHA256 and each executable's ELF machine. It rejects unsafe or duplicate
tar entries and installs the directory only after verification. Download or
verification failure stops package assembly.

The URL references the backend bundle published with ADX build #152; the ADX
commit in that artifact name identifies its publishing build, while the embedded
backend manifest identifies the sandboxd revision. Updating this dependency
requires a matching source pin and a verified replacement archive pin. ARM
package jobs currently exclude this x86-only external runtime bundle.
