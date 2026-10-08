# sandboxd protocol

- Repository: https://github.com/inclusionAI/sandboxd
- Selected change: https://github.com/inclusionAI/sandboxd/pull/56
- Pinned head: `efc201531d7e2e9d69505da151eb66084b61eebf`
- ADX builds this revision without downstream source patches. Rootfs defaults,
  bootstrap mounts and managed-file paths are adapted by the control plane to
  the published sandboxd protocol.
- Source path: `api/runtime/v1/sandbox-api.proto`
- License: Apache-2.0; source and license copied verbatim.

The pinned protocol supplies `StartResponse.sandbox_ip` as field 4 and includes
the Checkpoint RPC. Node Manager generates its sandboxd client from this file.
Build and E2E inputs must use this same revision, rather than an independently
maintained control-plane copy of the backend API.
