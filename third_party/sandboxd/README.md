# sandboxd protocol

- Repository: https://github.com/inclusionAI/sandboxd
- Selected change: https://github.com/inclusionAI/sandboxd/pull/56
- Pinned head: `efc201531d7e2e9d69505da151eb66084b61eebf`
- Downstream patches are content-addressed in `source.json` and applied only by
  the external backend build. The current patch maps the public S3 contract to
  Nydus' S3 backend instead of the incompatible OSS backend. It also mounts
  Firecracker guest runtime filesystems before injecting managed files, so
  `/run/adx/image-process.json` remains visible for OCI entrypoint inheritance.
  The backend build therefore exports both the host binaries and the patched
  Firecracker guest `initrd.img`; the runtime-kit verifier binds both hashes.
- Source path: `api/runtime/v1/sandbox-api.proto`
- License: Apache-2.0; source and license copied verbatim.

The pinned protocol supplies `StartResponse.sandbox_ip` as field 4 and includes
the Checkpoint RPC. Node Manager generates its sandboxd client from this file.
Build and E2E inputs must use this same revision, rather than an independently
maintained control-plane copy of the backend API.
