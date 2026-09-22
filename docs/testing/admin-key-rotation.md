# Administrator key rotation

The deployment generates and persists the initial key. Master reads `key_file`
at startup and atomically reconciles the complete configured administrator set
in Redis. Removed administrator keys receive persistent revocation tombstones.
Tenant keys are preserved. Duplicate, expired, revoked, empty or conflicting
administrator sets fail before changing active keys. At least one valid
administrator key is required. Missing/unreadable key files abort startup.

Existing ingress authentication caches expire normally; rotation does not
terminate existing streams. See the [deployment contract](../deployment/adxctl.md#管理员-key-的生成读取与轮换)
for retrieval, restart and staged-transition behavior.

## Verification on 2026-09-22

- RED: the new storage tests failed to compile before `reconcile_administrators`
  existed. Logs: `out/ci/admin-key/red.log`.
- Master normal suite: 53 passed, 50 ignored.
- Linux real Redis storage suite: 26 passed, including replacement, idempotent
  retries, tenant preservation, revoked-key reuse rejection,
  fencing of old Master sessions, invalid sets and multiple administrators.
- Linux RPC suite: 20 passed. The Master process test starts with a key, restarts
  with the same key, replaces the file and restarts again; the new key succeeds
  and the old key returns `Unauthenticated`.
- Optional API Server subprocess branches of the RPC suite were not run
  (`ADX_TEST_API_SERVER` unset); full public-path evidence is recorded below.
- Workspace Clippy with all targets/features and `-D warnings`: passed.
- Logs: `out/ci/admin-key/master.log`, `linux.log`, `clippy.log`.

These tests use isolated Redis instances. Process restarts retain Redis state;
this round does not establish Redis crash/AOF recovery behavior for rotation.

## Public-path standalone verification

AKernel used a new data directory with deployment-generated keys. gVisor and
Firecracker each passed six SDK integration cases, with one custom OCI case
skipped. After atomically replacing `key_file` and restarting services, the
public resources API returned HTTP 200 for the new key and HTTP 401 for the old
key. `data/token` exposed the new key, and a second restart preserved the result.

- Image: `akernel-adx-validation:admin-key`.
- Image ID: `sha256:5f151ac9ed048174972e3025ecd75dc8e6e20e54b564d983079aaf8a2b4522ae`.
- Remote log: `/var/log/akernel-admin-key-e2e.log`.

This was an overlay of locally built binaries and updated startup scripts. It
was not a newly published release. Kubernetes and cache expiry while keeping
ingress running were not exercised by this test.
