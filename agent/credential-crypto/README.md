# Workspace credential encryption

`adx-credential-crypto` implements the configuration center's credential encryption module. It provides an internal HTTPS executable and reusable Axum router outside user images, as its own ADX Template/Binding. Deployment requires publishing the service template and configuring the Gateway entrypoint. The release executable is used to build the service image and for trusted administration; it is not installed as a host service. DigitalMate bot-agent owns provider OAuth, refresh and calls, stores ciphertext in its existing workspace, and delegates task execution to WorkSwarm. The credential client remains in DigitalMate, outside the execution backend.

## Existing ADX authority, minimal new authorization

User/workspace/Environment ownership stays in the existing `AgentBinding`. Only Activator reads that authoritative metadata. Crypto never reads Binding Redis records directly and does not maintain another user/workspace directory.

The `service-grant` record uses the existing `adx-agent-store` Redis repository, namespace, conditional writes and reconnect/timeout handling. It contains only the credential digest, Binding `scope` reference, `generation`, fixed audience `credential-crypto`, purpose `orbit-config` and expiry. The workspace is derived on every verification from the existing Binding's `parameters.workspace`; it is not copied into the grant record. The raw credential is returned once to trusted provisioning and stored only in the workspace's private read-only identity file.

Issue checks the current Binding revision/generation atomically with the new grant. Every verification performs point reads of the grant and its Binding; no full-table/file scan or positive authorization cache exists. Expiry, explicit revocation, Binding deletion intent, removal, generation replacement, wrong tenant/workspace/purpose all deny access. Requests already admitted may finish during a concurrent revocation. A paused Environment keeps the same Binding generation; template/version replacement requires a newly issued grant referencing that Binding, while the stable workspace context can still open its existing ciphertext.

The existing private control credential authorizes issuance/revocation. A **separate verify-only service credential** authorizes the crypto-to-Activator verification call. It cannot issue grants, delete bindings or access existing control routes. Neither credential is a platform tenant API key. Tenant-level keys are not handed to DigitalMate. This reuses ADX's control authentication and storage mechanisms; it does not extend the Coordinator's tenant-key privileges.

## Encryption boundary

- Fresh random 256-bit DEK on every seal, AES-256-GCM payload and AES-256-GCM wrapping under a dedicated server-side KEK. KEK/plaintext DEK never enter DigitalMate.
- Context is `{tenant, workspace, purpose}`, 1–128 ASCII letters/digits/underscore/hyphen per field. The request context must match what Activator derives; caller-supplied IDs alone grant no access.
- Crypto persists only its dedicated KEK keyring and deployment configuration. ADX Redis stores authorization metadata, not KEKs, DEKs or provider credentials. Provider ciphertext remains in the user's workspace.
- Envelope: `version=1`, `algorithm=AES-256-GCM`, `key_backend=local`, `key_id`, `context`, `wrapped_key`, `ciphertext`. Encrypted binary fields are Base64 of nonce(12) + ciphertext + tag(16).
- AAD is the JSON string array `[layer,"1",tenant,workspace,purpose]`; the wrapping layer appends key_id. Layers are `adx.credentials.data` and `adx.credentials.dek`. Rewrap preserves data ciphertext; changing workspace context requires decrypt/re-encrypt, not a Redis field edit.
- KEK is independent of `ADX_CREDENTIAL_KEY_FILE`. KMS is future work, not an implemented backend. No request bodies, keys or bearer credentials are logged.

## Interfaces

| Interface | Authentication | Behavior |
| --- | --- | --- |
| Crypto `POST /v1/seal` | Workspace credential | `{context, plaintext:BASE64}` → `{envelope}` |
| Crypto `POST /v1/open` | Workspace credential | `{context, envelope}` → `{plaintext:BASE64}` |
| Crypto `GET /healthz` | none | 204 process liveness; not authority/Redis readiness |
| Activator `POST /internal/adx/v1/service-grants/issue` | Existing private control credential | Scope + expected generation + purpose + TTL → credential once, digest, derived context and expiry |
| Activator `POST /internal/adx/v1/service-grants/verify` | Separate verify-only credential | Credential + expected context → `{allowed}`; resolves current Binding |
| Activator `POST /internal/adx/v1/service-grants/revoke` | Existing private control credential | Digest → idempotent revocation |

Crypto errors: 401 missing bearer, 403 denied workspace grant, 400 invalid payload/envelope, 503 `AUTHORITY_UNAVAILABLE` or `CRYPTO_UNAVAILABLE`. A failed verifier credential or unavailable Redis/Activator is a service dependency error, not a missing Feishu connection. There is no plaintext/file-policy fallback. Mobile actions and DigitalMate's seal/open wire format are unchanged.

Crypto always serves TLS. Client-to-authority requests use a pooled client, no redirects/proxy discovery, a 3-second default timeout and a 16 KiB response cap. HTTPS is the default; explicit `allow_loopback_http=true` permits HTTP only to loopback for local fixtures. Public grant management routes must not be exposed to user environments.

Crypto bounds: plaintext 128 KiB, body 512 KiB, 128 simultaneous TCP connections, 10-second TLS handshake and 30-second total connection lifetime. Its application-facing HTTP keepalive remains disabled; connection reuse on that client path and production capacity validation remain separate work. Overload closes excess incoming connections. Grant endpoint bodies are bounded to 16 KiB.

## Provisioning and deployment

ADX-managed hosting uses the [service template](../activator/examples/credential-crypto-template.json) and [image](../../build/images/Dockerfile.credential-crypto). It inherits the serve command as UID/GID 10002, mounts protected server material read-only at `/run/adx-credential-crypto`, and disables idle reclamation. Preprovision identical keyrings on eligible Workers; missing materials fail rather than initialize. This private directory never enters a DigitalMate user mount. Template-publishing, execution and host administrators remain trusted; hosting does not provide hardware-backed key protection.

The `ADX_CREDENTIAL_CRYPTO_CONFIG` [Gateway settings](../../build/config/examples/credential-crypto-gateway.json) require the existing `ADX_AGENT_CONFIG`. TLS `POST /v1/seal` and `/v1/open` verify each workspace grant before resolving one operator-selected Binding and connecting through Gateway/Relay with backend TLS name/CA verification. Crypto verifies the grant again. Gateway reuses the existing ActivatorClient/LocalControl for fresh verification; the independent Crypto service keeps its verify-only identity. Gateway remains a trusted control-plane component. Gateway holds no KEK; DigitalMate receives no tenant API key. Request/response limits are 512 KiB each, admission concurrency 128, total deadline 1–300 seconds (default 60). HTTP exchanges use the existing backend pool and its endpoint limit (default 64). Crypto uses one-shot connections to keep transport identities separate; verification retains a 3-second cap and 16 KiB reply bound. Queries, upgrades and ambiguous Authorization are rejected; client routing headers are stripped. No business request is replayed after TLS/writes. Unconfigured Gateways retain existing dispatch. The [deployment guide](../../docs/deployment/credential-crypto.md#adx-模板托管) covers publication, activation and distinct frontend/backend CA files. Existing instances are not migrated automatically.

Build with `cargo build --locked -p adx-credential-crypto -p adx-activator --bins`. Use the existing ADX Redis instance and Agent namespace; it must have appropriate durability, memory capacity and eviction policy for authoritative metadata. Redis loss/eviction denies access until trusted recovery; do not silently restore stale revoked grants. Restrict its existing ACL/network access. Crypto itself needs no Redis credentials.

Enable `credential_grants_token_env` in [Activator configuration](../activator/examples/credential-grants.json). Its value names an environment variable containing the **verify-only** secret. It must differ from `ADX_ACTIVATOR_SERVICE_TOKEN`. The optional router is disabled when this setting is absent. All selected Activator replicas must expose this route consistently. An embedded deployment must mount `adx_activator::service_grants::router` using its existing Activator instance and equivalent private transport; Gateway embedded-mode assembly is not automatically modified here.

The [server configuration](../../build/config/examples/credential-crypto.json) listens inside the managed Environment and reads protected files from `/run/adx-credential-crypto`. Its `authorization` object points to the private Activator verifier, not a client-facing endpoint. Prepare the corresponding files under the template's host source `/mnt/private/adx-credential-crypto`, owned/readable by UID/GID 10002, with directory 0700 and files 0400/0600. Mount this directory only into the service. The image's inherited `serve` command is the runtime entrypoint; publishing the template does not initialize keys or launch a host daemon.

Initialize the protected keyring once, before publishing the service template:

```sh
adx-credential-crypto init-keyring --output /mnt/private/adx-credential-crypto/connection-keyring.json
```

Use Activator's `credential_delivery` for managed workspace identity issuance and renewal. Issuance/revocation remain in the existing private Activator control API, including trusted manual provisioning and emergency revocation; the crypto executable does not duplicate these workflows. Treat an issue response's one-time credential as a secret, never log it, and do not blindly repeat an unknown issuance. Default lifetime is 30 days, maximum 365; Redis EXAT and explicit expiry checks deny expired identities. The same durable delivery/revocation recovery applies after restart.

Use [the DigitalMate template](../activator/examples/orbit-credential-crypto-template.json) to mount only this workspace's identity directory read-only at `/run/orbit-crypto`, readable by UID/GID 10001. It contains `identity` and trusted `ca.pem`; no control credential, verifier credential or encryption key is mounted. The template uses ADX's existing `${workspace}` for both the workspace context and mount selection. Its environment uses `DIGITALMATE_SECRET_STORE=remote` and `DIGITALMATE_CRYPTO_*`, matching DigitalMate's current client. The existing template name `orbit`, volume paths and `/run/orbit-crypto` mount are retained. The `orbit-config` purpose value is an authenticated cryptographic context, not a display name; renaming it requires controlled decrypt/re-encrypt rather than a variable rename.

Standalone Activator can opt into `credential_delivery` (see the configuration example). Deployment explicitly mounts an identity-only shared volume at `root` on the trusted Activator and the corresponding scoped `root/users/${workspace}` directory read-only on Workers. Activator does not assume access to arbitrary remote node files. Pre-create the trusted shared root and each workspace-volume parent; Platform intentionally creates only the final workspace directory.

For the configured template in remote mode, Activator checks the committed Binding and generation, takes a nonblocking shared-volume file lock, issues a grant, and atomically delivers `identity` and `ca.pem` before Environment creation. The identity is 0400 and owned by the configured workload UID/GID. Server-only metadata and locks are 0700/0600; they contain Binding references, expiry and credential digest, never provider secrets or KEKs. Other workspaces and management credentials are not mounted into DigitalMate.

Configured DigitalMate routing bypasses the normal activation cache so near-expiry or revoked delivered credentials can be replaced. Automatic delivery checks the `DIGITALMATE_*` environment; manual provisioning and other templates keep their existing behavior. The standalone process also checks delivered identities on startup and then after half the renewal window, capped at 300 seconds, without creating or recovering Environments. A one-second window therefore uses a 500 ms polling interval; scheduling delays or unavailable dependencies can still cause expiry. The example configuration uses a 30-day lifetime with a 7-day renewal window. Before atomically publishing a new identity file, delivery persists the replacement digest and all pending old digests in its private metadata. Revocation errors or unknown outcomes leave the pending digests durable for an idempotent retry on a later request or restart; the current identity remains valid. During a failure an old credential can remain authorized until revocation succeeds, its original expiry passes or its Binding becomes invalid. Metadata read/parse failures stop delivery instead of discarding pending revocations. Removal/deletion is still enforced by authoritative Binding verification. The optional delivery mode is Unix-only and requires working shared-volume advisory locks, directory fsync and ownership operations. An embedded deployment must wire the same delivery and renewal lifetime explicitly.

See the [deployment guide](../../docs/deployment/credential-crypto.md) for the ADX-managed rollout and verification procedure. Offline key commands are operator tools, not another service deployment mode. Deployment-specific namespaces, endpoints, identities and keys are supplied by operators and are not included in this repository. Application workspace storage must meet the application's own filesystem and locking requirements; encryption checks do not establish database or storage correctness.

## Rotation and recovery

```sh
adx-credential-crypto rotate-key --keyring /mnt/private/adx-credential-crypto/connection-keyring.json --key-id connection-v2
# Roll out the managed service after keyring changes; retain old KEKs until data and backups are migrated.
adx-credential-crypto rewrap --keyring /mnt/private/adx-credential-crypto/connection-keyring.json --input old-envelope.json --output new-envelope.json
```

Rewrap is an offline operator operation on raw envelope JSON, refuses output overwrite and returns no DEK. Existing `adx-envelope-v1:` prefixed application envelopes require controlled export/import with the writer stopped. Keep protected key backups and prove restoration before retiring an old KEK. The version-1 envelope/AAD is unchanged by this authorization change, so unchanged workspace contexts do not require re-encrypting provider credentials.

## Verification and limits

Service context and validation reuse `adx-agent-core::ServiceAccess`; Gateway transport and TLS reuse existing ADX pool/transport machinery. Encryption AAD and the version-1 envelope remain unchanged.

Run `make agent-test JOBS=2` for Agent and crypto tests, `make rust-check JOBS=2` for the workspace format/Clippy gate, and `make python-check PYTHON_CHECK_BASE=<updated-target-ref>` before committing packaging changes. The `service_grants` store test has an ignored case requiring a disposable `ADX_AGENT_TEST_REDIS_URL`; run it explicitly with `cargo test --locked -p adx-agent-store --features test-memory --test service_grants -- --ignored`. It verifies actual Redis expiry, revocation, Binding generation and conditional writes. The crypto service tests use an in-process authority and synthetic credentials; they are not a cluster deployment or provider OAuth test.

The service has explicit request/connection limits, but no production capacity guarantee. Sustained mixed-workspace load, cross-host identity delivery/locking, disaster recovery and cloud deployment require separate acceptance. The optional identity delivery path is Unix-only; Windows deployments must use trusted manual identity provisioning and owner-only file ACLs.
