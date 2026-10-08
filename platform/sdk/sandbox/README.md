# Agent DX Sandbox SDKs

This subtree is the multi-language SDK workspace for Agent DX remote
sandboxes.  The Python SDK is implemented today; Go, Rust, and Java are reserved
as first-class SDK directories so future clients can share the same repository,
release process, examples policy, and protocol vocabulary.

## Layout

| Path | Status | Purpose |
| --- | --- | --- |
| `python/` | implemented | Python package `adx-sandbox`, exposing `adx_sandbox.Sandbox` and the CLI entrypoint. |
| `go/` | planned | Future Go SDK module. Keep Go-specific code, examples, and tests here. |
| `rust/` | planned | Future Rust crate. Keep Rust-specific code, examples, and tests here. |
| `java/` | planned | Future Java/Maven or Gradle SDK. Keep Java-specific code, examples, and tests here. |
| `build.sh` | root build entrypoint | Builds the Python SDK by default for existing CI/release callers. |

## Implemented SDK

See [`python/README.md`](python/README.md) for install/configuration details and
runnable examples.

Quick build from the monorepo root:

```bash
PYTHON=python3 bash platform/sdk/sandbox/build.sh /tmp/adx-sandbox-dist
```

Equivalent Python-only build:

```bash
cd platform/sdk/sandbox/python
PYTHON=python3 bash build.sh /tmp/adx-sandbox-dist
```

## Cross-language conventions

All language SDKs should keep the same user-facing concepts:

- `Sandbox` lifecycle: create, command execution, filesystem operations, kill.
- Python callers may pass an explicit `ConnectionConfig`; all SDKs retain
  environment fallback through `ADX_SERVER_ADDRESS`, `ADX_TOKEN`, optional
  `ADX_GATEWAY_ADDRESS`, and TLS flags.
- Frontend control plane: `/api/sandbox/v1/sandboxes`.
- Direct file and action data plane through `/direct/...`, plus reverse tunnel
  access through `/tunnel/...`.
- Runnable examples only. Infra-specific or nonportable demos should live in docs
  or private test fixtures, not in public SDK example directories.

## HTTP RESTful API contract

All language SDKs should target the current frontend HTTP/WS contract instead
of exposing runtime-internal ports to users. The detailed platform reference is
maintained in the [Go HTTP reference](../../../gateway/apiserver/docs/sandbox-lifecycle-api.md). That reference distinguishes retained client options from the new server's supported capabilities.

### Environment and auth

The Python SDK accepts the same values as an immutable `ConnectionConfig` on
`Sandbox`, `Sandbox.delete`, and `resources`. Explicit configuration is kept on
the SDK objects and does not read or modify process-global `ADX_*` connection
variables. Omitting it preserves the environment-based behavior below.

| Setting | Meaning |
| --- | --- |
| `ADX_SERVER_ADDRESS` | Primary service entry `host:port`. Used for lifecycle and required `/direct` traffic. |
| `ADX_TOKEN` | API Key for the new platform; the SDK transports an opaque credential. |
| `ADX_TLS` | `1/true/yes` selects `https://` for primary service routes; `0/false/no` selects plaintext HTTP. |
| `ADX_GATEWAY_ADDRESS` | Optional gateway/router `host:port` for reverse tunnel and user port URLs; falls back to `ADX_SERVER_ADDRESS`. |
| `ADX_GATEWAY_TLS` | `1/true/yes` selects `wss://` for `/tunnel` and `https://` for user port URLs; default is plaintext. |

### Control plane

Base path: `/api/sandbox/v1/sandboxes` on `ADX_SERVER_ADDRESS`.

| Method | Path | Body / query | Result |
| --- | --- | --- | --- |
| `POST` | `/api/sandbox/v1/sandboxes` | `CreateV1Request` JSON | `{sandboxId, instanceId, status, tunnel?}` |
| `DELETE` | `/api/sandbox/v1/sandboxes/{sandboxID}` | none | idempotent teardown; `404` is treated as already deleted by the SDK |
| `POST` | `/api/sandbox/v1/sandboxes/{sandboxID}/invoke` | compatibility route | unsupported by the new backend; use `/direct` |

`CreateV1Request` fields used by SDKs include `name`, `namespace`, `tenant`,
`runtime`, `image`/`rootfs`, `ports`, `idleTimeoutSeconds`,
`createTimeoutSeconds`, `scheduleTimeoutSeconds`, `cpu`, `memory`,
`cpu_limit`, `mem_limit`, `storage_limit_mb`, `env`, `mounts`, `extra_config`,
`tunnel`, and the
optional per-sandbox `dataPlane` security policy. Python exposes the latter as
`DataPlaneSecurityPolicy`; the client model accepts `tls` and `tls-token`, but per-sandbox policy is rejected by the new control backend. Direct remains TLS with authentication. Mounts, extra_config, public ports and independent limits in the retained schema are also not supported by this server.
Frontend owns internal EXECD port environment injection (`EXECD_HTTP_PORT`,
`EXECD_TUNNEL_WS_PORT`, `EXECD_TUNNEL_HTTP_PORT`); SDK callers should request
features declaratively instead of setting those ports.

Create and schedule timeouts use seconds. The create timeout covers scheduling,
the SDK-owned 30-second runtime initialization budget, and a 30-second frontend
response buffer. The initialization value is carried in an internal request
field and is not exposed as a constructor option. Callers normally set only
one public timeout: `createTimeoutSeconds = scheduleTimeoutSeconds + 60`. If
both public timeouts are sent, their difference must be at least 30 seconds;
the SDK expands the outer create budget when the full 60-second reserve is not
covered.

### Direct data plane

Commands/files use Ingress `/direct` → Relay → EXECD. Ingress authenticates and strips public credentials before node forwarding. The SDK retains a legacy `/invoke` fallback, but the new Go backend rejects that transport; it is not a second working data path.

| Method | Path | Body / query | Use |
| --- | --- | --- | --- |
| `POST` | `/direct/{safeID}/invoke` | `{"action": string, "args": object}` | low-latency command/fs/shell action invoke |
| `GET` | `/direct/{safeID}/healthz` | none | EXECD health probe |
| `POST` | `/direct/{safeID}/upload?path=<abs>&type=file|tar` | raw bytes or tar stream | binary upload / directory upload |
| `GET` | `/direct/{safeID}/download?path=<abs>&type=file|tar` | none | binary download / directory download |

`safeID` is the router-safe form of `sandboxID`. The old explicit-port form
`/direct/{safeID}/{execdPort}/...` is a frontend compatibility alias only; new
SDKs should not expose it.

### Tunnel and user ports

| Surface | URL shape | Notes |
| --- | --- | --- |
| Reverse tunnel | `/tunnel/{safeID}` | SDK connects a local upstream to the gateway; default is plaintext `ws://` and does not send `ADX_TOKEN`. |
| User port forwarding | `http://<sandbox-router>/<safeID>/<port>` | Returned by SDK `get_port_url(port)` for ports requested at create time. The SDK URL helper remains; new control-plane publication of user ports is unsupported. |

The shared action envelope is always `{"action": <name>, "args": {...}}`.
Supported action names include process (`process.exec`, `process.start`,
`process.poll`, `process.wait`, `process.kill`, `process.send_stdin`), file
(`file.read`, `file.write`, `file.list`, `file.exists`, `file.remove`,
`file.rename`, `file.mkdir`, `file.stat`), and shell (`shell.create`,
`shell.run`, `shell.poll`, `shell.delete`) operations.

## Adding another language SDK

When adding Go/Rust/Java support:

1. Put language-native package metadata under that language directory only.
2. Add a language-local `README.md`, `examples/`, and `tests/`.
3. Reuse the protocol names and environment variables above.
4. Add CI/build steps explicitly for that language; do not overload the Python
   package build.
5. Keep root `build.sh` stable unless the release pipeline is updated deliberately.
