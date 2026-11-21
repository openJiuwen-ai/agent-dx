# ADX public HTTP contracts

This directory contains the machine-readable contracts for the public ADX HTTP
surface. The Rust API Server and Gateway/EXECD implementations remain the source
of truth; changes to a public route must update the matching contract in the
same change.

- [`environment.yaml`](environment.yaml) describes the public Environment management API,
  schedulable node resources, scheduler compatibility operations and
  administrator-only tenant API Key management. API Server serves these routes
  through Ingress. Agent components use the same Environment capability; Agent
  business routes are documented separately under `agent/`.
- [`data-plane.yaml`](data-plane.yaml) describes the typed runtime data API
  forwarded by Ingress and Relay to EXECD: health, invocation, resumable upload,
  download, PTY and command-watch WebSocket setup.

The generic `/{environmentId}/{port}/{path}` and `/tunnel/{environmentId}/...`
surfaces proxy application-defined protocols. They are routing contracts rather
than typed ADX payloads and are not expanded as OpenAPI operations. EXECD
checkpoint cooperation is an internal Adxlet contract documented in
[`../http/runtime-control.md`](../http/runtime-control.md).

Create requests accept and ignore unknown top-level fields for caller
compatibility. Declared fields and nested objects retain their documented
validation rules. Direct streaming uploads have no ADX-imposed file-size limit;
file permissions follow the [runtime data reference](../http/runtime-data.md#文件上传与权限).

## Runtime action contracts

`POST /direct/{instanceId}/invoke` uses `InvokeRequest.oneOf` to select typed
process, file and shell arguments from `action`. Each variant's
`x-adx-result-schema` points to its result schema. The HTTP 200 response uses
`InvokeResult.anyOf`: the action is not repeated in responses, so the request
selects the result type. EXECD operation errors, Ingress demand errors and HTTP
transport failures have separate error contracts. Schemas describe supported
caller payloads; they do not imply that EXECD rejects every additional property.

`process.exec` is the synchronous primitive. SDK `commands.run` uses
`process.start` plus result observation for its default/long wait path and
background commands; short execution timeouts use `process.exec`. A missing
per-command cwd on the stable-command path produces `FAILED`, `SPAWN_FAILED`
and null `exit_code`; the synchronous primitive uses exit_code=-1 and stderr.
`process.wait` deadline expiry returns `WAIT_TIMEOUT` while the
process continues running. Omitted or empty synchronous commands are valid empty
shell commands. Binary invoke file data is hex; streaming upload and
download bodies contain raw bytes. Persistent shell actions use a PTY and SDK
framing; they are distinct from command records.

The [runtime data reference](../http/runtime-data.md) lists arguments, wire
results, timeout behavior and error mappings with their current support bounds.

Idle pause uses `idleTimeoutSeconds`, `idleAction: pause` and `pauseOptions`.
The resulting Paused Environment supports either manual resume or authorized
traffic-triggered resume. Both share the same durable intent; no separate
auto-resume flag is required. See the
[lifecycle API](../../../gateway/apiserver/docs/environment-lifecycle-api.md#idle-pause-and-auto-resume).

## Contract checks

```sh
python3 -m venv out/docs-venv
out/docs-venv/bin/python -m pip install -r build/docs/requirements.txt
out/docs-venv/bin/python build/docs/openapi_check.py
out/docs-venv/bin/python -m unittest discover -s build/docs/tests -v
python3 build/docs/check.py
```

The basic CI source gate runs the OpenAPI checks. They compare the implemented
Environment/scheduler/admin routes, create JSON fields and all public
`process.*`/`file.*`/`shell.*`/`entrypoint.*` dispatch aliases with the contracts;
validate schema syntax, local references, operation IDs and schema examples;
and test missing-route/field/action detection and invalid payloads.

This is a contract gate, not runtime E2E acceptance or a proof that every
implementation branch matches the schema. Changes to Rust routing/dispatch
structure must keep the extractor and its tests in sync. Generic forwarding,
internal RPCs and Agent business APIs are outside this check's scope. Older
`/api/environment/create` and `/api/environment/{id}` lifecycle aliases keep their
existing compatibility behavior; the canonical public contract is v1.
