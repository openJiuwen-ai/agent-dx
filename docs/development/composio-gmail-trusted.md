# Composio Gmail trusted module candidate

Status: Gmail authorization/read routes remain **unwired and undeployed**.
The separate ingestion module now has explicit existing-process startup assembly.
Development baseline includes official master `d86a2ee9` and merged #179.
Branch: `feat/gmail-composio-trusted`. Phase B extends the existing configuration
and identity owners in this candidate, with independent design review still required.

Delivery alignment: draft ADX #268 now carries both the trusted Composio adapter
and the separate ingestion coordination module, in the existing DigitalMateGateway
process outside user sandboxes. No additional service or coordinator PR is
required by this module split. #179 remains its own configuration-center producer;
DigitalMate #1192 remains the separate-repository consumer. Official baseline
updates use ordinary merges without rewriting the branch history.

## Responsibility and public inputs

`gateway/src/ingress/digitalmategateway/composio` keeps the Composio project API key
outside the user sandbox and permits only Gmail authorization and fixed read
operations. Its parent exports the module. The current phase adds service-secret
import and separate-purpose identity in the existing owners, plus ingestion
startup; it does not change Environment lifecycle ownership.

`Composio::new` requires a key from trusted deployment configuration, the exact
custom Gmail auth config, a real `Authority` and a real shared `Budget`. There are
no defaults for either authority or budget and no provider token storage. Key
configuration is not accepted from request JSON, mobile clients, models, user
sandboxes or template files. The operator imports it into the configuration
center's server-only namespace. Gmail route/authority assembly still depends on
the actual public Registry, Scan and session permission owners.

`Scope::from_authenticated_foreground` is an **internal assembly seam**, not an
authentication implementation. The business identity owner must first validate
tenant, user, workspace, current foreground purpose and revocation. The type has
no Deserialize implementation and cannot be constructed by an HTTP JSON decoder.
Merely calling its constructor or presenting a privacy/crypto bearer proves no
authority. Background identities and organization sharing are not supported.

The stable `dm_` Composio user ID hashes the versioned tenant/user/workspace tuple
with NUL separators; accepted fields contain no controls. Email is not an identity
key, and the mapping is independent of CPU word size or changing OAuth secrets.

`Authority` is a candidate port to the real product connection/authorization
owners. It must enforce scope, generations and atomic state transitions through
their published proxies. This module supplies no connection table, flow database,
fake production implementation, private owner access or second state writer.
`Budget` acquires shared organization/user allowance before every physical
Composio request, including account checks and profile reads. Cluster-wide
fairness/persistence is required of that owner; the module does not simulate it
with an unlimited or per-process counter.

`PostgresBudget` now supplies a real shared organization/user implementation.
Database-clock fixed minute windows charge both counters atomically; denying a
user rolls back the organization charge. A user's allowance spans workspaces.
Replicas must use the same database and stable organization ID; mismatched limit
configuration fails closed. Database failure never falls back to unlimited
access. This does not reserve a minimum share per user or replace upstream 429
handling. The Gateway Gmail routes still need to select this owner at assembly.

## Operations and observable outcomes

| Public call | Behavior and owner requirement |
| --- | --- |
| `start(scope, registration)` | Read current attempt; owner durably CAS-claims Starting before link POST; bind only the account returned from that exact POST. A lost/malformed response remains unknown and cannot trigger another blind POST. |
| `confirm(scope, registration, session_uri)` | Reauthenticated callback context; redeem single-use session with derived user ID; require exact previously bound account and attempt alias; online verify ownership/custom config/Gmail/private/ACTIVE; read profile; owner atomically publishes verification receipt. |
| `reconcile_authorization` | Recover a lost complete_auth reply by reading only the account already bound to this attempt; never replay the single-use session. INITIALIZING/INITIATED stays pending. |
| Completed flow retry | `verified_receipt` reads the owner's retained current-generation proof if the response after owner commit was lost. No repeat complete_auth or receipt write; missing, cancelled or stale proof cannot fabricate success. |
| `cancel` | Delegate local cancellation to the flow owner, which fences late completion. Pending upstream state/cleanup remains that owner's obligation. |
| `gmail(scope, connection, request)` | Resolve current connection, generation and existing tool grant; verify account ownership and bound mailbox online; check owner again before dispatch and after response. Return no late content after disconnect. |
| `disconnect` | Owner fences access first and retains a cleanup handle. Online verify the upstream account belongs to this scope, then best-effort revoke; a previously REVOKED account reconciles without another effect. Failure cannot re-enable local access or be reported as successful upstream revocation. |

Flow receipts publish account identity only. The DigitalMate sandbox consumer
uses ScanRegistrations.keep_external → ConnectionRegistry.save_external for the
sole business connection write. It must recheck the live attempt inside that
write. ACTIVE, requested scopes or possession of an account ID grant no tools.
Search, read_headers and read_body are distinct owner-provided permissions;
provider scope failure still independently refuses access.

Allowed requests are typed `GmailRequest::Search` and `Read`. No user-supplied URL,
method, headers, toolkit, tool slug, Composio user or account ID is accepted.
The private client always specifies both connected_account_id and user_id and
constructs `GET` endpoints under fixed Gmail paths. Profile uses `/users/me`;
messages pin the owner/profile-verified email as Google's userId via encoded URL
path segments. A token rebound between profile and message dispatch cannot select
the other mailbox through `me`. Search pages are limited to 1–100, queries/pages bounded and IDs cannot
contain path/query syntax. Metadata is the default read representation; full
body requires an explicit request and permission. Attachments and write tools
are absent. Bodies are normalized in the DigitalMate mail adapter, not here.

Project Key HTTP 401 is `not_configured`; provider 401 is `reauth_required`, 403
is `consent_required`, and 429 retains bounded retry_after. Outages do not demand
user reauthorization. Effectful network/5xx/malformed replies retain
`outcome_unknown`. No automatic retries or redirects are performed. Streamed
responses are capped at 1 MiB; arbitrary upstream error text, credential fields
and headers are not exposed through errors. Account details deserialize only
safe metadata; API-key headers are marked sensitive. The module emits no payload
logs and adds no plaintext token/secret storage.

## Production prerequisites and open integration

1. Publish/review the #179 business identity and purpose contract; provide real
   callback reauthentication and foreground tool grants. Privacy authorization
   must not implicitly grant arbitrary Composio access.
2. Publish/review DigitalMate [#1176](https://github.com/openJiuwen-ai/DigitalMate/pull/1176)
   connection/authorization owner adapters, current-generation reads and flow
   receipt persistence. The controlled test Authority is **not** that producer.
3. Define recovery for a lost link/create reply and persistence across restarts;
   no blind effect retries are allowed. Confirm CAS/cancel/expiry/replacement and
   cleanup handles with real temporary persistence. Existing #1176 save_external
   rejects replacing a ca ID; a same-mailbox reauthorization that creates a new
   ca requires an owner-published replacement contract, not a private bypass.
4. Supply real organization budget, trusted configuration and route assembly.
   No public Gateway URL is available from this module.
5. Configure a custom Google OAuth application, Gmail API, `gmail.readonly`,
   matching Composio auth config and provider redirect URI. Configure Composio's
   **project callback identity verifier**; it must hold connections pending until
   a signed-in matching user completes them. This is a deployment precondition,
   not something a constructor name or the controlled upstream proves. Verify
   actual OAuth protocol, callback and system-browser behavior with an account.
6. Complete applicable Google restricted-scope verification/security assessment
   before external release. Secrets are supplied through secure local/deployment
   configuration, never chat or committed examples. No OAuth app was registered,
   purchased or deployed by this task.

The separate [ingestion coordinator](connector-ingestion.md) owns Webhook/polling due work, durable
reception, scheduling/batching, requesting ADX readiness and delivery. It may
share the DigitalMateGateway process but is not this module. This module does
not wake Environments, ingest whole mailboxes, send mail, report notifications,
share organization accounts or implement legacy credential/workspace migration.

## Verification scope

The module tests use the actual HTTP client against a loopback Axum Composio
substitute, plus in-memory `ControlledAuthority`/`ControlledBudget`. They verify
the wire shape, account/attempt checks, grants, cancellation/expiry/late results,
revocation, response limits and error separation. They do not prove real owner
persistence, identity, fair cluster limiting, OAuth/Google behavior or assembly.

The full target command is:

```text
cargo fmt --all -- --check
cargo test -p data-plane-gateway --features agent-api composio -- --nocapture
cargo clippy --workspace --all-targets --all-features -- -D warnings
python -B build/docs/check.py
git diff --check
```

On the Windows baseline, the full target test fails before this module compiles:
existing `crates/observability/src/capture.rs` imports Unix-only fd/OpenOptions
interfaces and O_NOFOLLOW. No source or platform configuration was changed to
hide that failure. A task-owned minimal Cargo validation program directly loads
these exact module sources and runs their tests/clippy; this is module evidence,
**not a full Gateway target PASS**. On Windows docs checking needs process-local
`PYTHONUTF8=1`; the first default-GBK failure is retained. The Windows failure is
platform-specific history. Linux local verification now uses the real Gateway
target and workspace, as recorded below. Independent review, native CI, real
owner/provider assembly, deployment and device checks remain required.

### 2026-11-13 Linux verification and delivery permission correction

On official baseline plus local `89349d4d`, real Gateway ordinary tests273 and
explicit PostgreSQL tests10 passed. The subsequent delivery-permission correction
passed12 affected checks: actual PostgreSQL/HTTP purpose verification, real TLS
to an independent Python receiver with SQLite staging, subscription revocation,
duplicate POST and a second independent process reopening the same staging store,
plus startup/entrypoint checks. Revoked retries are refused before connection
access; a committed staging receipt is not rolled back or made execution authority.
Binding, provider and connection facts remain controlled inputs, not live Gmail,
Outlook, ADX deployment or Host/Session execution proof.

The prototype full-configuration bootstrap has been removed from the configuration
owner and Python consumer. Delivery cannot decrypt user model keys or obtain a
privacy identity. `/v1/ingestion/bootstrap` validates delivery then returns503
`AUTH_UNAVAILABLE` without value/token until Runtime publishes a purpose-limited
secret-free contract. The same-root identity and sole existing writers are retained.

Current local canonical-LF `cargo fmt --all -- --check` and the real
`cargo clippy --workspace --all-targets --all-features -- -D warnings` both pass.
Source audit checks1024 Rust/Cargo/SQL inputs and8 exact imported Python inputs;
the last test-only borrow simplification is the sole change after the12 passing
checks. Earlier compiler, format, wrong error-code and missing fixture import-path
failures are retained. Existing orbit_ws fixture background SendError log and AFS
build-identity build-script notice are recorded without source/assertion changes.
This is local checked-source evidence, not native CI or complete mail-to-report proof.

Official API references checked 2026-11-12:
[accounts and callback verification](https://docs.composio.dev/reference/api-reference/connected-accounts),
[link](https://docs.composio.dev/reference/api-reference/connected-accounts/postConnectedAccountsLink),
[complete_auth](https://docs.composio.dev/reference/api-reference/connected-accounts/postConnectedAccountsCompleteAuth),
[proxy](https://docs.composio.dev/reference/api-reference/tools/postToolsExecuteProxy),
[revoke](https://docs.composio.dev/reference/api-reference/connected-accounts/postConnectedAccountsByNanoidRevoke),
[custom OAuth](https://docs.composio.dev/docs/auth-configuration/custom-auth-configs),
[Gmail search/timezones](https://developers.google.com/workspace/gmail/api/guides/filtering)
and [Gmail scopes](https://developers.google.com/workspace/gmail/api/auth/scopes).
