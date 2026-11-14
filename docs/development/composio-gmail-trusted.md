# Composio mail trusted module candidate

Status: the existing Gateway process now prepares the configured Composio client,
encrypted project key, shared PostgreSQL budget and actual owner/Relay adapter.
Normal connector identities can use fixed account validation and task-scoped mail
reads; foreground OAuth still requires the existing business session.
Subscription reconciliation, receiver/worker deployment and live mail acceptance
remain unfinished. The separate ingestion module has its own explicit startup.
Development baseline includes official master `52c48719` and merged #179.
Branch: `feat/gmail-composio-trusted`. Phase B extends the existing configuration
and identity owners in this candidate, with independent design review still required.

The explicit `Composio::with_m44_configs` factory now binds the actual owner
issuer and session/workload guard to M44's nine fixed public operations over
the existing `AgentV2Access`/Ingress Relay. It selects the existing full Binding
generation and exact Environment/port/path, reissues only the short owner-call
credential after readiness, and never retries a physically sent owner write.
Metadata and current access are read from M44; only `read_history` enables
header/search projections. Body access requires a current task/account/tool Read
proof; subscription proofs cannot grant it. No second connection or
authorization writer is introduced. Disconnect remains unavailable until the
original owner publishes its fencing operation.

Outlook requires its immutable online Graph subject even when M44 has no display
email. Gmail combines its online mailbox profile with Google's fixed OAuth2 v2
UserInfo endpoint, requesting `id,email,verified_email`. The immutable Google
`id` is required for the original M44 verified-effect writer. An optional
UserInfo email must match the mailbox and be verified; a profile-only response
may supply the ID without an email. Email and Composio user_id are not substitutes.
The existing Google profile scope supports this endpoint without requiring an
additional OpenID scope. Actual managed-account responses remain unverified.
The Gmail regression first failed on the missing immutable ID; the final focused
suite has 67 passing Composio tests and one existing ignored PG test, and focused
library Clippy passes with warnings denied.
Those checks compiled the actual Relay adapter but used controlled provider/
authority ports; real Rust-to-M44 composition, formal startup, persisted runtime
connector identity, full workspace gates and real Outlook are still incomplete.

The normal connector carrier is now validated across repositories against the
actual existing HTTPS identity service and persisted PG grant. The explicitly
run ignored test `normal_connector_recovers_over_real_https_after_access_expiry_without_mobile`
passed once (334 filtered). The original signer constructed an access-expired
normal credential by changing only exp on the same actually admitted grant;
neither the service clock nor database recovery expiry was changed. Independent
Python consumer processes renewed/verified/saved it, restarted from its private
file, then refused it after the current Binding generation changed. Database
assertions found zero mobile sessions and one grant. Consumer source and ADX
inputs were hashed before/after with zero changes; the task-owned PG was stopped
after UID/executable/data-path/PID verification.

Binding observations remain controlled. This case does not contain an accepted
ingestion batch, M44 read, actual Outlook account or report endpoint, so it does
not close the accepted-mail-to-report recovery or production startup gates.

Delivery alignment: draft ADX #268 now carries both the trusted Composio adapter
and the separate ingestion coordination module, in the existing DigitalMateGateway
process outside user sandboxes. No additional service or coordinator PR is
required by this module split. #179 remains its own configuration-center producer;
DigitalMate #1192 remains the separate-repository consumer. Official baseline
updates use ordinary merges without rewriting the branch history.

## Responsibility and public inputs

`gateway/src/ingress/digitalmategateway/composio` keeps the Composio project API key
outside the user sandbox and permits only declared Gmail/Outlook authorization and fixed read
operations. Its parent exports the module. The current phase adds service-secret
import and separate-purpose identity in the existing owners, plus ingestion
startup; it does not change Environment lifecycle ownership.

`Composio::with_configs` requires a server-only project key, explicit per-provider
`MailAuthConfig` entries, a real `Authority` and shared `Budget`. Each entry fixes
an exact auth-config ID and `Custom` or `ComposioManaged` ownership; missing
providers and reused IDs fail closed. Online account checks require that exact
provider/config/ownership, OAuth2, PRIVATE, current user and active status.
`Composio::new` remains the existing custom-Gmail convenience constructor.
Neither authority nor budget has a default; no provider tokens are stored here.
The user's first live-test selection is now managed Gmail and personal Outlook.
Customer-owned OAuth applications remain an optional explicit configuration.

Configuration never comes from request JSON, a model, sandbox or mobile client.
The project key is imported into #179's server-only namespace. The existing
asynchronous `DigitalMateGatewayApi::build` prepares the real client, shared budget
and owner issuer when trusted `ADX_JIUWEN_CONFIG` enables `composio`. It requires
matching `privacy`/`digitalmate` templates, not `ingestion`, a webhook secret, a
background toggle or a model call. The synchronous constructor refuses this
configuration so startup cannot silently leave the client absent.

The additional configuration section is:

```json
{
  "composio": {
    "auth_configs": {
      "outlook": {"id": "ac_operator_supplied", "ownership": "composio_managed"}
    },
    "organization": "operator-stable-organization",
    "database": {"database_url_env": "ADX_COMPOSIO_DATABASE_URL", "database_max_connections": 4},
    "limits": {"organization_per_minute": 60, "user_per_minute": 10}
  }
}
```

`gmail` uses the same optional per-provider shape. Ownership is explicitly
`custom` or `composio_managed`; the example ID must be replaced by the operator's
real project configuration. No API key, upstream origin or callback URL is accepted
in this section. The key comes from existing `ComposioProjectKey` service-secret
storage. The operator first applies [budget.sql](../../gateway/src/ingress/digitalmategateway/composio/budget.sql);
startup uses `PostgresBudget::from_pool` and performs no schema creation. Missing
key/schema, invalid bounds, policy mismatch or a database failure refuses startup.
The same private bounded/TLS pool builder serves ingestion and request accounting,
with separate caller-owned pools and schemas; neither borrows the account pool.

The prepared HTTP client, connection pool and request budget are shared between
requests. The existing request's `Arc<Ingress>` only binds the actual M44 Relay
adapter, avoiding an API/Ingress reference cycle or a new lifecycle service.

`Scope::from_authenticated_foreground` is an **internal assembly seam**, not an
authentication implementation. The business identity owner must first validate
tenant, user, workspace, current foreground purpose and revocation. The type has
no Deserialize implementation and cannot be constructed by an HTTP JSON decoder.
Merely calling its constructor or presenting a privacy/crypto bearer proves no
authority. Normal connector workloads cannot create OAuth effects. Mail requires
typed task/epoch or current subscription-intent facts in addition to identity;
those facts grant nothing until the actual M44 owner authorizes them.

`Authority::authorize_read` uses the existing fixed Relay and the separately
admitted `Read` owner operation. The reply must match `connector.mail.read`, the
exact connection, raw mail-request SHA-256, task epoch or intent revision and
current authorization generation/record revision, with an expiry within15 seconds.
The M44 query uses the original task, sharing/tool restrictions and account consent;
it creates no question, task or grant. Gateway rechecks after budget acquisition
before physical mail dispatch and again after a provider result. Late revoked
content is suppressed; no retry creates another task or account permission.

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
handling. The formal configured Gateway selects this same owner at assembly.

## Operations and observable outcomes

| Public call | Behavior and owner requirement |
| --- | --- |
| `start(scope, registration)` | Read current attempt; owner durably CAS-claims Starting before link POST; bind only the account returned from that exact POST. A lost/malformed response remains unknown and cannot trigger another blind POST. |
| `confirm(scope, registration, session_uri)` | Reauthenticated callback context; real owner must durably claim a domain-separated request digest before single-use redemption. Repeated unknown outcomes query the original account rather than redeem again. Require exact bound account/alias and online ownership/config/provider/private/ACTIVE, then owner publishes the verification receipt. |
| `reconcile_authorization` | Recover a lost complete_auth reply by reading only the account already bound to this attempt; never replay the single-use session. INITIALIZING/INITIATED stays pending. |
| Completed flow retry | `verified_receipt` reads the owner's retained current-generation proof if the response after owner commit was lost. No repeat complete_auth or receipt write; missing, cancelled or stale proof cannot fabricate success. |
| `cancel` | Delegate local cancellation to the flow owner, which fences late completion. Pending upstream state/cleanup remains that owner's obligation. |
| `mail(scope, connection, MailRequest)` (legacy Gmail wrapper retained) | Resolve current connection, generation and existing tool grant; verify account ownership and bound mailbox online; check owner again before dispatch and after response. Return no late content after disconnect. |
| `POST /composio/subscription` | Normal connector identity only; consume the actual saved M44 intent, synchronize the exact configured account/trigger and register through the existing Inbox. Request JSON only supplies connection_id; it cannot enable an intent. Missing ingestion is not_configured. |
| `disconnect` | Owner fences access first and retains a cleanup handle. Online verify the upstream account belongs to this scope, then best-effort revoke; a previously REVOKED account reconciles without another effect. Failure cannot re-enable local access or be reported as successful upstream revocation. |

Flow receipts publish account identity only. The DigitalMate sandbox consumer
uses ScanRegistrations.keep_external → ConnectionRegistry.save_external for the
sole business connection write. It must recheck the live attempt inside that
write. ACTIVE, requested scopes or possession of an account ID grant no tools.
Search, read_headers and read_body are distinct owner-provided permissions;
provider scope failure still independently refuses access.

The common `MailRequest` selects typed `GmailRequest` or `OutlookRequest` search
and read operations. Its provider must match the connection owner's canonical
Google/Microsoft grant; a request selector is not permission. No arbitrary method, headers, toolkit, tool slug, Composio user/account or URL is
accepted. Outlook nextLink is opaque pagination data checked against the exact
verified mailbox and original bounded query before use.
Outlook keyword search uses Graph's fixed `$search` projection over from/subject/body.
Because Graph does not combine message `$search` and `$filter`, Gateway requests
only IDs plus sender/time metadata and applies exact sender and UTC bounds before
returning IDs. Empty filtered pages retain their validated continuation. The
provider caps keyword matches at1,000; this does not prove full-history coverage.
[Microsoft Graph search semantics](https://learn.microsoft.com/en-us/graph/search-query-parameter).
The private client always specifies both connected_account_id and user_id and
constructs `GET` endpoints under fixed Gmail paths plus the fixed Google UserInfo
endpoint. Mailbox profile uses `/users/me`;
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

### Foreground managed Outlook first

The current priority is the app's Outlook connect gesture, browser consent,
listing and explicit reading, followed by session consumption. A real business
login and M44 current-attempt/effect authority are required. The connect gesture
itself has no task ID and does not require background Host admission or ingestion
bootstrap. Mail tools separately need actual current task/operation permission.
The simple background path is also being connected for one Outlook account and
one new message: explicit owner enable/disable, exact event-ID dedupe, existing
Gateway PostgreSQL staging, existing ADX readiness and the actual receiver worker.
It does not require building a general scheduling or Host policy platform.

The `composio::http` candidate adapts already-authenticated calls; it does not
authenticate `Scope` or mount itself. The fixed prefix is
`/api/digitalmate/connectors/v1/composio`; POST `start`, `state`, `cancel` accept
only `registration`, and POST `confirm` additionally accepts opaque `session_uri`.
POST `mail` accepts `provider`, `connection_id`, a typed `request` and `access`, e.g.
`{"provider":"outlook","connection_id":"connection-one","request":{"operation":"read","message_id":"msg_A","body":false},"access":{"kind":"foreground","task_id":"task-one","execution_epoch":0,"authorization_generation":1,"record_revision":1}}`.
Missing access is forbidden. JSON cannot self-report allowed/body permission.
Requests cannot choose tenant, user, workspace, auth config, account, URL, headers
or arbitrary tool. Extra/duplicate fields, queries, browser Origin and oversized
bodies are refused. Replies carry no-store/no-referrer. The public callback
landing is static and cannot activate an account or implement app handoff.

The outer entry consumes #179's actual business identity for OAuth effects and
normal connector identity for validation/mail, rechecking the actual guard after
I/O. Formal startup binds the actual M44 owner and existing Relay. A controlled
test Scope is not authentication or deployed acceptance.

Do not assume Feishu's ArkWeb works for every provider. In particular,
[Google OAuth policy](https://developers.google.com/identity/protocols/oauth2/policies)
prohibits a developer-controlled embedded user agent. Gmail needs a system
browser or OS authentication session; a controlled return to the app must use
native authenticated confirmation. No business bearer enters a web page, URL or
JavaScript bridge. Browser return still needs actual platform verification.

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
5. Select exact managed Gmail/Outlook auth configs for the first live test. Custom
   applications are optional, with their appropriate provider redirect/scopes.
   Microsoft profile verification requires delegated User.Read alongside Mail.Read.
   Configure Composio's
   **project callback identity verifier**; it must hold connections pending until
   a signed-in matching user completes them. This is a deployment precondition,
   not something a constructor name or the controlled upstream proves. Verify
   actual OAuth protocol, callback and system-browser behavior with an account.
6. Complete applicable provider/application release prerequisites for the selected
   app ownership; self-owned Google verification is not a managed-test prerequisite. Secrets are supplied through secure local/deployment
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
privacy identity. `/v1/ingestion/bootstrap` accepts the current delivery bearer
and exactly `{}`. With the existing configured owner runtime, it returns only
`purpose: "ingestion.connector-bootstrap"`, the current `target`, a
`connector_token` and `expires_at`. The existing grant and signer are reused;
the signed bootstrap deadline cannot outlive delivery and `/v1/connector/renew`
refuses this credential. The target is derived from the complete current Binding
and configured port, matching the persisted batch scope/generation/port. Missing
owner configuration still returns `AUTH_UNAVAILABLE`. No privacy bearer, settings,
model keys or task/model permission is returned. Receiver intent and connection
permission checks remain mandatory. Local producer tests do not prove a deployed
cold receiver or live mailbox integration.

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

## Shared Outlook and managed-app increment (2026-11-13)

Both providers reuse one HTTP client, current connection/attempt authority, shared
request budget, account verification, error/unknown-outcome handling and late
revocation fence. Provider differences remain fixed read endpoint/projection logic.
Microsoft's online `/me` profile supplies an immutable Graph subject; owner grants
must match both that subject and mailbox. Actual message reads use
`/v1.0/users/{verified_subject}/messages`, not a mutable `me` selector. `User.Read`
and `Mail.Read` are delegated personal-account permissions; there is no shared
mailbox, app-only flow, attachment fetch or write endpoint.

Outlook supports sender and canonical UTC whole-second bounds,1–100 rows, and
opaque nextLink. Pagination cannot change origin, subject, resource, select,
filter or top, add duplicate/extra parameters or request attachments. It preserves
the complete provider URL after validation. List/header projections omit body and
bodyPreview; body is explicit and still checked against the real owner grant.
Gmail header reads likewise select/project only requested headers and suppress
snippet and MIME body/parts, even if the upstream returned those extra fields.

A real test-first compile failure recorded the missing shared API/types. The first
43-case component pass was followed by an additional runtime RED demonstrating
Gmail metadata snippet leakage; the final44-case focused suite passes. The same
actual Gateway agent-api lib binary passes255 ordinary cases,0 failures,13
explicitly ignored cases. Full-workspace fmt and all-target/all-feature clippy
with `-D warnings` pass;1025 Rust/Cargo/SQL inputs include the new Outlook module
and were unchanged during verification. Ignored PG/Redis/SSH/performance checks
were not run in this increment. Local HTTP/provider/Authority/Budget fixtures do
not prove live managed consent, mail delivery or production route assembly.

Read-only NB readiness checks confirm two managed OAuth2 configurations. Its four
Gmail connections are expired and Outlook has none; no old connection is adopted
as product authority. No new account/trigger or project setting was written.
Real browser consent, project verifier, M44 current-attempt proof, background
intent/Host permission and minimal Runtime startup remain explicit integration
requirements. Managed OAuth removes customer app registration from the first
test's prerequisites; it does not remove those product authority requirements.

Primary references: [Graph messages](https://learn.microsoft.com/en-us/graph/api/user-list-messages?view=graph-rest-1.0),
[Graph profile](https://learn.microsoft.com/en-us/graph/api/user-get?view=graph-rest-1.0),
[Composio Connect Link and callback identity verification](https://docs.composio.dev/reference/api-reference/connected-accounts).

## HTTP adapter and single-use confirmation increment

The new HTTP adapter first failed compilation because the API did not exist.
Its first implementation compile exposed an insufficient generic Body error
bound; correcting it to the actual existing HTTP entry's bounds produced six
passing HTTP cases and50 total Composio cases. The single-use counterexample then
actually executed and failed: three confirmation requests dispatched instead of
one. The caller now requires `Authority::claim_confirmation(scope, attempt,
session_digest)` before dispatch; no default authority implementation is added.
Only a domain-separated digest of scope/provider/account/registration/revision
and session reaches that port. Invalid input consumes no claim; successful retry
reads the existing receipt, while unknown confirmation can only reconcile.

The final source passes53 Composio cases and264 ordinary Gateway library cases
(0 failures,13 ignored); the53 are included in264. Changed Rust formatting passes
with the workspace's actual2021 edition. All1026 Rust/Cargo/SQL inputs were
unchanged during that run. The real HTTP adapter and upstream local HTTP server
participated; Authority/Budget/provider responses were controlled. Real M44
durable claims, actual authenticated route mounting, real browser return and
real Outlook mailbox delivery remain unverified. Full-workspace Clippy and PG
were not rerun for this increment; earlier receipts retain their exact inputs.

## Actual foreground entry and current-session checks

`DigitalMateGatewayApi::with_composio` explicitly supplies a real client built
with Authority and shared Budget. The actual Ingress dispatcher recognizes the
fixed connector routes; they return404 without that assembly. Admission uses
the configured DigitalMate application and actual ForegroundAdmission, then
reads at most16KiB within10 seconds and rechecks the session before dispatch and
before returning. JSON cannot construct tenant/user/workspace or select config.

`Scope::from_foreground_identity` retains the real session guard. Every physical
Composio request checks it before the budget, after a possible budget wait, and
before returning content. Real owner issuers require `foreground_identity()`;
detached namespace fields cannot substitute for it. The public browser landing
remains static, GET-only, no-store and performs no confirmation or echo of query
secrets. Existing WebSocket/login routes are unchanged.

The composed source includes actual identity producer71ae71b2 and its persisted
PG/HTTPS proof252df9bb (locally952e9450/4f2e8295). With existing agent-api,mock-e2e
test features,8 foreground cases pass,54 Composio/budget cases pass with1 ignored,
and the same actual Gateway binary passes313 cases with16 ignored. Scope/session,
managed target and actual dispatcher participate; account/Environment/provider
services remain controlled. This does not establish real M44 HTTP consumption,
App start/state/confirm, browser return, managed Outlook or deployment.

An initial agent-api-only run compiled but selected0 foreground cases because
the repository places that module under mock-e2e; it was correctly refused as
verification. The rerun uses the existing feature rather than changing its gate.
Its original six-file fmt check refused CRLF worktree bytes; only those line
endings were normalized to LF and the same six-file formatter check then passes.
The successful test process also emitted an existing Orbit WebSocket fixture
SendError panic from an unchanged file while returning0; that observation is
retained separately rather than reported as a panic-free run. Full-workspace
Clippy, real two-side HTTPS and PG are still unrun for this new mounted source.

## Formal configuration and account validation follow-up

`POST /api/digitalmate/connectors/v1/composio/validate` accepts only
`{"connection_id":"..."}`. It requires the actual normal connector-purpose
identity and live original grant/Binding, not a phone/privacy/delivery bearer.
The M44 adapter uses existing `connection` and `metadata` owner operations to
check association, external custody, provider/account/subject and record generation.
It intentionally accepts an unvalidated current account without requiring
`usable` or calling `ensure_ready`, so the Registry's own validation cannot recurse.

Gateway then verifies the fixed project/config/ownership account, online mailbox
profile and a fixed one-ID search probe. Mail IDs/content are discarded. Current
metadata is checked before the probe and after its result; changes are refused.
The returned bounded proof contains the exact connection/account/subject,
authorization generation, record revision, display email and `read_history`
evidence. The endpoint writes no connection/capability/intent; the original M44
Registry validates that same record and owns its subsequent CAS. Gmail first
validation pins its immutable UserInfo ID and online mailbox profile email;
Outlook still pins its immutable Graph
subject and can have an empty display email. Neither case enables a subscription.

The formal process regression first failed on the real unknown `composio` field.
After implementation, configuration runs once and passes; a fresh PG database
and actual `IngressService::bind/serve` separately pass two exact startup tests:
project key/shared budget required with ingestion off, and existing ingestion
webhook startup/shutdown. The missing budget test proves startup creates no schema.
The isolated process uses synthetic service/provider configuration, never a live
Composio key, account, model or mail call.

The paired validation source passes58 Composio cases and331 ordinary Gateway
library cases, with19 ignored cases still unrun in that suite. Focused Gateway
library Clippy passes with `-D warnings`. The unchanged Orbit WebSocket fixture's
background SendError panic remains in the successful ordinary log. Global fmt
on this Windows CRLF checkout reports988 newline-style diagnostics and no code
diffs; full-workspace Clippy and actual Gateway-to-M44/Outlook/report/SFS/device
acceptance are still open. These results do not claim deployed mail delivery.

## Formal subscription synchronization follow-up

`POST /api/digitalmate/connectors/v1/composio/subscription` now consumes the real
normal connector identity and a strict connection-only body. Prepared startup
shares the typed M44 authority, project client/budget and existing ingestion
Inbox; no second queue, connection writer, pool or lifecycle service is created.
Without ingestion, OAuth and mail still work while synchronization fails closed.
See the [subscription contract and exact verification scope](ingestion-subscription-state.md#formal-subscription-http-assembly).

HTTP/Prepared increment `ebc2932970eb5c7a14fa35aea88d90c06295eac0` is combined
with official master `18f9c673812dbc988185136011023ea28bdd38f7` in
`d8c4743eceedefd87370ce97651ca55eb43459a6`. The merged source passes348 ordinary
Gateway cases/28 ignored, four additional explicit PostgreSQL/actual startup
cases, focused library Clippy, explicit changed-file formatting and docs checks.
The HTTP/PG case uses the actual project client and Inbox but controlled intent
and provider replies; real M44, OAuth, callbacks and HK/SFS remain unaccepted.
