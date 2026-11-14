# Composio Gmail and Outlook server entry

The existing DigitalMateGateway process implements authorization, account
validation, fixed mail reads and subscription synchronization. The project Key
stays outside user sandboxes, encrypted by the existing configuration center.
This is implemented code; real-provider OAuth/mailbox acceptance is separate.

Metadata and body reads retain the provider's message version: Gmail `historyId`
and Outlook `changeKey`. These fields are explicitly selected and retained by the
metadata projection without exposing a snippet or body. Consumers use one version
for both representations; opening a search result must not invalidate its own
task merely because the body is now present. A provider version change remains an
actual source change and must retain the existing invalidation behavior.

## Ownership and assembly

- M44 Scan/Registry remains the only authorization-attempt, connection and intent
  writer. `composio/owner.rs` calls its fixed public proxy using the existing
  signed owner-purpose issuer; it does not read M44 tables or invent grants.
- `composio/client.rs` owns bounded provider calls. `PostgresBudget` charges each
  physical request against shared organization/user windows, including profile
  and account probes. A denied user rolls back the organization charge. Database
  failures never fall back to unlimited allowance.
- The identity owner supplies an opaque `ExistingServiceAdmission` to shared
  `AgentV2Access::select_existing` / `connect_service_when_ready`. Shared access
  owns generation-fenced resolution, route/open retry and the post-readiness
  recheck. Mail adapters neither inspect Environment identity/phase nor own a
  route-opening loop. Missing/rebuilt Bindings cannot be initialized or
  reselected for old admitted work. Ordinary select/WS admission semantics stay
  unchanged; an execution replacement within the admitted generation can use
  the current route without changing the logical authorization. Refused-listener
  waits recheck that admission even when route-store revocation lags; they retain
  the original deadline and one pre-write route refresh budget.
- `entrypoint_runtime.rs` is the Unix runtime assembly. It reads the project Key
  through the existing configuration center, prepares one client/budget and
  binds the actual M44 adapter with each request's Relay handle. No ownership
  cycle, additional service or new Environment lifecycle implementation is added.

## Trusted configuration

`ADX_JIUWEN_CONFIG` may contain:

```json
{
  "composio": {
    "auth_configs": {
      "gmail": {"id": "ac_operator_google", "ownership": "custom"},
      "outlook": {"id": "ac_operator_microsoft", "ownership": "composio_managed"}
    },
    "organization": "operator-stable-organization",
    "database": {"database_url_env": "ADX_COMPOSIO_DATABASE_URL", "database_max_connections": 4},
    "limits": {"organization_per_minute": 60, "user_per_minute": 10}
  }
}
```

IDs are placeholders; either provider is optional. Ownership is explicitly
`custom` or `composio_managed`, never inferred from a request. Matching `privacy`
and `digitalmate` template configuration, encrypted project Key and pre-applied
[budget schema](../../gateway/src/ingress/digitalmategateway/composio/budget.sql)
are required. See [ingestion configuration](connector-ingestion.md) for private
imports and database TLS. Startup performs no DDL and refuses missing dependencies.
Composio without ingestion still supports foreground authorization and mail;
subscription synchronization returns `not_configured` until ingestion is enabled.

## Public HTTP contract

Prefix: `/api/digitalmate/connectors/v1/composio`. Only fixed operations are
accepted; JSON cannot select tenant/user/workspace, auth config, account, upstream
URL, headers or arbitrary tools. Duplicate/extra fields, queries, browser Origin
and oversized bodies are rejected. Responses use no-store/no-referrer.

| POST operation | Input and authenticated scope |
| --- | --- |
| `start`, `state`, `cancel` | `registration`; current business session |
| `confirm` | `registration`, opaque `session_uri`; current business session |
| `validate` | `connection_id`; normal connector identity |
| `mail` | `provider`, `connection_id`, typed `request`, typed `access`; normal connector identity and real M44 read proof |
| `subscription` | `connection_id`; normal connector identity, consumes existing saved intent |

GET `callback` is a static landing page. It does not authorize an account or
perform native App handoff. A privacy or delivery bearer cannot perform these
normal-workload calls. `Scope::from_authenticated_foreground` is an internal
assembly seam, not authentication.

Start/confirm CAS-claim the actual attempt before physical effects. Unknown
outcomes reconcile the account already bound to that attempt; no blind repeated
link POST or single-use redemption occurs. Cancellation, expiry and replacement
fence late results. A completed retry uses the retained owner receipt.

Every account probe requires exact account ID, mapped user, provider, auth config,
ownership, OAuth2 and positive `experimental.account_type=PRIVATE`. Missing/null
type evidence is refused. `INITIALIZING`/`INITIATED` remains `authorization_pending`
on both authorization and read paths; it does not become a usable read grant.
The state endpoint projects this as a waiting phase; other endpoints return
HTTP202 with `error: authorization_pending`. Consumers must recognize that exact
unfinished result, never interpret an arbitrary202 as success or an outage.

Gmail combines the online mailbox address with the immutable Google OAuth2
UserInfo ID. Optional UserInfo email must match and be verified. Outlook requires
the immutable Graph subject. Messages pin that verified mailbox; pagination
must preserve its provider and original query. Search/header/body permissions
remain distinct; current task/intent and connection facts are checked around
budget waits and provider I/O. Late revoked results are discarded.

Metadata is the default. Body access requires explicit current task permission;
subscription proof cannot grant it. No sending or attachment tools are exposed.
Outlook keyword search combines a fixed Graph search projection with exact local
sender/UTC filtering; provider limits do not prove full-history coverage.

Project HTTP401 means `not_configured`; provider401 means `reauth_required`,
provider403 `consent_required`, and429 preserves bounded retry_after. Effectful
transport/5xx/malformed responses remain `outcome_unknown`. No automatic redirect
or physical request retry is performed. Error text contains no provider body/key.

## Existing login behavior and verification

When the configured DigitalMate privacy owner participates in a business WS
handshake, `admit_workload` issues both privacy and connector-purpose credentials
on the same existing grant. Gateway strips any caller-supplied internal header,
then injects `x-digitalmate-connector-identity` for the selected user instance.
Connector identity does not grant mail permission. This affects the existing
DigitalMate WS admission path and requires its authentication/forwarding regression.

Unit tests use controlled provider/authority replies; explicit PostgreSQL tests
exercise encrypted imports, budget and formal bind/serve. The identity TLS tests
in this repository use in-repository clients. Cross-repository process and live
deployment acceptance are separate evidence, not prerequisites for ordinary CI.
Google OAuth uses a system browser/OS authorization session, following
[Google policy](https://developers.google.com/identity/protocols/oauth2/policies).
Actual provider callbacks/scopes, multi-account behavior and browser return need
live acceptance. See [delivery contract](ingestion-delivery-contract.md).
