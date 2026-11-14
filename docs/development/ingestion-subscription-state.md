# Ingestion subscription registration state

The PostgreSQL ingestion `Inbox` remains the sole subscription/queue writer.
It does not own account authorization or the user's background opt-in. A caller
must read the actual current connection/intent owner before provider effects or
registration. A decoded `SubscriptionBinding` is not that permission.

## Published owner methods

- `Inbox::subscription(id)` returns the existing persisted registration, including
  its local enabled fence, or `None`. It validates the stored representation and
  does not refresh, grant access, or read another owner's tables.
- `Inbox::disable_subscription(expected)` compares the complete previously read
  binding under the existing workspace transaction lock. A changed account,
  target or generation returns `LeaseLost`. Retrying the same disabled binding
  succeeds; the method never invents an intent revision or enables a subscription.
- Stopping retires only this subscription's pending events/ready batches, releases
  their capacity, and invalidates its active lease. Another account's lease in the
  same workspace remains usable. The disabled record is retained so a provider
  disable whose response was lost can be retried after restart.
- Replacement registration uses the same retirement helper. Existing
  `Inbox::register` generation and conflict rules are preserved; an old enabled
  projection cannot resurrect a fenced epoch.

No schema, queue, signer, credential store or lifecycle authority is added.
Provider I/O is performed outside the Inbox transaction. Actual provider trigger
management and startup mounting are separate consumers; these methods alone do
not install automatic subscriptions.

## Intent synchronization port

`SubscriptionCoordinator::synchronize(scope, connection_id)` consumes existing
owner opt-in through `IntentPort`, the restricted project client through
`TriggerPort`, and the same Inbox. The authenticated adapter matches current
Connection/Metadata/Intent facts; routing comes from the real issuer/Binding.
`None` is explicit deletion. A transport failure or temporarily unusable account
is not opt-out. Account, auth config, mapped project user and configured trigger
must match; no first-account fallback, caller-provided URL or arbitrary trigger
is accepted. Identity data does not substitute for the owner adapter.

The coordinator compares current facts around external I/O and registration.
It fences a stopped registration before upstream disable, retains a failed
cleanup for restart/retry, and refuses stale epochs. The returned enabled binding
is an existing serde projection, not new authority. The core does not hardcode
mail providers, mail filters or toolkit slugs. The production adapter and HTTP
mount must be supplied separately; a passing substituted-port test does not
prove that production route is installed.

Existing same-generation registration conflicts remain explicit. Restart within
the same Binding is covered; changing the actual Binding target requires explicit
rebind rather than silently inventing an intent revision. Cross-owner/HTTP work
is not one transaction. The receiver must still verify live intent and the exact
binding; synchronization failures must be retried against current owner facts.

## Verification scope

Source base: `c4e2168f185be54ab9650fc1af52ca6be5de7d1d`. New tests fail to compile
against the old methods (missing APIs), then three task-owned PostgreSQL tests
pass for reopen/idempotent stop, full-target CAS, stale epoch refusal, exact
capacity retirement, in-flight fencing and two-account lease isolation. The two
existing PostgreSQL persistence regressions and seventeen ordinary ingestion
tests pass. Focused production-library Clippy with warnings denied and workspace
formatting pass. An inherited test-only unused-helper warning remains unchanged.

The isolated fixture contains synthetic provider data. It is not a real M44 or
Composio integration, whole-workspace Clippy, Hong Kong deployment, SFS or app
acceptance. The existing explicit PostgreSQL opt-in markers remain in place; the
five database tests above were run explicitly, rather than counted as skipped.

The synchronization candidate consumes the fixed foreground source
`073496c235da5b975ee10806e0b9477fbea99489`. Four additional real-Inbox PostgreSQL
cases pass with controlled intent/provider ports: two accounts/non-mail profile,
replacement during provider I/O, foreign provider response, wrong namespace,
unusable permission, transport failure and restart of pending disable. Its RED
removes the coordinator implementation; the initial compile also found an invalid
`TemplateRef` conversion, which was fixed before the isolated missing-constructor
RED was rerun. No assertion or production owner was weakened. These cases do not
prove provider OAuth, actual M44 transport, HTTP mount or Hong Kong acceptance.

## Composio production adapters

`composio/subscriptions.rs` implements both ports using the existing M44 proxy,
project client, configured auth configs and PostgreSQL request budget. Assembly
can call its component-local `coordinator(client, typed_owner, inbox)` and reuse
the same typed M44 proxy. `Composio::subscriptions` is the explicit public
composition entry. Neither starts another service or accepts a caller's key,
namespace, upstream URL, trigger type or filter configuration.

Intent reads use the existing signed `Intent` operation. Connection/Metadata and
Intent must agree, and issuer routing before/after the owner reads must match.
A generic HTTP404/transport failure is not interpreted as user opt-out; only the
typed owner's explicit deleted state yields `None`.

Provider operations use the [v3.1 trigger API](https://docs.composio.dev/reference/api-reference/triggers).
The client lists with exact account/auth-config/user/type filters, checks every
returned item, follows bounded cursors, and refuses ambiguous matches. The
same-configuration upsert always names the connected account, then its returned
trigger ID is independently resolved before registration. Lost completion is
reconciled through listing, not blind creation. Disable preserves its target for
retries. The basic Outlook profile watches the default inbox without sender,
recipient, subject, importance or attachment filtering; another filtered trigger
is never silently reused. Documented materialized inbox/null defaults are accepted.

Seven actual loopback project-HTTP cases pass with synthetic provider responses,
including exact-account creation, repeat/unknown reconciliation, wrong-user
refusal, paginated ambiguity and avoiding filters that drop ordinary mail.
Initial transport cases failed; after adding diagnostics and explicitly excluding
loopback from the test process proxy, all seven pass without relaxing assertions
or production TLS. Focused library Clippy and workspace formatting pass. The
controlled project uses a synthetic key and namespace; these tests do not prove
actual issuer/M44 transport or real provider authorization. HTTP/Prepared mounting
and Hong Kong acceptance remain consumer work.
