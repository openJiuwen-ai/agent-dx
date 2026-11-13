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
