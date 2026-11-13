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
