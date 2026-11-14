# Ingestion subscription state and synchronization

## Owners and persistence

M44 owns user authorization, connection generations and saved background intent.
The PostgreSQL `Inbox` is the sole writer of its subscription projection, events,
batches and delivery receipts. A serialized binding or `enabled` flag grants
nothing. Provider I/O occurs outside Inbox transactions.

| Method | Contract |
| --- | --- |
| `Inbox::subscription(id)` | Read the existing validated projection, or None; no refresh or new grant |
| `Inbox::disable_subscription(expected)` | Compare complete persisted target under the workspace lock; changed target returns LeaseLost; identical disabled retry is idempotent |
| `Inbox::register(binding)` | Preserve generation/conflict rules; replacement retires old pending capacity and leases, never resurrects a fenced epoch |

Disabling retires only this subscription's pending events/ready batches and
invalidates its lease. Another account's lease in the same workspace survives.
The disabled projection is retained for retrying an unknown provider disable.

## Synchronization and HTTP entry

`SubscriptionCoordinator::synchronize(scope, connection_id)` reads actual owner
facts through `IntentPort`, invokes restricted `TriggerPort`, then writes through
Inbox. M44 Connection/Metadata/Intent must agree before/after I/O. The identity owner
produces the actual existing authority_ref; business code does not compare Binding
incarnations or persist execution ports. Typed deletion is opt-out; HTTP404, temporary unusability and transport
failure are not. Local stop fences before provider disable; failed cleanup stays
retryable. Existing same-generation conflicts require explicit rebind.

`composio/subscriptions.rs` supplies the production ports using the shared M44
proxy, project client, auth configs and PostgreSQL budget. It lists triggers with
exact account/config/user/type filters and bounded cursors, checks each item and
refuses ambiguous results. Upsert names the exact account; its returned trigger
is independently resolved. An unknown completion reconciles by listing rather
than blind creation. Disable retains the original target for retries.

The basic Outlook profile watches the default inbox without sender/recipient/
subject/importance/attachment filters; a different filtered trigger is not reused.
Documented inbox/null defaults are accepted. Provider mappings remain in the
adapter; the generic coordinator has no hardcoded email filters/toolkit policy.

`POST /api/digitalmate/connectors/v1/composio/subscription` accepts exactly
`{"connection_id":"..."}` with normal connector identity. It checks the current
grant/Binding around I/O and consumes the already saved M44 intent. Business,
privacy and delivery bearers are refused; JSON cannot set opt-in/account/target.
Success is `{"state":"enabled","binding":...}` or `{"state":"disabled"}`;
provider/owner failure remains an error. Missing ingestion is `not_configured`.

`PreparedComposio::bind` reuses its client, budget, typed M44 proxy and already
opened ingestion Inbox. It creates no second queue, pool, service or lifecycle
owner. Composio foreground reads do not depend on enabling ingestion.

## Failure and verification boundaries

Cross-owner/HTTP effects are not one transaction. Re-read current facts on retry;
the receiver independently checks live intent and exact Binding. Stop/replacement
cannot undo already sent provider I/O; no late result may grant a stale epoch.
After a consumer restart, a deleted connection absent from its cache/queue is not
automatically discovered; this code does not claim all such triggers are cleaned.

Ordinary tests cover strict HTTP/projection checks with controlled authorities
and providers. Explicit PostgreSQL cases cover full-target CAS, stop/reopen,
capacity retirement, two-account isolation, synchronization races, unknown effects
and cleanup retry. Formal startup tests exercise existing encrypted imports,
shared budget and Ingress bind/serve; they do not prove live OAuth or mailbox data.
See [Composio entry](composio-gmail-trusted.md),
[intake operation](connector-ingestion.md) and
[delivery contract](ingestion-delivery-contract.md).
