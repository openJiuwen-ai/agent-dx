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
| `Inbox::register_policy(binding, policy, revision)` | Consume owner facts; a newer rules-only revision preserves accepted batches; permission replacement retires old pending capacity and leases |

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

## Structural screening

The authenticated M44 `intent` request selects `screening_contract:1`; its reply includes `screening_policy` and
`screening_revision`. Missing fields are refused rather than silently ignoring
a user choice. The empty version-1 policy/revision0 accepts every otherwise valid event. A nonempty policy requires a positive
owner revision. Rules are bounded to16 unique IDs, exact `label`/`container`
values of at most256 UTF-8 bytes, and `suppress`/`quarantine` actions. There is no
JSON path, expression, regex, title/body/sender/thread or semantic classifier.
Deploy the compatible owner first: an old request still receives the old strict
reply shape. A new Gateway does not retry an unsupported owner request without
the policy contract and silently discard user choices.

M44 is the sole user-choice writer. Rules-only updates use a separate CAS
revision and preserve the read-permission `intent_revision`; otherwise a simple
filter edit would revoke already accepted work. Inbox updates the projection
under its existing workspace/subscription locks. Equal-revision disagreement and
rollback are refused. Existing batches, digests, leases and receipts are not
rewritten by a rules-only update. New rules affect subsequent first receipts
after successful synchronization; they do not retroactively delete prior input.
The owner save and Gateway synchronization are separate operations; owner save
alone does not mean the Gateway has installed the new rules.

Only the provider adapter projects event metadata. Gmail uses the schema's root
`label_ids`; an unrecognized field such as `labelIds` cannot supply facts.
Malformed values and missing facts remain
unknown. The actual Composio trigger schemas were read from the project on
2026-11-15: Gmail `20261105_00`, Outlook `20261112_00`. Outlook uses
`outlook_message.parentFolderId` or deprecated `message.parent_folder_id`, only
when that metadata's message ID matches the root event message ID; conflicting
aliases remain unknown. ID-only Outlook
notifications therefore pass to the sandbox without a filtering read. Values
are matched exactly, without case folding or guessing a well-known folder from
its opaque ID. Body, headers, promotional categories and unsubscribe text never
implicitly exclude an event. Unknown provider shapes accept by default.

Every first receipt persists action, stable reason, matched rule ID, policy
version/revision/digest with the original event identity/digest in the same Inbox
transaction. Exact replay returns that saved decision without reapplying rules;
same ID/different bytes remains409. Suppressed input keeps no body. Explicitly
quarantined input is held without scheduling, charged to the existing count/byte
and per-subscription capacity, and expires after `dedupe_retention_seconds`.
Permission revocation also clears its raw bytes. Retained receipt metadata is
bounded to100 times `max_pending_events`; exhaustion returns503 rather than
acknowledging unrecorded work. Quarantine wins if several explicit rules match.
Only accepted input sets a wake deadline and enters a batch.

Schema2 is required. Apply `schema.sql` for a new owner database. Existing schema1
requires stopped writers, settled-only wire-v2 projection upgrade where needed,
then the schema owner's [guarded SQL](../../gateway/src/ingress/digitalmategateway/ingestion/upgrade_screening_v2.sql).
It preserves terminal identities and defaults all existing subscriptions to
accept; startup does not migrate. No Hong Kong rollout is part of this change.

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
