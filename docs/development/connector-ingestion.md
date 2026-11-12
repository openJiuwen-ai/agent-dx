# Gateway ingestion coordination preparation

This is the second responsibility module within existing draft ADX #268. The
configuration center (#179), trusted Composio adapter and ingestion coordinator
all belong in the existing DigitalMateGateway process outside user sandboxes.
No separate deployment/service or additional coordinator PR is introduced.
DigitalMate #1192 is the separate-repository consumer and remains independent.

This batch's source is based on official master
`f2e2c0c735f23ac16fe86a5140f1eab03602ed6b`, incorporated without force-pushing
or copying #179 WIP. The top-level architecture and ingestion-design documents
are maintained by the coordinating chat and are not changed by this batch.

## Implemented bounded slice

The Composio adapter owns `composio/webhook.rs`: trusted secret rotation overlap,
HMAC-SHA256 verification on original request bytes, exactly one required header,
300-second past/future window, signature version and size limits. It accepts the
documented V3 trigger envelope. Lifecycle/project events and other envelope
versions return an explicit unsupported-envelope error; no handler currently
acknowledges or discards them. HTTP assembly must route them separately or retain
them according to an explicitly accepted policy. Verification is not delivery
deduplication and is never a durable reception acknowledgement.

The coordinator owns `ingestion/`: binding a verified envelope to an explicit
current subscription projection; exact matching of user/account/auth config and
trigger; connection/subscription and Binding generations; refusal of disabled or
stale inputs. Signed provider metadata cannot by itself select a workspace.
The projection is a candidate internal input, not a credential or a second
connection registry; real public owner reads remain required at every phase.

`prepare_batch` examines the full supplied pending set rather than advancing a
sequence high-water mark. It preserves different event IDs even with identical
mail text/thread, absorbs only exact ID/digest duplicates, refuses conflicting
replays, limits a batch to 50 events and 1 MiB of raw event payload, and reports
remaining distinct inputs. The stable manifest binds scope, target, generations
and exact event IDs/digests. A store must atomically claim and persist this exact
manifest before dispatch; a retry must use that claim rather than rebuild a batch
against changed pending rows. These pure rules are not a scheduler or queue.

The coordinator has no Composio SDK/client, project API key or signing secret.
Verification stays with the trusted adapter. Future trigger management and
background reads must consume purpose-scoped restricted ports on that same
adapter and its shared budget; they must not create another account/permission
layer. The current foreground-only Scope does not authorize background calls.
#179's exact published secure configuration/identity contract must be consumed
when available; this batch assumes no arbitrary service-secret distribution API.

## Remaining implementation and activation gates

| Responsibility | Current state and receiving owner |
| --- | --- |
| Trigger subscription create/update/disable and unknown outcomes | NOT IMPLEMENTED; add restricted operations to the existing Composio adapter after pinning current schema/version. Coordinator stores its own subscription intent/projection, not connection authority. |
| Durable webhook receive and exact replay protection | NOT IMPLEMENTED; dedicated ingestion PostgreSQL schema/transaction and capacity checks, commit before HTTP success. No successful acknowledgement API exists in this slice. |
| Due polling, leases, fair budget, bounded concurrency and lost-wakeup prevention | NOT IMPLEMENTED; persistence/claiming and recovery tests required. Batch preparation does not establish these. |
| Existing Binding-only ADX readiness and Relay delivery | NOT INTEGRATED. Current AgentV2Access selection can initialize/reselect bindings and is not automatically a fixed-generation background capability. Publish/consume the required narrow semantics, without private selection fields or lifecycle control. |
| Sandbox durable batch acceptance/status and lost-response reconciliation | NOT PUBLISHED/INTEGRATED; consumer owner must persist before accepted and deduplicate immutable batch ID/digest. |
| Revocation, restart recovery and two-Gateway races | Local immutable projection tests only; real owner/database/worker/ACK scenarios remain NOT_RUN. |
| Identity and secure project configuration | #179 is ready for review but unmerged at this batch's check, head `4f8c343276a9e536cfcfef7ecb508b8fec1002f8`. No changes to its identity/assembly/private credential state. |
| Actual trigger/OAuth/paused-sandbox/report acceptance | NOT_RUN; no account creation, deployment, shared-device use or merge authorized/performed. |

The same PR may progress these responsibilities in reviewable commits. The 26
existing on-demand controlled HTTP tests do not establish ingestion acceptance.
New tests cover only the actual verifier and coordination rules, including an
independent Python HMAC vector and a controlled non-mail trigger. No real
PostgreSQL, subscription owner, ADX activation or sandbox acceptance participates.

The prior Windows full-Gateway build blocker (Unix-only observability) remains
open. Direct-source module validation is scoped evidence, not a full-target or
deployed result. Code and checks follow [the trusted adapter guide](composio-gmail-trusted.md).

Official signature/envelope reference checked 2026-11-12:
[Composio receiving events](https://docs.composio.dev/docs/setting-up-triggers/subscribing-to-events).
The secret is used as raw UTF-8 bytes as documented there; this is not a generic
Svix secret decoder or a promise to support undocumented webhook versions.
