# Scheduling rules

`adx-scheduling` contains read-only Filter/Score rules and their composition. It uses `adx-core` contracts and `im` persistent collections. Global chooses a Domain by round robin; Domain selects nodes and reserves resources; Node Manager performs final local admission.

## Modules and static registration

| Module | Responsibility |
|---|---|
| `adx-core/src/scheduling.rs` | Typed selectors, hard/soft policies, device inventories, card allocations and DeviceLedger |
| `src/lib.rs` | Filter/Score interfaces, immutable cluster snapshot, profile validation, weighted selection |
| `src/plugins.rs` | NodeAvailable, ResourceFit, ResourceBalance(Pack/Spread) |
| `src/constraints.rs` | DeviceFit, NodeAffinity, InstanceAffinity, Topology; NodePreference, InstancePreference, TopologyPreference |
| `master/src/domain.rs` | Queue ownership, framework call, atomic scalar/card reservation and release |
| `node-manager` | Fresh inventory check, local scalar/card reservation, sandboxd device mapping and confirmed cleanup |

Default filters, in order: `node-available → resource-fit → device-fit → node-affinity → instance-affinity → topology-spread`. These hard filters apply to every profile. Default scores: resource balance, node preference, Instance preference and topology preference, each with weight 1.

`Master::new(domain_count, placement)` installs that profile. `Framework::new(additional_filters, weighted_scores)` / `Master::with_framework` allow static Rust composition. Only eligible candidates are scored. Scores must be in `0..=MAX_SCORE` (3,000,000). Higher weighted sums win; ties use ascending node ID. Empty scoring configuration uses node ID and therefore disables soft preferences. Invalid names/weights/scores are rejected. Plugins must not mutate reservations or perform blocking network requests. Errors preserve queued work and do not consume capacity.

## GPU/NPU whole cards

Requests carry kind (`gpu`/`npu`), optional exact model and positive whole-card count. The node inventory supplies kind, model, node-local u32 ID and health. IDs are unique within a kind, so GPU 0 and NPU 0 are distinct cards. No fractional allocation, sharing or virtual partitions are modeled.

Model-specific requests are matched before wildcard requests; a wildcard cannot consume the only card satisfying a narrower request. Card selection is deterministic by kind/ID. Domain commits CPU/memory/disk and concrete cards together. Pending starts already consume cards. Inventory refresh does not erase reservations, including reservations of cards that temporarily disappear or become unhealthy. Release requires the matching assignment generation and confirmed lifecycle cleanup.

Assignment carries the concrete IDs/model. Node Manager checks the assignment against the request and fresh local inventory, reserves locally, and forwards IDs grouped as `gpu`/`npu` in sandboxd `StartRequest.xpu_allocations`. Failed starts retain all reservations until cleanup succeeds. CPU-only instances do not require an accelerator inventory; accelerator requests require a nonexpired inventory supplied through `update_devices`.

## Node and Instance affinity

Selectors support exact labels and In/NotIn/Exists/DoesNotExist/Gt/Lt expressions. All expressions within a selector are AND. Required node selectors form OR alternatives; empty required-node policy means unrestricted. NotIn matches a missing label; numeric comparisons require a valid integer node value and exactly one integer operand.

Required Instance affinity terms are AND: each needs a matching peer in the candidate's topology value. A self-affine group can bootstrap a term only when no existing matching peer exists anywhere and the incoming Instance itself matches it. Required anti-affinity rejects matching peers in the candidate's topology value. Hard terms reject missing topology labels. Existing peers' hard anti-affinity also constrains incoming requests, even when the newcomer has no anti-affinity policy.

Peer selectors default to the same tenant. An explicit tenant list targets those tenants; request authorization for cross-tenant policy belongs at API admission. Soft node and peer preferences are weighted, normalized scores. Soft anti-affinity awards matching nodes without conflicting peers, with zero reward for missing topology labels. These are scheduling-time rules: label changes do not evict running instances.

## Topology spread

Each spread term defines a topology key, same-tenant Instance selector, positive `max_skew`, positive `min_domains`, and DoNotSchedule or ScheduleAnyway.

Eligible topology values come from available nodes matching the request's required node affinity. Free resources and free cards do not change topology-domain eligibility. Counts include matching allocated Instances, including pending starts, using current node topology labels. Hard spread checks the candidate count after placement minus the global minimum against max_skew. When eligible values are fewer than min_domains, the minimum is zero. A missing candidate topology label fails the hard check. Multiple hard constraints are AND.

ScheduleAnyway prefers less-populated values but does not reject placement for skew; missing topology labels receive zero score. The default profile combines this preference with other scores, rather than claiming it overrides every other preference.

Master incrementally publishes a coherent snapshot across all embedded Domains. Bounded rounds share its persistent roots; each reservation updates the view before the next request. Peer and spread checks see all recorded assignments. Selection remains within the Domain chosen by Global; these rules do not add cross-Domain retry or migration. Both queues and assignment ledgers are currently in memory and must be restored before exposing a restarted service.

## Contract and configuration

`control.proto` carries typed policies, node labels/inventory and concrete allocations. Rust conversions reject unknown enums, missing constraint submessages, invalid selectors and duplicate allocations. The policy is available through Rust and internal gRPC contracts. CLI configuration, external Sandbox API field mapping and physical collector discovery are part of the remaining service assembly, not supplied by this scheduling library.

Example `InstanceSpec.scheduling` JSON representation (the protobuf fields model the same structure):

```json
{
  "labels": {"app": "worker"},
  "devices": [{"kind": "gpu", "model": "model-a", "count": 1}],
  "required_node": [{"match_labels": {"pool": "accelerated"}}],
  "preferred_node": [{"selector": {"match_labels": {"storage": "local"}}, "weight": 10}],
  "required_anti_affinity": [{"selector": {"match_labels": {"app": "worker"}}, "topology_key": "host", "tenants": []}],
  "topology_spread": [{"selector": {"match_labels": {"app": "worker"}}, "topology_key": "zone", "max_skew": 1, "min_domains": 2, "when_unsatisfiable": "DoNotSchedule"}]
}
```

Nodes supply the corresponding pool/storage/host/zone labels. `labels` on the Instance are peer/spread labels; they are distinct from node labels.

Validation covers models, healthy inventory, exclusivity and release, local admission/failed cleanup, hard/soft affinity, self-bootstrap, reverse anti-affinity, tenant scope, cross-Domain snapshots, hard/soft spread, invalid policies, typed protobuf round trips and the actual UDS gRPC adapter's device payload. Tests use synthetic inventories and a sandboxd protocol fixture; physical GPU/NPU execution and full-platform Buildkite E2E are separate validation gates.

## Scheduling hot path

The migrated optimizations are documented with baseline provenance, configuration, event-loop integration, tests and measurements in [scheduling performance](../../../docs/testing/scheduling-performance.md).

- `snapshot.rs`: persistent node/Instance maps, tenant/label indexes and reverse anti-affinity membership. Snapshot fields are read-only to callers; `update_node/place/remove` update the indexes together. Holding an old snapshot does not expose later changes.
- `query.rs`: prepare peer queries once per request, using the narrowest exact-label/tenant index and evaluating remaining selector expressions. Plugins share the prepared view across candidates.
- `master/src/journal.rs`: bounded node-mutation sequence; caches read only the new suffix, refresh absolute reservation values and rebuild on overflow.
- `master/src/domain.rs`: semantic computation groups and ranked candidate reuse. After a reservation/release/update, re-evaluate changed nodes and reposition their scores, including Spread. Unknown plugin profiles and non-scalar policies use normal per-request evaluation.
- `master/src/queue.rs`: ordered priority/FIFO trees per tenant, round-robin across tenants, retained tickets for deferred requests. No full backlog drain on each assignment.

These changes do not add distributed queues or persistence. The event-loop API is `take_ready_domain` + `schedule_round`; `RoundOutcome.error` can coexist with successful assignments, which the caller must still dispatch. Topology expansion is outside this optimization iteration.
