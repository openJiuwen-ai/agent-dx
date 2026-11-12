# Coordinator autoscaling HTTP

Coordinator provides the external autoscaling controller with an optional HTTP
management listener. Configure `autoscaling_listen` in the Coordinator service:

```json
{"autoscaling_listen": "127.0.0.1:19092"}
```

Omitting the field disables this listener. The listener uses plain HTTP and
performs no authentication or authorization checks. The configured address is
its direct management entrypoint; examples bind loopback. Internal component
RPC retains its configured transport security.

[OpenAPI contract](../../platform/api/openapi/autoscaling.yaml)

The same isolated listener supports [node-upgrade management](../../platform/api/http/node-upgrades.md).
Scheduling pause remains separate from execution fencing and strict empty evidence.

## Requests

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/global-scheduler/scheduling_queue` | Observe requests waiting for central scheduling |
| POST | `/global-scheduler/node/localschedulingstatus?node_id=NODE` | Persist administrative scheduling pause |
| DELETE | `/global-scheduler/node/localschedulingstatus?node_id=NODE` | Remove administrative scheduling pause |
| GET | `/instance-manager/query-tenant-instances` | Query authoritative Environment ownership |

```bash
curl --fail --silent --show-error \
  'http://127.0.0.1:19092/global-scheduler/scheduling_queue'
curl --fail --silent --show-error --request POST \
  'http://127.0.0.1:19092/global-scheduler/node/localschedulingstatus?node_id=node-a'
curl --fail --silent --show-error \
  'http://127.0.0.1:19092/instance-manager/query-tenant-instances?node_id=node-a&page=1&page_size=100'
```

All responses use ordinary JSON. Query values must be URL encoded. Unknown
routes return 404. Invalid, blank or duplicate query parameters return 400;
unknown Environment-query parameters also return 400. A missing node returns
404. Unavailable state or Redis returns 503, never a successful empty result.
Timeouts return 504 when a response can still be delivered. Errors have the form
`{"error":{"code":"...","message":"..."}}`. A lost write response does not
prove that the scheduling change failed; query the node again.

## Scheduling queue

The response contains `count` and `instanceInfos`. Each entry has `instanceID`,
`requestID` (both the Environment ID), `resources`, `enqueueTimeMs` and
`waitDurationMs`, plus `schedulingRequirements.runtimeClass`. The runtime class
is the resolved value in the queued Environment specification, including defaults
or snapshot inheritance. A candidate node must report that runtime in its
supported runtime classes. Values are open strings, such as `runc`, `runsc`,
`firecracker` or `kata`; matching runtime alone does not establish that a node
satisfies every scheduling constraint.

Times are decimal millisecond strings. Resource strings use
`cpu` in millicores (`1000m`), `memory` and `Disk` in MiB (`1024Mi`), and optional
`GPU`/`NPU` card totals. Device models and other placement constraints are omitted.

Coordinator returns at most 10,000 waiting requests, ordered by enqueue time and
ID. `count` is the returned entry count, not the complete backlog size. Assigned
requests leave this queue before runtime startup completes. The waiting queue
is in memory; an empty result after restart is not a durable demand history.

## Node scheduling pause

POST returns `{"status":"evicting","message":"success"}`. DELETE returns
`{"status":"normal","message":"success"}`. These responses describe the
requested administrative state. DELETE does not override pressure, expired
heartbeats or reconciliation admission.

Administrative pause survives heartbeats. New central allocations and local-first
ownership claims exclude the paused node. Manual resume and traffic-triggered
wake wait while the original node is administratively paused; removing the pause
allows the existing pending demand to proceed. Existing Running Environments
continue serving traffic.

Pause does not cancel allocations or lifecycle operations already accepted before
the pause. These may finish afterward. It does not migrate or delete existing
Environments and does not report drain completion. An autoscaler must inspect
existing ownership and finish its Environment handling policy before removing
the node. Local-only recovery data is allowed to be lost when a node is removed;
subsequent recovery requiring those bytes can fail.

## Environment query

| Query parameter | Contract |
| --- | --- |
| `node_id` | Optional exact authoritative Assignment node ID |
| `tenant_id` | Optional exact tenant ID; omit to query all tenants |
| `instance_id` | Optional exact Environment ID; combines with the other filters |
| `page` | One-based page number, default 1 |
| `page_size` | Page length, default 100, valid range 1–1000 |

Filters are applied before pagination. `tenant_id=system` filters the literal
`system` tenant. Results are sorted by Environment ID. `count` is the total
number matching all filters in this request's authoritative storage snapshot;
`instanceInfos` is the requested page. A page beyond the end is empty while
`count` remains the matching total. `epoch` and `revision` identify the observed
Coordinator session and control snapshot. Pages are separate observations, not
a retained cross-request snapshot; recheck ownership before node removal.

Example response (illustrative):

```json
{
  "count": 1,
  "instanceInfos": [
    {
      "instanceID": "tenant-task",
      "tenant_id": "tenant",
      "node_id": "node-a",
      "generation": 7,
      "revision": 0,
      "status": "pending",
      "resources_held": true,
      "invalidated": false,
      "restart_pending": false,
      "resume_pending": false,
      "recovery_pending": false
    }
  ],
  "epoch": 1,
  "revision": 42,
  "node": {
    "node_id": "node-a",
    "scheduling_paused": true,
    "accepting_allocations": false
  }
}
```

Reserved ownership with no node result is included as `pending`, revision 0.
Starting, Running, Pausing, Paused, Resuming, Deleting and Failed records remain
visible. Deleted records without held resources are omitted. `resources_held`
includes pending recovery reservations; `status` alone does not determine
capacity release. The response excludes execution environment variables and
checkpoint credentials.

With `node_id`, `node` reports the persisted administrative flag and effective
admission based on the recorded capacity admission, session, live heartbeat and
reconciliation state. Without a node filter, this field is omitted. Neither
zero allocatable capacity nor a paused administrative state proves a node is
empty. The caller decides how to handle remaining records, including Paused
Environments whose execution resources are released.

## Validation

The `on_demand` integration tests exercise the direct HTTP listener, storage
queries, pagination, parameter errors, Redis failure, and manual/traffic wake
under administrative pause. The `rpc` local-first pause regression checks that a
new create reaching a paused node starts on another node. They use real Redis
and local RPC with controlled runtime fixtures; they do not establish deployed
cluster or sandboxd readiness.

The targeted real-runtime driver is `ADX_FC_SUITE=autoscaling` with
`build/e2e/firecracker/transfer.py`. It exercises two Firecracker backends in
separate network namespaces on one KVM host, the installed SDK and the direct
Coordinator management listener. See [runtime acceptance](../../build/e2e/firecracker/README.md)
for the six required cases and strict evidence contract. This targeted suite is
separate from the complete platform profile.

On 2026-10-14, the targeted suite passed all six cases on the local ARM64 KVM
host using the uncommitted autoscaling worktree based on `8f88ebffec85`. Both
central and local-first queued creates completed after unpause; local claim
execution was observed. Manual resume and forwarded HTTP wake each remained
blocked for over three seconds with no physical VM, then preserved the guest
PID and file after unpause. Final authoritative ownership, both backend
inventories and MinIO business-object inventory were empty. The run used
1000m CPU / 128 MiB per instance and a 120-second node RPC budget. Evidence is
under `out/autoscaling/e2e/evidence-04/`; this establishes the targeted local
suite only.
