# Environment create ownership

`POST /api/environment/v1/environments` returns a confirmed-running result as
JSON inside the normal response envelope, or as the final SSE event:

```json
{"environmentId":"tenant-worker","status":"running","requestId":"create-request","newlyCreated":true}
```

`newlyCreated` comes from the node's creation result. It is true only when that
logical create confirms a new instance, and false when an existing successful
Environment ID or Request ID supplies a reused result. A cached response for
the same logical request retains its original ownership bit. Recovery after a
lost response may conservatively return false. `requestId` echoes the current
request and is not evidence of creation ownership.

Clients may automatically delete during local initialization rollback only
when `newlyCreated` is exactly boolean true. Reuse, attachment, missing or
malformed ownership must preserve the remote workload and close local clients.
This rule does not restrict explicit deletion or change detached lifetimes.

Successful Request ID receipts are scoped to the tenant and retained in Redis for
600 seconds from their first publication. Replays do not extend that deadline;
Redis and Coordinator restarts preserve it. After expiry, the Request ID alone
no longer identifies an earlier creation. An existing Environment ID still
reuses its current result; if the earlier Environment was deleted, a request
after the replay window can create a new Environment. Receipt expiry does not
delete an Environment or release its resources.

Reusable snapshot restores inherit scheduling reservations and execution limits
independently. An omitted or zero CPU, memory or writable-storage limit inherits
the saved limit; it must not become the smaller scheduling request. Explicit
positive limits remain subject to runtime compatibility and request validation.
Fresh creation retains the existing zero-limit behavior of following its request.

`inheritEntrypoint=true` is a fresh-create option, not a snapshot override.
Combining it with `snapshotId` is rejected with HTTP 400, matching the native
Python SDK. For snapshot restores, omit it or supply `false`: neither disables
the captured image-process metadata. The Coordinator inherits the saved flag
and required rootfs metadata so Execd resumes the original processes without
starting the entrypoint again. Execution-limit overrides above do not permit
changing this captured process geometry.

Internal normalized requests may already carry the inherited flag. A `true`
flag must match a source snapshot with the flag enabled; enabling it for a
snapshot whose flag is `false` is rejected. A `false` protobuf value is treated
as unspecified during inheritance because that field has no presence bit.
