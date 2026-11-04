# Native remote read concurrency candidate

Status: this branch uses one native asynchronous read core and removes the native synchronous/asynchronous selector. The [consolidation delivery](unified-read-20261026.md) separates frozen stage17 evidence from current-mainline integration validation.

The semantic baseline is PR95 `b7894c4f13ca1bf161c336eca9b92f938f579c31`. Read CRC remains off and write CRC remains enabled. This change does not enable prefetch, disable TLS, change the FUSE library, or add a zero-copy interface.

## Configuration

`fuse_workers` controls executing dispatch workers. Optional `fuse_max_tasks` independently limits admitted work, including queued, active, asynchronously waiting and awaiting-reply tasks. When omitted its value remains `fuse_workers * 8`. An explicit smaller positive value is legal. The task-capacity CLI override is `--fuse-max-tasks N`. Native gRPC asynchronous reads are the only native remote-read execution model; no user switch selects synchronous versus asynchronous code.

Example (native DFS, gRPC; resource settings are optional):

```toml
fuse_workers = 1
fuse_max_tasks = 32
dfs_read_crc_mode = "off"

[dfs_read_pipeline]
max_rpcs = 4
max_reply_bytes = 16777216
max_disk_reads = 2
max_batch_ops = 4
max_pending = 128
task_timeout_ms = 30000
```

Defaults are RPC=4, reply bytes=256MiB, disk reads=2, group operations=4, pending=256, timeout=30000ms. RPC/disk/group limits must be 1..128, pending and task capacity 1..65536, reply bytes 8MiB..1GiB, timeout 1..60000ms. A and B need the same protocol capability, but their limits can differ; the sender caps a group to the advertised receiver limit. A keeps its existing eight service admissions, separate from its global disk-depth limit.

The native entry covers immutable and refreshed views, multiple objects, sparse ranges, CRC off/receiver/both (`both` is no longer configurable since S1a-7), the existing legacy-object disk cache and its prefetch, and dirty overlays. Cache and CRC rules themselves are preserved. A dirty read may still require the existing write-publication barrier; that write operation is adapted separately before asynchronously reading its resulting layout. Native gRPC owner-handle reads also await their transport. Genuine local file operations and external synchronous backends keep explicit synchronous adapters; native network waits do not use those adapters. Small ranges retain their actual length. An original request is published only after its complete result and final authority/handle checks.

## One request and its owners

The FUSE callback admits an owned dispatch ticket. Transport capability selects the native future interface; a stale view, cache setting, CRC mode or request shape cannot silently select a second native synchronous lane. If its read turn is already eligible, it starts the owned asynchronous route/Meta/RPC future outside dispatch locks, without first waking a synchronous worker. The starter only submits a future; it does not perform IO or wait. Reads queued behind an exclusive fh turn retain the existing dispatch release route. That task holds the fh turn and the shared inode read guard. It keeps only a descriptor while queued, then acquires reply capacity before allocation after selection. It uses a direct RPC when there is no backlog, or a bounded logical per-peer queue when busy. Each peer has one active batch/response stream; global RPC credit also remains bounded. Only already eligible compatible requests from the same open handle/context may share an RPC. No timer waits to collect a group.

A cached authority lookup performs local checks only. Cold authority establishment retains the original Meta-anchored setup lane. The existing peer-serve wait (default 20ms) remains a resource-admission limit. Data RPCs forward the remaining original deadline using grpc-timeout; processing, disk waits and response sending use that budget capped by task_timeout_ms. They do not inherit the admission wait, and retrying does not renew the original B deadline. The A response task reserves body/control capacity before requesting a globally shared disk permit. The blocking pool performs the real positioned read and returns its disk permit before asynchronous response sending. A bounded, ordered read window may overlap later ranges. Send pressure retains byte leases and the service lease, not disk workers. Cancellation drains admitted reads; a running blocking syscall is not assumed cancellable.

B decodes each range directly into its final private buffer, using the existing protocol decoder. No group body is concatenated. Header, completion, per-range length, ordering and final stream EOF are required before publishing any grouped result. Unsupported capability/context before body delivery falls back to the existing single-range protocol with the original deadlines. Body corruption/protocol errors do not publish partial replies. Source recovery remains on the original version and existing retry bounds.

A bounded completion queue carries data, ReplyData, inode guard and dispatch completion ownership to a reply executor. It does not acquire a second dispatch slot. The executor only writes the FUSE reply; it never waits for disk or network. Shutdown drains dispatch before retiring this executor. FUSE ASYNC_DIO is requested for the native asynchronous backend so that multiple direct reads already supplied on one fh can reach it; a serial application still produces one read at a time.

## Minimal programming pattern

Use the runtime's existing tasks, futures, semaphores and bounded channels. There is no new general-purpose thread pool or public scheduling framework. `ReadFuture`, `ReadLifetime` and the internal `ReadScope` express owned asynchronous work and propagate the original deadline to descendants; dispatch tickets and buffer leases remain AFS-specific because they carry file-ordering and resource contracts.

```rust,ignore
fn receive(request, reply) {
    let ticket = dispatch.admit(request.order_key)?; // counts until actual finish
    runtime.spawn(async move {
        let plan = prepare_with_async_metadata(request).await?;
        let body = byte_budget.reserve_then_allocate(plan.capacity).await?;
        let rpc = rpc_slots.acquire_owned().await?;
        let result = read_remote(plan, body).await; // suspends this task, no thread held
        drop(rpc);
        completions.send(Ready { reply, result, ticket }).await?;
    });
}
fn reply_loop(ready) {
    ready.reply.send(ready.result); // synchronous kernel write, no network wait
    drop(ready.ticket);             // close/write barriers can now proceed
}
async fn read_on_A(range) {
    let bytes = shared_bytes.reserve(range.capacity).await?;
    let disk = global_disk_slots.acquire_owned().await?;
    let body = spawn_blocking(move || {
        let _disk = disk;           // owned inside the real blocking operation
        positioned_read(range, bytes)
    }).await??;
    send_response(body).await       // last frame owner retains the byte lease
}
```

Production code additionally preserves absolute deadlines, errors, original open version/authority, cancellation acknowledgement and descendant work ownership. Dropping a waiting future does not prove its child or blocking operation ended. A descendant retains the same task lifetime, without creating another admission or reply. Errors before admission explicitly reply and own no dispatch credit.

## Observations and acceptance

`afs_dfs_read_pipeline_*{stage=...}` reports fixed-cardinality source IO, issued range RPCs, peer pending/queued tasks, owned reply capacity, A/B byte-budget and RPC-slot waits, disk admission waits, response production, admitted dispatch tasks, and completion-queue waits. `source_io` is positioned API calls, not hardware queue depth. `send_wait` covers the complete response-production scope including enqueueing; it is not pure network stall. `fuse_task` is a state observation including dispatch waiting and running work, so its duration counter is intentionally unused. Durations overlap and are not additive E2E wall time. Peaks are process-lifetime peaks; do not subtract them as if they were counts. Active byte credits and pool backing gauges describe overlapping ownership: native pool active plus idle backing capacity shares its configured total budget. Transport buffers, allocator overhead and application memory remain separate.

This candidate's validation and performance evidence is stored outside the source tree in the task ledger. Functional validation is not a performance claim; fewer RPCs or more overlapping reads do not establish stable E2E acceleration. Existing integrity/compatibility admission gaps inherited from PR95 remain listed in `read-path-update-20261025.md` and are not cleared by this refactor.

## Consolidation acceptance

The historical candidate06 binary is frozen with its pipeline enabled. It is compared with this source's single native execution model under the fixed 24-slot contract. Ordinary complete wall time, mixed slow-read/light-work progress, and timeout/retirement/recovery are separate acceptance dimensions. Passing a configuration or unit test is not a claim that the final candidate passed Linux FUSE or performance acceptance. See the task evidence ledger for the actual executed count, input identity, failures and stop reason.
