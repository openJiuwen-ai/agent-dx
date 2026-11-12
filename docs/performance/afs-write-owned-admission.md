# Ordinary DFS FUSE write admission

Ordinary explicit-offset writes to a local-owner native DFS file return after the
mount owns the entire request in its bounded input queue. This is the normal
behavior, with no enable/disable switch or experimental environment input.
`write()` success means memory ownership, not persistence. A process or host crash
can lose acknowledged writes which have not completed a successful sync.

`dfs_write_queue_bytes` (CLI `--dfs-write-queue-bytes`) controls normal admitted
payload per DFS mount. Its default is 2147483648 (2GiB), with range
8388608..1099511627776 (8MiB..1TiB), additionally checked to fit `usize`.
CLI overrides TOML, which overrides the default. All files share this allowance.
An additional reserve of min(normal/8, 32MiB) owns requests awaiting admission;
at the default, total payload capacity is 2080MiB. At most 4096 acknowledged body
entries are queued (failed retained work included, the executing entry excluded);
beyond that a write waits like a quota wait, with its reply withheld. At most
65536 withheld entries exist; each blocks one application write or one kernel
background request, so the bound is far above the kernel's in-flight requests.
A single FUSE write must fit the normal allowance; an oversized request returns
E2BIG before payload allocation. Consider the negotiated FUSE request size when
choosing a smaller allowance. The reserve never reduces the normal admission window. Executing
requests and failed retained bodies remain charged. Normal capacity and the entry bound backpressure the application by retaining its owned input and withholding the reply, while returning the receive callback. Only exhausting the reserve (total payload) or the withheld-entry bound returns EAGAIN before input ownership or success; allocation failure returns ENOMEM. A file's failure is reported to that file. Another file's write that must wait keeps waiting while draining healthy entries can still admit it; it returns EAGAIN only when retained failed bodies alone leave no room (their bytes or entries fill the normal allowance or the entry bound), rather than blocking until the failed file is synced or closed. `afs_fuse_front_write_admissions_total{filesystem,outcome}` counts writes acknowledged at once (`admitted`), after a wait (`waited`) and each rejection reason. Allocator metadata and downstream
DFS buffers are additional memory. This limit does not alter downstream dirty-body or idle-pool settings. Promotion retains the same Vec, with no additional payload copy. No per-file thread or 2GiB up-front allocation is introduced.

The FIFO body consumer preserves write order, including overlapping writes and
writes through multiple handles. It calls the existing complete DFS backend,
including write CRC, replication, catalog and Meta work. File callbacks fence only
the addressed inode; cross-file copy acquires both inode gates as one sorted set.
The inode gate is a reader/writer gate: reads, getattr, statfs, readlink,
directory reads, getxattr/listxattr and read-only opens share it and wait only
for that inode's admitted bodies; writes, resizes, syncs, xattr changes and the
remaining file callbacks hold it exclusively, and a waiting exclusive callback
fences new readers. Same-inode dispatcher turns include deferred read completion
across handles. Namespace changes retain a conservative mount-wide barrier that
also drains dispatched callbacks; a path LOOKUP excludes only namespace changes and
waits for admitted bodies, not for in-flight callbacks. Gates survive the real
dispatched operation, rather than just its enqueue. Fixed local replies retain
validation without acquiring these body/dispatcher barriers; a fixed local
getxattr (the kernel's per-write `security.capability` probe) is answered on the
receive thread.

Fsync/flush/release register parked recovery continuations outside normal body
and dispatcher admission. A file prefix settles with success or a recorded error;
it need not become clean before recovery can observe failure. A continuation holds
its file gate through actual backend completion. Ordinary control admission has a
4096-operation bound; RELEASE lifecycle capacity belongs to already-existing FUSE
handles and is not rejected by that ordinary bound. Ready continuations reuse
existing workers; no per-request thread is allocated and workers do not wait for
front-queue predecessors. Shared worker/disk service and the single body worker
can still queue unrelated work; no backend write concurrency is added here.

When no write bodies are pending, ordinary reads retain the existing concurrent
read dispatcher. Append, O_SYNC/O_DSYNC, remote-owner writes, kernel writeback and
OwnerFS retain their previous reply points; append/sync requests drain preceding
ordinary writes first. Runtime request flags are checked in addition to the open
handle capability.

A failed job retains its body and short-write progress. Later writes of the same
inode observe the error; another inode can continue. An explicit fsync/flush/close
retries that inode's retained work once, then executes the original backend sync
or close. Failure is not silently converted into success. Failed close still
releases its fd and retained bodies; other opens retain the inode error until a
successful explicit retry or their last close. Successful fsync/close keep the
existing persistence and required-replica guarantees. These guarantees require
checking the syscall result; unsynced write success alone is insufficient.

Fixed local replies do not wait for queued bodies, consume errors or trigger
retry. DFS's exact `security.capability` query keeps its inode validation and
original EOPNOTSUPP. Other xattrs and other backends retain their existing checks.
Unsupported locks/access and pure parameter rejections keep their old responses.
The USRBIO fd-control path is preserved and fenced before transfer, rather than
being disabled for the mount. Unmount drains runnable work, reports retained
errors, releases all captured bodies and joins the queue worker.

The frontend body queue remains one FIFO with one body consumer. This change
adds neither per-file body queues nor round-robin scheduling. Removing unrelated
callback barriers can change when requests arrive; small-file completion is not
guaranteed to improve. Record all file endpoints and the whole workload end,
rather than treating one endpoint's latency as a complete throughput judgment.

The package excludes zero-copy ownership redesigns, inline write shortcuts,
region parallelism, object-size changes, O_DIRECT/huge-page experiments,
persistence-barrier reductions and multi-host sharding. The per-mount normal
limit is charged on demand and is not an up-front allocation or a bound for all
process memory. Downstream buffers, allocator metadata and concurrent mounts add
to the memory requirement. The automatically derived waiting reserve never reduces normal
admission. There is no enable/disable switch; the normal queue size is configurable.

Historical cohorts, exact validation commands and final comparison results are
recorded in `afs-write-early-reply-20261029.md`. Historical 2GiB private-pipeline
results and different workload definitions must not be combined into one gain.
Local Linux/FUSE validation does not certify remote CI, all deployment modes or
long-running failure recovery.


## Reply scheduling and tuning

Ready ordinary-write admission results execute inline, after input ownership and
outside the body state lock. When normal quota is full, the receiver returns
without acknowledging the retained request; its callback runs when quota is
promoted, or reports the admission error. There is no separate reply queue or
reply worker. Backend processing remains asynchronous and `fsync`/`close` keep
waiting for the original completion and error barriers. Existing sync, append,
writeback and other-backend reply points are unchanged.

Size the normal allowance from available memory and the burst to absorb: if input
arrives faster than the body consumer can accept it, the difference accumulates
here. The body consumer releasing an input does not itself prove disk durability.
A larger limit absorbs longer bursts; it does not improve sustained disk throughput.
Several files share the same limit and can fill it together. It is not per-file,
not a maximum file size and not an up-front allocation. Executing and failed
retained bodies still count. Downstream live/idle buffers, protocols, allocator and
kernel memory must be budgeted separately; 2GiB is not an RSS cap or a claim of
optimal sizing. Keep headroom for those other users when tuning this parameter.


## Measurement observer isolation

Progress and trace output must stay in bounded memory during the measured region
and be exported after the business completion timestamp. A small same-device
progress `pwrite` can wait for inode timestamps and ext4/JBD2 journal buffer access,
even without `fsync`; byte count alone does not bound its latency. Include the
application and helper processes when tracing gaps before the next FUSE request.
Record any post-timing export separately. If same-disk progress output is part of
a real workload, name and account for that I/O explicitly rather than silently
calling it instrumentation.

The historical write cohorts included such observer I/O. Keep their raw timings,
but do not attribute the entire difference to AFS or retrospectively subtract an
estimated observer cost. With in-memory progress, the six-case reply comparison
did not reproduce a stable 7.5% slowdown: direct/thread group medians were
8.246182/8.245387 seconds, while paired changes had both signs. This does not prove
performance equivalence or a stable thread benefit. Keep the simpler inline-ready
path; do not import private tracing or reply-toggle environment controls into the
product. Functional gates and byte/copy evidence remain separately reportable.
