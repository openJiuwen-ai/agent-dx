# Ordinary-write early replies and file-scoped barriers

Tracking: [Issue #62](https://gitcode.com/openJiuwen/agent-dx/issues/62).
Base: `c27efdb1bdefe4a366a124ffb7837695aad0ca03`.

An ordinary explicit-offset write to a local-owner native DFS file returns after
AFS owns the full input and has admitted it to bounded memory. The existing
backend then performs CRC, chunk construction, required replication, Catalog and
Meta work. `fsync`, flush/close, overlapping writes and read-after-write retain
their ordering/error dependencies. See [ownership and failure contract](afs-write-owned-admission.md).

This package removes unrelated file waits and fixed-local-answer barriers. It
keeps one shared FIFO body queue and one consumer, with 2GiB normal admission and
an additional 32MiB owned/unacknowledged reserve. It does not add fairness
scheduling, region parallelism, payload zero-copy, a new switch or a backend
persistence shortcut. Memory is charged on demand; downstream buffers and page
cache remain additional. Acknowledged unsynced writes can be lost on a crash.

## Measurement contract

The comparison baseline is the frozen old-barrier candidate enlarged to the same
2GiB normal capacity. It already replies after enqueue. It is not clean upstream,
so this comparison measures the file-barrier package, not the entire gain of
early replies over upstream. Both final versions remove the same experiment-only
per-request probes. CRC, replicas, object size, TLS, native buffer budget and
backend concurrency stay fixed.

The reproducible workload is [write_delivery_bench.c](../../afs/tools/probes/write_delivery_bench.c):
compile explicitly on Linux with `gcc -std=c11 -O3 -pthread -Wall -Wextra -Werror`.
It is a test helper and is not a product/release binary. The driver uses one
process and two worker pthreads with an internal barrier and common
`CLOCK_MONOTONIC` T0. Input buffers and a committed 64KiB fixture are prepared
before T0. Both workers open their files inside T0. One writes 1024 x 1MiB to a new
1GiB file and fsyncs/closes it. The other overwrites the same existing 64KiB file
32 times and fsyncs after each write, then closes. There are no inserted sleeps or
external start signals. These are two files, not 32 independent tiny files.

Report T0 to large write delivery, large fsync/close, small final close, and both
thread joins for every trial. Small syscall durations include their waits;
concurrent waiting intervals must not be added into wall time. The primary is
T0 through both joins. Sampling sums sibling cgroup CPU once, includes app memory
and page cache, and records backlog as a sampled lower estimate. Missing device
I/O and private write-CRC scan counters stay missing rather than zero.

The single-VM cloud165 namespaces share a disk. Required replicas=2, source and
receiver use 4CPU/8GiB per node, Meta uses the same node budget, native dirty budget
is64MiB and upload slots=8. B prefetch is off; write CRC on, remote read CRC off,
transport encryption off with existing certificate authentication retained. This
is a product/FUSE test, not independent physical-disk scaling proof.

## Validation

Final production Rust source passed Linux fmt, 18 front-queue regressions,
1704 AFS library tests (39 existing ignored), 10 public contracts (17 ignored),
strict workspace/all-targets/all-features Clippy, release, and OwnerFS-only
library/binary compilation. The new regression explicitly preserves cross-file
FIFO arrival order. Local gates do not certify remote CI. Exact final runtime and
comparison evidence is recorded below.

## Historical samples stay separate

The earlier instrumented same-process six-trial cohort used the same workload:

| Pair | Old barriers: all done (s) | File barriers: all done (s) | Old small done (s) | File barriers small done (s) |
| --- | ---: | ---: | ---: | ---: |
| 1 | 8.343264 | 7.875854 | 0.522811 | 7.875827 |
| 2 | 8.279890 | 7.691527 | 0.507588 | 7.691505 |
| 3 | 8.544474 | 8.104768 | 0.516003 | 8.104736 |

All three whole-workload pairs were faster by5.15%–7.11%, while the small thread
finished much later. This tradeoff is retained, not hidden by median syscall
latency or described as proof of a new scheduler requirement. The older two-process
cohorts had injected10ms gaps and a pre-opened small fd; their figures are not
merged into these pairs. The first observer batch used asynchronous log timestamps
and cannot support internal phase attribution; its full elapsed/resource records
remain retained separately. None of these old cohorts is relabelled as a new
production-binary result or a clean-upstream comparison.

## Final production-binary comparison

All six fixed positions completed, order B/C, C/B, B/C. All times below are
seconds from the same in-process T0; last column includes both thread joins.

| Pair | Version | Large write delivered | Large fsync/close | Small32 operations/close | Whole workload |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 | baseline | 1.214127 | 8.373452 | 0.533653 | 8.373519 |
| 1 | candidate | 0.711140 | 7.349046 | 7.752656 | 7.752686 |
| 2 | baseline | 1.195370 | 8.254346 | 0.518696 | 8.254386 |
| 2 | candidate | 0.720078 | 8.065496 | 8.448514 | 8.448546 |
| 3 | baseline | 1.211010 | 8.452363 | 0.533672 | 8.452390 |
| 3 | candidate | 0.711758 | 7.377698 | 7.769389 | 7.769423 |

The complete pairs change by **−7.41%, +2.35%, −8.08%** (paired median−7.41%).
Large write delivery changes by a paired median−41.23%; its endpoint is memory
admission, not persistence. Whole-workload speedup is smaller and one pair is
slower. Candidate whole-time max/min spread is about9%, so do not promise a
stable fixed percentage or extrapolate to all workloads. All trials, including
the slower pair, are retained in [the full trial ledger](afs-write-early-reply-20261029.csv).

The small thread completes at0.519–0.534s for the baseline and7.753–8.449s for the
candidate. The body FIFO and consumer count do not change: AFS lets other files
enter earlier, but a tiny body may now arrive behind a large admitted prefix.
The application thread then waits at fsync. This is a completion-order tradeoff,
not a new per-file scheduler or a claim that a queued request alone costs CPU.
No fairness policy is introduced to change these outcomes.

CPU per main1GiB (plus the fixed2MiB small writes), summed once across sibling
cgroups, changes by paired median−2.16%; sampled total memory peak by−0.68%.
Each trial has1056 FUSE writes and1075838976 input bytes, the same measured
builder-copy bytes and required-replica receive bytes, and288 completed native
uploads. Front owned-copy bytes have the same derived total; this is a source
accounting inference, not an added private copy counter. Exact private write-CRC
scan counters and device io.stat are unavailable and are not reported as zero.
App-level timestamps for all192 small operations, original metrics, resource
samples, physical-object SHA checks and all historical cohorts are retained in
the task evidence. Current formal binaries have no per-request experiment probe;
older internal phase observations remain a separate evidence cohort.

## Final real-FUSE qualification

- Two handles, overlapping writes across a boundary, read-after-write,
  truncate/extend and original append/synchronous paths passed.
- A3GiB ordinary write actually filled the2GiB normal window and used1MiB of
  unacknowledged reserve: charged peak2148532224bytes, normal2147483648,
  total bound2181038080. Concurrent fsync still advanced; an independent file's
  fsync completed before the large writer finished. Full3GiB SHA matched on both
  mounts and all physical replicas before reusing only the new test bodies.
- Required-replica outage reported sync failure; close released its fd without
  advertising persistence. Recovery and restart matched durable witnesses.
  Failed-case state and all logs remain retained.
- Actual USRBIO POSIX-fd handoff parent,1GiB write child and restart child passed.
- Both final versions passed native/real-FUSE driver qualification. All six
  measured cases passed both-mount full content and physical-replica SHA checks.
  No OOM occurred; public configuration and frozen runtime inputs stayed fixed.

The source/ELF boundary is explicit: candidate node SHA256
`cf66bb6a2b2ebabf6b689970fb6ceedbab577e1ee7dd7e8d625a68cccde25753`, baseline node
`ab4e6e858855e35cc673e6cf919a78203e152759ac2d556fe5c28f726225d8e9`.
Candidate Rust/Cargo/proto inputs (655 files) match the final Linux gate; later
changes only refresh documentation and add the byte-identical qualified C helper.
Runtime evidence archive SHA256
`48dcfcb841a62877ccd492c8a634d733e5ab19f59ea86e203e05a4dcd3bdad8e`,968 files
verified by size/SHA. Cloud165 was normally returned with no owned process/mount;
source, cache, binaries, logs and failed states remain. No system configuration
change, private overlay, hardware replacement or old-body deletion was used.

Operational attempts are not hidden: a new workspace initially lacked the shared
Cargo cache, so an index refresh was cancelled and a cacheless offline resolution
failed with zero tests. After linking the existing cache, the locked offline
baseline gate passed1691 tests/39 ignored and release. No dependency versions or
AFS source were changed for that recovery. An initially reused workspace name was
moved to a distinct publication workspace; its old frozen source was restored and
old root binaries/run data were preserved.

Remote CI and upstream merge are separate admissions. Local gates and this fixed
workload do not establish all-backend, all-deployment, long-run or universal
small-file latency guarantees. Review the crash-loss window,2GiB per-mount memory
bound and FIFO completion tradeoff before upstream acceptance.
