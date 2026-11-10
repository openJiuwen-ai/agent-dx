# USRBIO self-optimization validation (2026-10-25)

## Result and boundary

The combined USRBIO optimization and AFS gate-fix changes satisfy the local
engineering gate below. The SDK's value is already established; durable AFS
end-to-end performance is not this validation's admission criterion.

The product path remains application slice -> shared IOV/SQ/eventfd -> Node
Backend callback -> CQ/async wakeup -> application result. This is a single-node
Linux RAM-backend comparison, not disk throughput or a 3FS head-to-head result.

Large opportunistic write waves (at least 8 MiB) now copy/submit 4 MiB banks so
Node consumption overlaps preparation of later banks. The entire wave retains
its block ownership until every distinct completion is known. Must-fill and
single-request behavior is preserved. Reads use a shared lazy pool of at most
four workers only when sampled callback residence exceeds 250 us per rounded-up
MiB, the Backend permits concurrency, and IOV ranges do not overlap. Read groups
join before an intervening write. Fast callbacks retain the serial path.

Completion validation rejects duplicate/missing/oversized results before block
reuse or read-output publication. EOF returns zero. Zero-push timeout/cancellation
recycles reservations; an accepted prefix with unknown outcome retains the whole
wave, including during staged writes.

## Five-round result

Baseline: `d24c158c186e4358b886228a8578ebe2cce7b1b6`, with the same final fixture.
Candidate and baseline binary identities, source snapshots and all raw records
are pinned in the evidence branch linked below. The VM used 8 vCPU/12 GiB,
client CPUs 0-1, Node CPUs 2-7. There were 19 cases x 5 rounds x baseline/candidate/
minimum-async-reference, 285 samples and 855 passes. Each sample reused one IOV
and ring for three passes. Steady state is the mean of the last two passes;
first pass, setup, cleanup and the complete three-pass component session are
also retained. Round order: BCR, CRB, RBC, BRC, CBR.

Payloads were nonzero pseudorandom data. Each write pass changed seed, each read
output was poisoned before use, and complete content/Backend bytes/call counts
were verified. Actual Backend transfer totaled 601.875 GiB. Host swapouts did
not grow. No slow samples were replaced or retried to pass the threshold.

| 1 GiB write case | Baseline steady ms | Candidate steady ms | Paired median time reduction | Wins |
| --- | ---: | ---: | ---: | ---: |
| 1 MiB / Q8 | 54.14 | 46.87 | 10.91% | 5/5 |
| 1 MiB / Q32 | 52.64 | 41.89 | 22.27% | 5/5 |
| 4 MiB / Q8 | 47.36 | 42.51 | 10.08% | 5/5 |
| 4 MiB / Q32 | 48.60 | 39.58 | 18.58% | 5/5 |

These paired reductions are not obtained by dividing independent medians.
The largest bulk candidate gap to the same-contract minimum async reference
was 5.66%, within the 10% engineering target. That reference bounds client
incremental cost; Node scheduling was also checked through serial/fixed-pool/
adaptive diagnostics and actual concurrency. This does not prove an absolute
hardware optimum.

Tradeoffs are explicit: bulk-write total CPU time increased about 27-64%, also
seen in the minimal pipelined reference. The complete 4 MiB/Q8 three-pass
session improved 9.75%, below 10%; the other three bulk-write sessions improved
12.85-21.02%. Plain RAM reads and Q1 were essentially unchanged. The worst
small-block paired regression was 3.59%, below the 10% protection threshold.
One first-pass RAM read transiently enabled the pool and returned to serial
on both steady passes; its original result is retained.

A separate same-RAM model with a configured 1 ms delay per 4 MiB read measured
461.75 -> 139.25 ms, paired reduction 70.22%, 5/5 wins, peak concurrency 1 -> 4.
It validates USRBIO's execution capability; it does not predict real disk or
network gains. Payload generation/verification is outside each IO window in
all arms; all IO copies remain inside. The component session includes setup,
three transfers and cleanup, not the entire synthetic-data fixture runtime.

## Regression and official gates

On the final combined tree, Linux `make afs-check ADX_AFS_ALL_FEATURES=1 JOBS=2`
passed without extra skips: logs total 50 suites / 1915 passed / 0 failed /
54 existing explicitly ignored tests. `make rust-check JOBS=2` passed too;
that target covers non-AFS workspace policy checks while `afs-check` covers
AFS default/all-features lint and all-targets tests. The ignored performance
and device cases are not claimed as executed.

Focused SDK (25 unit + 2 example), Node USRBIO (28), transport (38), cancellation,
partial publication, EOF, malformed completions, read eligibility, IOV overlap
and read/write order checks passed. The final AFS gate also includes the RPC
integrity classification and error-code catalog repairs described in its
separate commit. Remote CI/CLA/merge are independent of these local results.

## Reproduction and evidence

Build current RAM probe on Linux with `cargo build --release -p afs-tools --bin
usrbio_pipeline_probe`. See `afs/tools/probes/entrance_probe.md` for the
frozen reference and earlier diagnostic helpers. All executable probe code is
kept out of the default release binaries.

- [Immutable evidence navigation](https://gitcode.com/luozhancheng/agent-dx/blob/a2cf95f14627844c9092894a097547ee64a314e6/README.md)
- [Full five-round report, per-round results and raw archive](https://gitcode.com/luozhancheng/agent-dx/blob/a2cf95f14627844c9092894a097547ee64a314e6/features/usrbio/out/self-opt-20261025/REPORT.md)
- [Official green-gate fixes and complete logs](https://gitcode.com/luozhancheng/agent-dx/blob/a2cf95f14627844c9092894a097547ee64a314e6/features/usrbio/out/green-gate-fix-20261025/REPORT.md)
- [Publication manifest and omitted-artifact checksums](https://gitcode.com/luozhancheng/agent-dx/blob/a2cf95f14627844c9092894a097547ee64a314e6/MANIFEST.json)

The evidence branch retains earlier failed/rolled-back candidates and build
identity incidents as historical records. Those cohorts must not be merged
into the final statistics. Compiled binaries and payload datasets remain local,
with checksums in the manifest.
