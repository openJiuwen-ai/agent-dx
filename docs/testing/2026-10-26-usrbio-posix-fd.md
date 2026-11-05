# USRBIO POSIX fd registration validation (2026-10-26)

The feature validation cohort below is frozen at baseline `f7aaee0f` plus the
fd-registration changes. The submitted branch integrates `fb7ee076` master;
post-integration gates are recorded separately from the original RAM cohort.
This is single Linux ARM64/FUSE/DFS component evidence, not full ADX deployment,
physical-power-loss, multi-node, mTLS deployment, remote CI or a product storage
throughput claim. Publication and remote CI status are tracked separately by the merge request.

The [main example](../../afs/crates/client/examples/usrbio_round_trip.rs) now uses POSIX
create/open, `register_fd`, the existing rings, retained-original `sync_all`,
`unregister_fd`, then POSIX readback. [The path example](../../afs/crates/client/examples/usrbio_path_round_trip.rs)
retains the separate SDK open/register interface. Identity is the original FUSE
open, never a path re-open. Access bits and ring session are checked by Node;
current O_APPEND/O_PATH is rejected before RPC, including F_SETFL after open.
Do not change status flags or overlap POSIX data IO/mmap during the SDK phase.
See [the architecture contract](../architecture/afs.md#本机-sdk-usrbio-数据面).

The explicit Linux FUSE parent test passes: 1GiB nonzero distinct pseudo-random
blocks, prewarmed cached handoff, original-fd fsync, size/EOF, rename/path reuse,
permissions, retained dup after actual fd-number reuse, process-exit cleanup,
and a second OS process reopening persisted Meta/chunks. BLAKE3 is
`d864ae36bb6aad0a581dbfac75466a48a6c7ffc32c35b8007a30149e7f380ddd`.
There are no FUSE read/write callbacks during the SDK data phase. Run only with
/dev/fuse and a fresh owned test root; the default ignored marker is not proof:

```sh
CADX_FD_TEST_ROOT=/absolute/owned/test-root cargo test -p afs --lib -- --exact node::api::usrbio_e2e::usrbio_posix_fd_real_fuse_handoff --ignored --nocapture
```

SDK has 28 unit and 2 example passes. Original zero-enqueue timeout/cancel
reclamation and after-submission forfeiture tests pass. Official full AFS and
root Rust gates pass without additional skips (44 Cargo targets, 1921 passes,
0 failures, 58 ignored; nested subprocess summaries excluded). Four explicit
FUSE parent/child tests account for the new ignored markers and are executed by
the parent runtime run. Documentation checks are separate from runtime evidence.

The WAL gate race also has a narrow fix: only ENOENT of the exact sealed segment
already in CURRENT's durable retired set may be omitted from directory budget
accounting. Missing live/unknown entries, other IO failures, and present retired
capacity stay strict. A deterministic enumeration/deletion regression first
failed; it and negative controls now pass. The same 1ms readdir schedule probe
failed on both original and candidate binaries, then passes 10/10 after the fix.
Formal gates and performance do not load this probe.

Five paired rounds compare path SDK and fd SDK using the same RAM Backend,
1GiB data, 4MiB IO, Q1/Q8. Each session has three passes; steady time is the mean
of passes 2/3. Every read is fully verified, with independent write readback,
exact Backend byte/request counts and final hashes. Full lifecycle uses one
clock, includes setup/three passes/read checks/sync/unregister/close/destruction,
and excludes application data generation/backend prefilling. The prior declared
limit is paired steady median <=110% and at least 4/5 rounds <=110%.

| 1GiB / 4MiB | 稳态配对耗时变化 | 首遍配对变化 | 完整会话配对变化 | ≤110%轮数 |
| --- | ---: | ---: | ---: | ---: |
| 写 Q1 | +1.97% | +2.58% | +3.27% | 5/5 |
| 写 Q8 | +1.71% | -0.77% | +2.42% | 5/5 |
| 读 Q1 | +0.25% | -0.92% | +0.53% | 5/5 |
| 读 Q8 | -0.51% | -3.78% | -0.64% | 5/5 |

All four groups pass 5/5; full lifecycle's maximum paired median increase is
3.27%. This establishes admission of the new entry method under the stated RAM
cohort, not a universal limit or a new durable-backend speedup. Original and final
cohorts are retained separately. Final binaries have SHA256:

- FUSE test: `c61de7ea2da7e54ce943e7461bf159e222c79022171028a5fcc7a7384411e71d`
- performance probe: `503080c1419fbf36ce10662cae8b94f15dfb085d40a79fe3d5aebcf439395de9`

The maintained [probe](../../afs/tools/src/bin/entrance-probe.rs) accepts `sdk` and
`sdk_fd`, with optional final pass count `3`; its JSON separates first/steady IO,
setup, sync, cleanup and full wall time. Raw per-round output, exact commands,
source hashes, gate exits and failed-attempt history accompany the delivery.
