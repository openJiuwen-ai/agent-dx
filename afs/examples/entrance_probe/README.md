# Controlled USRBIO probes

`../usrbio_pipeline_probe.rs` runs the final same-RAM comparison. It uses the
product SDK in `sdk` mode and the frozen `native.rs` client for controlled
minimum-ring modes (`wire_async`, `wire_pipeline_async` and blocking diagnostics).
The reference is derived from d24c158; it is test code for a bounded, fully
verified workload, not a second supported SDK or cancellation-safe public API.

The final runner, matrix, payload checks, source/binary identities and results
are linked from `docs/testing/2026-10-25-usrbio-self-optimization.md` at the repo
root. Use the async reference for an equivalent notification contract. Blocking
modes and same-process copy diagnostics have different boundaries.

`../entrance_probe.rs` is the preceding same-Backend FUSE/USRBIO diagnostic.
`direct_hook.rs` belongs to the prior backend/sync ablation; its numbers are not
durability admission results. These helpers are never invoked by a normal product
build or benchmark mode.

Examples and helper scripts are test artifacts; default AFS release packaging
builds product bins only. Do not deploy patched ablation copies.

For POSIX-fd admission, `entrance_probe` also accepts `sdk_fd` alongside `sdk`.
Both use the same in-memory Backend, data, rings, block size and depth. The server
mount exposes the product fd bridge. The fd mode uses a POSIX open plus
`register_fd`, the path mode uses SDK open/register. JSON separates setup, hot IO,
sync, cleanup and the full lifecycle; compare paired read/write rounds at Q1/Q8.
An optional final argument (1..3, default 1) repeats IO within the same session;
3 reports first pass and the mean of passes 2/3 separately. Full lifecycle uses
one wall clock and includes all passes and in-client read validation.
This is a frontend nonregression check, not a claim about durable DFS throughput.
