//! Node fault layer. Normal paths only record and report (a `Result`, a
//! `CopyLifecycle` transition, a `GcPin`); the decisions those records feed
//! live here.
//!
//! - `classify`: named error classifications, one per question.
//! - `repair`: the replication-task loop (report quarantines, claim, copy,
//!   report).
//! - `reaper`: the GC collection loop.
//! - `lease_keeper`: background writer-lease renewal, plus `ensure_fresh` for
//!   the three main paths whose contract needs a fresh lease (J4).
//!
//! Loop conventions shared by `repair`, `reaper` and `lease_keeper`: each is
//! a named component built by `run_node` and started with `start(services)`
//! as one service of the node's `Services`; it stops when `services.stop`
//! fires, checked between passes, never inside one. Each pass runs on the
//! blocking pool. A pass error is logged at warn and retried on the next
//! period. A failed blocking worker is logged and retried, except in
//! `repair`, where a panic fails the service so a duplicate worker cannot
//! start. Node heartbeat and the mixed background tick stay in `run_node`
//! (R48).

pub(crate) mod classify;
#[cfg(feature = "dfs")]
pub(crate) mod lease_keeper;
#[cfg(feature = "dfs")]
pub(crate) mod reaper;
#[cfg(feature = "dfs")]
pub(crate) mod repair;
