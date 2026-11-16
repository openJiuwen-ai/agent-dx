//! Node-side GC execution.
//!
//! GC has three homes: shared records in `crate::dfs::gc`, Meta's reachability
//! planning in `meta::dfs::gc`, and what a Node runs, here:
//!
//! - `references`: reference pins (`GcControl`, `GcReferences`, `GcPin`) taken
//!   by handles, writes and commits so Meta never plans away in-flight data.
//! - `collect`: one collection pass (`Collector`) over the local chunk store or
//!   the external object store, driven by the fault layer's reaper loop.
//!
//! The store-local retire primitives stay in `chunk::collect` because they work
//! on `LocalChunkStore` internals. The Meta client for `GcControl` is
//! `rpc::meta::dfs_gc`.

mod collect;
mod references;

pub(crate) use collect::{Collector, ExternalObjects};
pub use references::{GcBatch, GcControl, GcPin, GcReferences, SoftReader};
pub(crate) use references::{GcReleaseMetrics, is_permanent_admission_failure};
