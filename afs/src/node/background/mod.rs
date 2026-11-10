//! Node background services: the long-lived tasks that run inside the Node's
//! `Services` set, each a named component.
//!
//! # Convention
//!
//! A component is a struct built by the assembly (`node::assembly`) and
//! started with `start(self, &mut Services)`, which spawns exactly one
//! service. It stops when `services.stop` fires, checked between passes,
//! never inside one; blocking passes run on the blocking pool. Each component
//! documents three things:
//!
//! - **class**: *fatal* — returning `Err` (or exiting at all) shuts the whole
//!   Node down, because `Services` treats the end of any service as the end of
//!   the Node; or *retry* — a failed pass is logged and retried next period,
//!   and the service returns only at shutdown (normally `Ok`). A retry-class
//!   component must never return before stop: even `Ok` ends the Node with
//!   "service exited unexpectedly".
//! - **log events**: the names it emits (`<component>.retry`-style names are
//!   kept exactly as they were before the components were named).
//! - **stop**: how `services.stop` ends it.
//!
//! # Components
//!
//! | # | Component | Where | Class |
//! |---|---|---|---|
//! | T1 | `ReadinessSampler` | `node::readiness` | retry |
//! | T2 | `MetaHeartbeat` | `node::registration` | fatal |
//! | T3 | `OwnerPeerReaper` | `node::assembly::ownerfs` | retry |
//! | T4 | `Reaper` | `node::fault::reaper` | retry |
//! | T4b | `SuspectReviewer` | `node::fault::suspect` | retry |
//! | T5 | `LeaseKeeper` | `node::fault::lease_keeper` | retry |
//! | T6 | `DfsBackgroundTick` | `tick` | retry |
//! | T7 | `LocalApiWatch` | `local_api_watch` | fatal |
//! | T8 | gRPC server | `node::assembly::grpc` | fatal |
//! | T9 | `RepairService` | `node::fault::repair` | retry (a panicked pass is fatal) |
//! | T10 | `RestServer` | `rest_server` | fatal |
//! | T11 | `RdmaSessionSweeper` | `rdma_session_sweeper` | retry (no error path) |
//! | T13 | workspace bind-root commands | `node::assembly::ownerfs` | fatal |
//! | T14 | `ScrubService` | `node::fault::scrub` | retry |
//!
//! T12 (the experimental native workspace monitor) was removed in S1e-3.
//!
//! # Threads owned by components
//!
//! Some components hold their own threads or tasks outside `Services`: the
//! local API task (`LocalApiServer`, abort handle), the two FUSE
//! `BackgroundSession`s, the `LocalChunkStore` catalog committer, sidecar
//! installer and Peer serve, `ObjectFetcher`, `DiskPeerCache`, the
//! `S3ChunkStore` verifier and the OwnerFs `MountManager`. The intended
//! convention for them is a `close_admission()` that stops new work and a
//! `shutdown()` that joins the threads, both called by
//! `node::assembly::shutdown` in its fixed order. Today each keeps its own
//! method names; this documents the convention only (S1e: no implementation
//! change).

mod local_api_watch;
mod rdma_session_sweeper;
mod rest_server;
#[cfg(feature = "dfs")]
pub(crate) mod tick;

pub(crate) use local_api_watch::LocalApiWatch;
pub(crate) use rdma_session_sweeper::RdmaSessionSweeper;
pub(crate) use rest_server::RestServer;

/// Run every pass in order and keep the first error.
///
/// All passes run even after one fails; a failed pass counts 0. Returns the
/// counts when every pass succeeded, otherwise the first error. The DFS tick
/// and the shutdown drain share it.
#[cfg(feature = "dfs")]
pub(crate) fn drain_all<const N: usize>(
    passes: [&dyn Fn() -> afs_error::Result<usize>; N],
) -> afs_error::Result<[usize; N]> {
    let mut first_error = None;
    let counts = passes.map(|pass| match pass() {
        Ok(count) => count,
        Err(error) => {
            first_error.get_or_insert(error);
            0
        }
    });
    match first_error {
        Some(error) => Err(error),
        None => Ok(counts),
    }
}

#[cfg(all(test, feature = "dfs"))]
mod tests {
    use super::drain_all;
    use std::cell::RefCell;

    #[test]
    fn drain_all_runs_every_pass_in_order_and_keeps_the_first_error() {
        let order = RefCell::new(Vec::new());
        let error = drain_all([
            &|| {
                order.borrow_mut().push(1);
                Ok(3)
            },
            &|| {
                order.borrow_mut().push(2);
                Err(afs_error::Error::coded(
                    afs_error::NODE_STORAGE_INVALID,
                    "first",
                ))
            },
            &|| {
                order.borrow_mut().push(3);
                Err(afs_error::Error::coded(
                    afs_error::NODE_STORAGE_INVALID,
                    "second",
                ))
            },
        ])
        .unwrap_err();
        assert_eq!(error.message(), "first");
        assert_eq!(*order.borrow(), [1, 2, 3]);
        assert_eq!(drain_all([&|| Ok(1), &|| Ok(2)]).unwrap(), [1, 2]);
    }
}
