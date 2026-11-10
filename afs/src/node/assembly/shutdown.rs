//! Node shutdown sequence (段 23–25). The order is a contract, pinned by
//! `node::shutdown_order_tests`; the code below runs in exactly this order:
//!
//! 1. Close admission (`close_admission`, run by `Services` right before it
//!    broadcasts stop): DFS `shutting_down`, DFS write admission, ChunkStore
//!    catalog/sidecar/Peer-serve admission (the catalog itself stays open for
//!    step 6), outboard range work, the DFS read authority, Peer-cache
//!    admission, the object fetcher; then the caller's trigger.
//! 2. `Services` stops: stop broadcast, 10 s drain, abort on timeout.
//! 3. The OwnerFs MountManager stops.
//! 4. The DFS FUSE session is joined, then the OwnerFs one, so no kernel
//!    request can dirty a file any more. A failed join is kept and then waits
//!    for the process watchdog.
//! 5. DFS drains (`drain_dfs`): writeback, remote releases, Peer owner handles,
//!    Peer lock sessions, GC releases — while its stores are still open.
//! 6. ChunkStore background workers exit: catalog committer, Peer serve,
//!    sidecar installer.
//! 7. Object fetcher, Peer cache, S3 verifier.
//! 8. Write buffers stop retaining idle bodies.
//! 9. The local API stops (10 s limit).
//!
//! Every step runs even after a failure; the first error is kept and returned.

use super::common::{Backends, FuseMounts};
#[cfg(feature = "dfs")]
use super::dfs::{DfsAssembly, DfsReadAdmission, DfsStorage};
#[cfg(any(feature = "ownerfs", feature = "dfs"))]
use crate::node::fuse;
#[cfg(feature = "dfs")]
use crate::node::{background, vfs};
use crate::{
    config::Config,
    node::{Node, api},
    runtime::{BoxError, Services},
};
#[cfg(feature = "dfs")]
use std::sync::{Arc, atomic::Ordering};

/// What the sequence closes, consumed step by step.
pub(super) struct ShutdownResources<'a> {
    pub(super) state: &'a Node,
    pub(super) backends: Backends,
    pub(super) mounts: FuseMounts,
    pub(super) local: api::local::LocalApiServer,
}

/// Serve until a signal or a fatal service exit, then run steps 1–9.
pub(super) async fn serve_until_shutdown(
    cfg: &Config,
    services: Services,
    resources: ShutdownResources<'_>,
    on_shutdown: impl FnOnce(),
) -> Result<(), BoxError> {
    let ShutdownResources {
        state,
        backends,
        mounts,
        local,
    } = resources;
    // Close read admission at the same lifecycle boundary that broadcasts
    // shutdown. A cached object must not outlive a known Node/session stop.
    let on_shutdown = || {
        close_admission(state, &backends);
        on_shutdown();
    };
    let mut shutdown_error = {
        // Deployment identity: every test must confirm the running binary matches
        // the expected source revision before trusting any measurement.
        afs_logging::info!(
            "build.info";
            "git" => env!("AFS_BUILD_ID"),
            "pkg_version" => env!("CARGO_PKG_VERSION"),
            "exe" => std::env::current_exe().ok().map(|p| p.display().to_string()).unwrap_or_default()
        );
        afs_logging::info!("node.ready";"grpc"=>cfg.grpc_listen.to_string(),"rest"=>cfg.rest_listen.to_string(),"uds"=>cfg.uds_path.display().to_string(),"ownerfs"=>cfg.ownerfs,"dfs"=>cfg.dfs);
        #[cfg(test)]
        record_shutdown_stage("serving");
        services.run_with_shutdown(on_shutdown).await.err()
    };
    #[cfg(test)]
    record_shutdown_stage("services_stopped");
    #[cfg(feature = "ownerfs")]
    if let Some(owner) = state.ownerfs.as_ref() {
        owner.mount_manager_shutdown();
        #[cfg(test)]
        record_shutdown_stage("ownerfs_mounts_stopped");
    }
    // Stop FUSE admission and observe its thread cleanup before the final dirty drain.
    // Dropping BackgroundSession alone detaches the thread and cannot prove this boundary.
    #[cfg(feature = "dfs")]
    if let Some(mounted) = mounts.dfs {
        join_fuse(mounted, &mut shutdown_error).await;
        #[cfg(test)]
        record_shutdown_stage("dfs_fuse_joined");
    }
    #[cfg(feature = "ownerfs")]
    if let Some(mounted) = mounts.ownerfs {
        join_fuse(mounted, &mut shutdown_error).await;
        #[cfg(test)]
        record_shutdown_stage("ownerfs_fuse_joined");
    }
    #[cfg(feature = "dfs")]
    if let Some(dfs) = state.dfs.clone() {
        drain_dfs(dfs, &mut shutdown_error).await;
        #[cfg(test)]
        record_shutdown_stage("dfs_drained");
    }
    #[cfg(feature = "dfs")]
    let mut backends = backends;
    #[cfg(feature = "dfs")]
    stop_dfs_stores(&mut backends.dfs, &mut shutdown_error).await;
    #[cfg(not(any(feature = "ownerfs", feature = "dfs")))]
    let _ = mounts;
    match tokio::time::timeout(std::time::Duration::from_secs(10), local.shutdown()).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => remember_shutdown_error(&mut shutdown_error, error.into()),
        Err(error) => remember_shutdown_error(&mut shutdown_error, error.into()),
    }
    #[cfg(test)]
    record_shutdown_stage("local_api_stopped");
    if let Some(error) = shutdown_error {
        afs_logging::error!("node.shutdown_failed"; "error" => error.to_string());
        return Err(error);
    }
    Ok(())
}

/// Step 1. Runs inside `Services::run_with_shutdown`, before the stop broadcast.
fn close_admission(state: &Node, backends: &Backends) {
    #[cfg(feature = "dfs")]
    let DfsAssembly {
        read:
            DfsReadAdmission {
                authority: dfs_read_authority,
                shutting_down: dfs_shutting_down,
            },
        storage:
            DfsStorage {
                chunk_store_for_shutdown: dfs_sidecar_store_for_shutdown,
                outboard_budget: dfs_outboard_budget,
                ..
            },
        peer_cache: dfs_peer_cache_for_shutdown,
        fetcher: dfs_fetcher_for_shutdown,
        ..
    } = &backends.dfs;
    #[cfg(not(feature = "dfs"))]
    let _ = (state, backends);
    #[cfg(feature = "dfs")]
    dfs_shutting_down.store(true, Ordering::Release);
    #[cfg(feature = "dfs")]
    if let Some(dfs) = &state.dfs {
        dfs.close_write_admission();
    }
    #[cfg(feature = "dfs")]
    if let Some(store) = &dfs_sidecar_store_for_shutdown {
        // Keep catalog admission available for the existing DFS drain below.
        store.note_catalog_stop();
        store.stop_sidecar_installation();
        store.close_peer_serve_admission();
    }
    #[cfg(feature = "dfs")]
    dfs_outboard_budget.stop_range_work();
    #[cfg(feature = "dfs")]
    if let Some(authority) = &dfs_read_authority {
        authority.stop();
    }
    #[cfg(feature = "dfs")]
    if let Some(cache) = &dfs_peer_cache_for_shutdown {
        cache.close_admission();
    }
    #[cfg(feature = "dfs")]
    if let Some(fetcher) = &dfs_fetcher_for_shutdown {
        fetcher.close();
    }
    #[cfg(test)]
    record_shutdown_stage("admission_closed");
}

/// Step 4, for one FUSE session.
#[cfg(any(feature = "ownerfs", feature = "dfs"))]
async fn join_fuse(mounted: fuse::MountedFuse, shutdown_error: &mut Option<BoxError>) {
    match crate::node::bridge::spawn_blocking(move || mounted.join()).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            afs_logging::error!("node.fuse_closure_failed"; "error" => error.to_string());
            remember_shutdown_error(shutdown_error, error.into());
            std::future::pending::<()>().await;
        }
        Err(error) => {
            afs_logging::error!("node.fuse_closure_failed"; "error" => error.to_string());
            remember_shutdown_error(shutdown_error, error.into());
            std::future::pending::<()>().await;
        }
    }
}

/// Step 5: the shutdown passes, all attempted, first error kept.
#[cfg(feature = "dfs")]
async fn drain_dfs(dfs: Arc<vfs::dfs::DistributedFs>, shutdown_error: &mut Option<BoxError>) {
    match crate::node::bridge::spawn_blocking(move || {
        let [committed, remote_releases, owner_handles, lock_sessions, _] =
            background::drain_all([
                &|| dfs.drain(),
                &|| dfs.drain_pending_remote_releases(),
                &|| dfs.reap_expired_peer_owner_handles(),
                &|| dfs.reap_expired_peer_lock_sessions(),
                &|| dfs.flush_gc_releases().map(|()| 0),
            ])?;
        Ok::<_, afs_error::Error>((committed, remote_releases, owner_handles, lock_sessions))
    })
    .await
    {
        Ok(Ok((committed, remote_releases, owner_handles, lock_sessions)))
            if committed > 0 || remote_releases > 0 || owner_handles > 0 || lock_sessions > 0 =>
        {
            if committed > 0 {
                afs_logging::info!("dfs.node_drain_versions_committed"; "count" => committed);
            }
            if remote_releases > 0 {
                afs_logging::info!("dfs.node_drain_remote_releases"; "count" => remote_releases);
            }
            if owner_handles > 0 {
                afs_logging::info!("dfs.node_drain_owner_handles_reaped"; "count" => owner_handles);
            }
            if lock_sessions > 0 {
                afs_logging::info!("dfs.node_drain_lock_sessions_reaped"; "count" => lock_sessions);
            }
        }
        Ok(Ok(_)) => {}
        Ok(Err(error)) => {
            afs_logging::error!("dfs.node_drain_incomplete"; "error" => error.to_string());
            remember_shutdown_error(shutdown_error, error.into());
        }
        Err(error) => {
            afs_logging::error!("dfs.node_drain_worker_failed"; "error" => error.to_string());
            remember_shutdown_error(shutdown_error, error.into());
        }
    }
}

/// Steps 6–8. Each handle is taken where the original sequence consumed it.
#[cfg(feature = "dfs")]
async fn stop_dfs_stores(dfs: &mut DfsAssembly, shutdown_error: &mut Option<BoxError>) {
    let dfs_sidecar_store_for_shutdown = dfs.storage.chunk_store_for_shutdown.take();
    let dfs_fetcher_for_shutdown = dfs.fetcher.take();
    let dfs_peer_cache_for_shutdown = dfs.peer_cache.take();
    let dfs_external_store_for_shutdown = dfs.external_store.take();
    let dfs_write_buffers = &dfs.write_buffers;
    #[cfg(feature = "dfs")]
    if let Some(store) = dfs_sidecar_store_for_shutdown {
        match crate::node::bridge::spawn_blocking(move || {
            // Attempt both drains even if the first one reports a failure.
            (
                store.shutdown_catalog_committer(),
                store.shutdown_peer_serve(),
                store.shutdown_sidecar_installer(),
            )
        })
        .await
        {
            Ok(results) => {
                for result in [results.0, results.1, results.2] {
                    if let Err(error) = result {
                        remember_shutdown_error(shutdown_error, error.into());
                    }
                }
            }
            Err(error) => remember_shutdown_error(shutdown_error, error.into()),
        }
        #[cfg(test)]
        record_shutdown_stage("chunk_store_exited");
    }
    #[cfg(all(test, feature = "dfs"))]
    if dfs_fetcher_for_shutdown.is_some() {
        record_shutdown_stage("fetcher_stopping");
    }
    #[cfg(feature = "dfs")]
    if let Some(fetcher) = dfs_fetcher_for_shutdown
        && let Err(error) = fetcher.shutdown().await
    {
        remember_shutdown_error(shutdown_error, error.into());
    }
    #[cfg(feature = "dfs")]
    if let Some(cache) = dfs_peer_cache_for_shutdown {
        match crate::node::bridge::spawn_blocking(move || cache.shutdown()).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                afs_logging::error!("dfs.peer_cache_shutdown_failed"; "error" => error.to_string());
                remember_shutdown_error(shutdown_error, error.into());
            }
            Err(error) => {
                afs_logging::error!("dfs.peer_cache_shutdown_failed"; "error" => error.to_string());
                remember_shutdown_error(shutdown_error, error.into());
            }
        }
        #[cfg(test)]
        record_shutdown_stage("peer_cache_exited");
    }
    #[cfg(feature = "dfs")]
    if let Some(store) = dfs_external_store_for_shutdown {
        match crate::node::bridge::spawn_blocking(move || store.shutdown_verifier()).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                afs_logging::error!("dfs.external_store_shutdown_failed"; "error" => error.to_string());
                remember_shutdown_error(shutdown_error, error.into());
            }
            Err(error) => {
                afs_logging::error!("dfs.external_store_shutdown_failed"; "error" => error.to_string());
                remember_shutdown_error(shutdown_error, error.into());
            }
        }
        #[cfg(test)]
        record_shutdown_stage("external_verifier_exited");
    }
    #[cfg(feature = "dfs")]
    {
        // Stop retaining idle bodies after normal drain. Outstanding owners keep
        // their allocations alive and release directly when their work ends.
        dfs_write_buffers.stop();
        let buffers = dfs_write_buffers.snapshot();
        afs_logging::info!("dfs.write_buffers_stopped";
            "live_bytes" => buffers.live_bytes,
            "idle_bytes" => buffers.idle_bytes);
        #[cfg(test)]
        record_shutdown_stage("write_buffers_stopped");
    }
}

fn remember_shutdown_error(first: &mut Option<BoxError>, error: BoxError) {
    if first.is_none() {
        *first = Some(error);
    }
}

/// Test-only witness of the shutdown sequence; `shutdown_order_tests` pins the
/// order. Each call sits right after the stage it names has finished.
#[cfg(test)]
fn record_shutdown_stage(stage: &str) {
    use std::io::Write;
    let Some(path) = std::env::var_os("AFS_TEST_SHUTDOWN_TRACE") else {
        return;
    };
    let mut trace = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("open shutdown trace");
    writeln!(trace, "{stage}").expect("append shutdown trace");
}

#[cfg(test)]
mod tests {
    #[test]
    fn cleanup_keeps_first_failure_after_later_cleanup() {
        let mut first = None;
        super::remember_shutdown_error(&mut first, std::io::Error::other("drain failed").into());
        super::remember_shutdown_error(
            &mut first,
            std::io::Error::other("local shutdown failed").into(),
        );
        assert_eq!(first.unwrap().to_string(), "drain failed");
    }
}
