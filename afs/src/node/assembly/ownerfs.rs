//! OwnerFs share of the Node assembly (OwnerFs 同事代码).
//!
//! Every block below is moved verbatim from the former `node::run_node`; only
//! the enclosing function signatures and the names of their inputs are new
//! (see `features/afs-dfs/out/stage-s1e-20261105/s1e4/ownerfs-impact.md`).
//! In startup order: the RPC metrics (`common::FsMetrics`), `assemble` (段 7),
//! the RDMA session table (`common::LocalEndpoints`), `mount_fuse`, the control
//! and `owner_files` gRPC pieces, `OwnerPeerReaper` (T3), the RDMA sweep
//! (`background::RdmaSessionSweeper`) and the workspace bind-root commands
//! (T13). Shutdown keeps its OwnerFs steps in `shutdown.rs` so the order stays
//! in one place.

use super::common::Startup;
#[cfg(feature = "ownerfs")]
use crate::node::registration::{
    META_FAIL_CLOSED_AFTER, META_RETRY_INTERVAL, meta_transport_error_is_retryable,
};
use crate::{
    config::Config,
    node::{Node, api, bridge, fuse, rpc, storage, vfs},
    runtime::{BoxError, Services, cancelled},
};
use std::{collections::HashSet, sync::Arc, time::Duration};

/// B-side slow-path connector. The root grant and Home location stay in the
/// OwnerFs business layer; this adapter turns Meta's authenticated node
/// endpoint into a cached OwnerFiles transport client and forwards reserved
/// workspace creation requests to Home.
#[cfg(feature = "ownerfs")]
struct OwnerFilesFactory {
    meta: Arc<rpc::meta::GrpcRootMeta>,
    peers: Arc<rpc::peer::PeerConnectionPool>,
    runtime: tokio::runtime::Handle,
    metrics: rpc::OwnerRpcMetrics,
    data_mode: rpc::peer::DataMode,
    rdma_device: Option<String>,
    timeout: std::time::Duration,
    rdma_admission: Arc<tokio::sync::Semaphore>,
}

#[cfg(feature = "ownerfs")]
impl vfs::ownerfs::RemoteFilesFactory for OwnerFilesFactory {
    fn prepare_root(
        &self,
        caller: &vfs::types::RequestContext,
        reservation: &vfs::ownerfs::root::RootReservation,
        params: &vfs::ownerfs::root::RootCreateParams,
    ) -> afs_error::Result<()> {
        let (uri, epoch) = self.meta.lookup_node_location(&reservation.home_node_id)?;
        bridge::block_on(&self.runtime, async {
            let channel = self
                .peers
                .channel(&reservation.home_node_id, epoch, &uri)
                .await?;
            tokio::time::timeout(
                self.timeout,
                rpc::ownerfs::prepare_root(channel, caller, reservation, params),
            )
            .await
            .map_err(|_| {
                afs_error::Error::coded(
                    afs_error::CLIENT_DEADLINE_EXCEEDED,
                    "OwnerFs creation request timed out",
                )
            })?
        })
    }

    fn supports_advisory_locks(&self) -> bool {
        true
    }

    fn supports_killpriv_v2(&self) -> bool {
        true
    }

    fn connect(
        &self,
        home_node_id: &str,
    ) -> afs_error::Result<Arc<dyn vfs::ownerfs::remote::RemoteFiles>> {
        let (uri, node_epoch) = self.meta.lookup_node_location(home_node_id)?;
        let channel = bridge::block_on(
            &self.runtime,
            self.peers
                .owner_files_channel(home_node_id, node_epoch, &uri),
        )?;
        let long_wait_channel = bridge::block_on(
            &self.runtime,
            self.peers.long_wait_channel(home_node_id, node_epoch, &uri),
        )?;
        Ok(Arc::new(
            rpc::peer::owner_files_client_from_channel_with_runtime_and_metrics(
                channel,
                self.runtime.clone(),
                Some(self.metrics.clone()),
            )
            .with_data_transport(self.data_mode, self.rdma_device.clone(), self.timeout)
            .with_rdma_admission(self.rdma_admission.clone())
            .with_long_wait_channel(long_wait_channel),
        ))
    }
}

/// What `assemble` hands to the later stages.
pub(super) struct OwnerFsAssembly {
    pub(super) instance: Option<Arc<vfs::ownerfs::OwnerFs>>,
    /// Present only with `experimental_ownerfs_workspace_bind`; taken by
    /// `start_workspace_bind_root_commands`.
    pub(super) workspace_bind_root_control: Option<(
        Arc<rpc::meta::GrpcRootMeta>,
        Arc<vfs::ownerfs::root::RootManager>,
    )>,
    /// Meta handle and root manager for the outlet mirror and the reclaim
    /// sweep. `None` without OwnerFs: neither task has anything to work on.
    pub(super) outlet_control: Option<super::outlet::OutletControl>,
}

/// 段 7: RootManager, RDMA probe, MountManager startup reconciliation.
pub(super) async fn assemble(
    cfg: &Config,
    startup: &Startup<'_>,
    peer_connections: &Arc<rpc::peer::PeerConnectionPool>,
    owner_rpc_metrics: &rpc::OwnerRpcMetrics,
) -> Result<OwnerFsAssembly, BoxError> {
    let meta_endpoint = startup.meta_endpoint;
    let session_id = &startup.session_id;
    let timeout = startup.timeout;
    let mut workspace_bind_root_control = None;
    let mut outlet_control = None;
    let ownerfs_instance = if cfg.ownerfs {
        let endpoint = meta_endpoint.expect("OwnerFs checked meta_endpoint");
        let root_meta = Arc::new(rpc::meta::GrpcRootMeta::new(
            endpoint,
            cfg.id.clone(),
            session_id.clone(),
            timeout,
            cfg.tls_config(),
        )?);
        let disk = Arc::new(storage::LocalFs::open(&cfg.ownerfs_data_dir)?);
        let recovery_disk = disk.clone();
        let recovery_node_id = cfg.id.clone();
        let recovery_session_id = session_id.clone();
        let owner_data_mode = cfg.data_mode;
        if owner_data_mode == rpc::peer::DataMode::Rdma && cfg.rdma_device.is_none() {
            return Err(afs_error::Error::coded(
                afs_error::CONFIG_INVALID,
                "OwnerFs RDMA requires rdma_device",
            )
            .into());
        }
        #[cfg(feature = "rdma")]
        if owner_data_mode == rpc::peer::DataMode::Rdma {
            let device = cfg
                .rdma_device
                .clone()
                .expect("OwnerFs RDMA checked rdma_device");
            crate::node::bridge::spawn_blocking(move || {
                afs_transport::rdma::RdmaEndpoint::open(&device)
            })
            .await?
            .map_err(|error| {
                afs_error::Error::coded(afs_error::NODE_TRANSFER_UNAVAILABLE, error.to_string())
            })?;
        }
        let remote_factory = Arc::new(OwnerFilesFactory {
            meta: root_meta.clone(),
            peers: peer_connections.clone(),
            runtime: tokio::runtime::Handle::current(),
            metrics: owner_rpc_metrics.clone(),
            data_mode: owner_data_mode,
            rdma_device: cfg.rdma_device.clone(),
            timeout,
            rdma_admission: Arc::new(tokio::sync::Semaphore::new(
                rpc::peer::OWNER_RDMA_MAX_CLIENT_WINDOWS,
            )),
        });
        let recovery_root_meta = root_meta.clone();
        let roots = Arc::new(
            crate::node::bridge::spawn_blocking(move || {
                vfs::ownerfs::root::RootManager::open(
                    recovery_node_id,
                    recovery_session_id,
                    recovery_root_meta,
                    recovery_disk,
                )
            })
            .await??,
        );
        if cfg.ownerfs_adopt_on_startup {
            let adopt_roots = roots.clone();
            match crate::node::bridge::spawn_blocking(move || {
                adopt_roots.adopt_pre_existing_roots()
            })
            .await
            {
                Ok(summary) => afs_logging::info!("ownerfs.adopt.completed";
                    "scanned" => summary.scanned,
                    "adopted" => summary.adopted,
                    "skipped" => summary.skipped,
                    "failed" => summary.failed),
                Err(error) => afs_logging::warn!("ownerfs.adopt.panicked";
                    "error" => error.to_string()),
            }
        }
        if cfg.experimental_ownerfs_workspace_bind {
            workspace_bind_root_control = Some((root_meta.clone(), roots.clone()));
        }
        let roots_for_startup = roots.clone();
        let owner = if cfg.experimental_ownerfs_workspace_bind {
            let egress_root = cfg
                .ownerfs_workspace_egress_root
                .clone()
                .expect("workspace bind requires egress root");
            vfs::ownerfs::OwnerFs::new_local_with_mount_manager(
                roots,
                disk,
                remote_factory,
                egress_root,
            )
        } else {
            vfs::ownerfs::OwnerFs::new_local_with_remote(roots, disk, remote_factory)
        };
        let owner = Arc::new(owner.with_workspace_defaults(cfg.workspace_default_identity()?));
        outlet_control = Some(super::outlet::OutletControl {
            meta: root_meta,
            peers: peer_connections.clone(),
            owner: owner.clone(),
        });
        if let Some(mm) = owner.mount_manager() {
            mm.attach_owner(&owner);
            // Startup egress reconciliation (D4 §4.4): detach residual bind
            // targets left by a previous process, then re-trigger mounts for
            // the recovered roots whose live claims were lost on restart.
            let recovered = roots_for_startup.recovered_roots();
            let known: HashSet<_> = recovered.iter().map(|(id, _)| id.clone()).collect();
            if let Err(error) = mm.reconcile_egress(&known) {
                afs_logging::warn!(
                    "ownerfs.mount_manager.reconcile_egress_failed";
                    "error" => error.to_string()
                );
            }
            for (id, name) in recovered {
                if let Err(error) = mm.request_mount(id, name) {
                    afs_logging::warn!(
                        "ownerfs.mount_manager.restart_mount_failed";
                        "error" => error.to_string()
                    );
                }
            }
        }
        Some(owner)
    } else {
        None
    };
    Ok(OwnerFsAssembly {
        instance: ownerfs_instance,
        workspace_bind_root_control,
        outlet_control,
    })
}

/// OwnerFs FUSE mount (段 11). On failure the local API is shut down before
/// the error returns, as before.
pub(super) async fn mount_fuse(
    cfg: &Config,
    ownerfs_instance: &Option<Arc<vfs::ownerfs::OwnerFs>>,
    fuse_request_metrics: &fuse::FuseRequestMetrics,
    local: api::local::LocalApiServer,
) -> Result<(Option<fuse::MountedFuse>, api::local::LocalApiServer), BoxError> {
    let mounted_ownerfs = match (&cfg.ownerfs_mount, &ownerfs_instance) {
        (Some(path), Some(ownerfs)) => match fuse::mount_ownerfs_with_metrics_and_options(
            ownerfs.clone(),
            path,
            fuse_request_metrics.clone(),
            fuse::FuseMountOptions::from(cfg),
        ) {
            Ok(session) => Some(session),
            Err(error) => {
                local.shutdown().await?;
                return Err(error.into());
            }
        },
        (Some(_), None) => {
            local.shutdown().await?;
            return Err(afs_error::Error::coded(
                afs_error::CONFIG_INVALID,
                "ownerfs_mount requires the OwnerFs backend",
            )
            .into());
        }
        (None, _) => None,
    };
    Ok((mounted_ownerfs, local))
}

/// The `OwnerControl` gRPC service: OwnerFs locks and RDMA registry (段 18).
pub(super) fn owner_control_server(
    state: &Node,
    dfs_trusted: &[(String, Vec<u8>)],
    owner_rdma_sessions: &rpc::control::RdmaSessionRegistry,
    lock_waits: rpc::control::BlockingLockWaits,
) -> Result<
    afs_protocol::node_control::owner_control_server::OwnerControlServer<
        rpc::control::OwnerControlService,
    >,
    BoxError,
> {
    let control = rpc::control::OwnerControlService::new(lock_waits);
    let control = if let Some(owner) = state.ownerfs.as_ref() {
        control
            .with_owner_locks(
                owner.peer_executor()?,
                Arc::new(rpc::data::MtlsPeerAuthenticator::new(dfs_trusted.to_vec())?),
            )
            .with_owner_rdma_registry(owner_rdma_sessions.clone())
    } else {
        control
    };
    Ok(control.into_server())
}

/// The `owner_files` gRPC service (段 18).
pub(super) fn owner_files_server(
    cfg: &Config,
    state: &Node,
    owner_rpc_metrics: &rpc::OwnerRpcMetrics,
    owner_rdma_sessions: &rpc::control::RdmaSessionRegistry,
) -> Result<
    afs_protocol::node_data::owner_files_server::OwnerFilesServer<rpc::data::OwnerFilesService>,
    BoxError,
> {
    let owner_files = if let Some(ownerfs) = state.ownerfs.as_ref() {
        let trusted = cfg
            .trusted_node_certs
            .iter()
            .map(|(node_id, path)| {
                Ok((node_id.clone(), crate::config::read_certificate_der(path)?))
            })
            .collect::<afs_error::Result<Vec<_>>>()?;
        let authenticator = Arc::new(rpc::data::MtlsPeerAuthenticator::new(trusted)?);
        let handler = rpc::data::make_owner_files_handler(ownerfs.peer_executor()?);
        rpc::data::make_owner_files_server_with_handler_metrics_and_rdma(
            handler,
            authenticator,
            owner_rpc_metrics.clone(),
            owner_rdma_sessions.clone(),
        )
    } else {
        rpc::data::make_owner_files_server()
    };
    Ok(owner_files)
}

/// T3 `OwnerPeerReaper`: every 10 s (first tick skipped) reaps peer sessions
/// whose Meta state proves them expired, on the blocking pool.
///
/// Retry class: a pass error (`ownerfs.peer_reaper_retry`) or a failed
/// blocking worker (`ownerfs.peer_reaper_worker_failed`) is logged and retried
/// on the next tick; unknown Meta state is never evidence that a peer died.
/// A reaped count above zero is logged (`ownerfs.peer_handles_reaped`). Stop:
/// `services.stop` ends it with `Ok`, checked between passes.
pub(super) struct OwnerPeerReaper {
    ownerfs: Arc<vfs::ownerfs::OwnerFs>,
}

impl OwnerPeerReaper {
    pub(super) fn new(ownerfs: Arc<vfs::ownerfs::OwnerFs>) -> Self {
        Self { ownerfs }
    }

    /// Start the reaper as one service; it stops with `services.stop`.
    pub(super) fn start(self, services: &mut Services) {
        let deletion_fs = self.ownerfs.clone();
        let deletion_stop = services.stop.subscribe();
        services.spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(10));
            let shutdown = cancelled(deletion_stop);
            tokio::pin!(shutdown);
            loop {
                tokio::select! {
                    _ = &mut shutdown => return Ok(()),
                    _ = tick.tick() => {
                        let fs = deletion_fs.clone();
                        match crate::node::bridge::spawn_blocking(move || fs.maintain_workspace_deletions()).await {
                            Ok(Ok(())) => {},
                            result => afs_logging::warn!("ownerfs.deletion_maintenance_retry"; "result" => ?result),
                        }
                    }
                }
            }
        });
        let ownerfs = self.ownerfs;
        let stop = services.stop.subscribe();
        services.spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(10));
            tick.tick().await;
            let shutdown = cancelled(stop);
            tokio::pin!(shutdown);
            loop {
                tokio::select! {
                    _ = &mut shutdown => return Ok(()),
                    _ = tick.tick() => {
                        let fs = ownerfs.clone();
                        match crate::node::bridge::spawn_blocking(move || fs.reap_expired_peer_sessions()).await {
                            Ok(Ok(count)) if count > 0 => {
                                afs_logging::info!("ownerfs.peer_handles_reaped"; "count" => count);
                            }
                            Ok(Ok(_)) => {}
                            Ok(Err(error)) => {
                                // Unknown Meta state is never evidence that a peer died.
                                afs_logging::warn!("ownerfs.peer_reaper_retry"; "error" => error.to_string());
                            }
                            Err(error) => {
                                afs_logging::warn!("ownerfs.peer_reaper_worker_failed"; "error" => error.to_string());
                            }
                        }
                    }
                }
            }
        });
    }
}

/// T13: start the workspace bind-root command loop when workspace bind is on.
///
/// Retryable Meta transport failures are retried inside `META_FAIL_CLOSED_AFTER`;
/// an untrusted control result, a refusal, or an expired window still ends the
/// Node. Stop: `services.stop`, checked before each poll and after the RPC.
pub(super) fn start_workspace_bind_root_commands(
    assembly: &mut OwnerFsAssembly,
    services: &mut Services,
) {
    let workspace_bind_root_control = assembly.workspace_bind_root_control.take();
    // Multi-workspace bind mount is handled by MountManager via mkdir triggers.
    // No single-workspace startup worker is needed.
    #[cfg(feature = "ownerfs")]
    if let Some((meta, roots)) = workspace_bind_root_control {
        let stop = services.stop.subscribe();
        services.spawn(run_workspace_bind_root_commands(
            meta,
            move |command| {
                roots.revoke_matching_home(&vfs::ownerfs::root::HomeRootRevocation {
                    command_id: command.command_id.clone(),
                    root_id: vfs::ownerfs::root::RootId(command.root_id.clone()),
                    root_epoch: command.root_epoch,
                    home_node_id: command.home_node_id.clone(),
                    home_session_id: command.home_session_id.clone(),
                    access_generation: command.access_generation,
                })
            },
            stop,
        ));
    }
}

/// In-session progress only: no persisted cursor or revoke ACK is produced.
/// A matching command rejects admission before Services starts normal closure.
#[cfg(feature = "ownerfs")]
fn process_workspace_bind_root_commands(
    after_revision: u64,
    batch: rpc::meta::RootCommandBatch,
    mut reject: impl FnMut(&rpc::meta::RootCommand) -> afs_error::Result<bool>,
) -> afs_error::Result<u64> {
    let rpc::meta::RootCommandBatch::Events {
        start_revision,
        next_revision,
        commands,
    } = batch
    else {
        return Err(afs_error::Error::coded(
            afs_error::NODE_OWNER_GRANT_UNAVAILABLE,
            "workspace bind RootCommand control requires recovery or is unsupported",
        ));
    };
    if start_revision != after_revision || next_revision <= start_revision {
        return Err(afs_error::Error::coded(
            afs_error::CLIENT_PROTOCOL_VIOLATION,
            "workspace bind RootCommand batch does not match the requested cursor",
        ));
    }
    for command in commands {
        if reject(&command)? {
            afs_logging::info!("ownerfs.workspace_bind_root_command_rejected";
                "command_id" => command.command_id,
                "revision" => command.revision,
                "root_id" => command.root_id,
                "root_epoch" => command.root_epoch,
                "access_generation" => command.access_generation);
            return Err(afs_error::Error::coded(
                afs_error::NODE_OWNER_GRANT_UNAVAILABLE,
                "workspace bind Home admission rejected by Meta RootCommand",
            ));
        }
        afs_logging::info!("ownerfs.workspace_bind_root_command_ignored";
            "command_id" => command.command_id,
            "revision" => command.revision,
            "root_id" => command.root_id);
    }
    // Filtered events still advance through the complete Meta history batch.
    Ok(next_revision - 1)
}

/// Blocking Meta poll boundary for RootCommand batches; tests script transport
/// failures and batches without a live Meta. `budget` is the remaining window.
#[cfg(feature = "ownerfs")]
trait RootCommandPoller: Send + Sync + 'static {
    fn poll_commands(
        &self,
        after_revision: u64,
        budget: Duration,
    ) -> afs_error::Result<rpc::meta::RootCommandBatch>;
}

#[cfg(feature = "ownerfs")]
impl RootCommandPoller for rpc::meta::GrpcRootMeta {
    fn poll_commands(
        &self,
        after_revision: u64,
        budget: Duration,
    ) -> afs_error::Result<rpc::meta::RootCommandBatch> {
        rpc::meta::GrpcRootMeta::poll_root_command_batch(self, after_revision, budget)
    }
}

/// Logs the fatal control event and returns the error unchanged.
#[cfg(feature = "ownerfs")]
fn control_fatal(after_revision: u64, error: afs_error::Error) -> afs_error::Error {
    afs_logging::error!("ownerfs.workspace_bind_root_command_control_failed";
        "after_revision" => after_revision,
        "error" => error.to_string());
    error
}

#[cfg(feature = "ownerfs")]
fn control_window_expired() -> afs_error::Error {
    afs_error::Error::coded(
        afs_error::CLIENT_DEADLINE_EXCEEDED,
        "workspace bind RootCommand control did not recover within the fail-closed window",
    )
}

#[cfg(feature = "ownerfs")]
async fn run_workspace_bind_root_commands(
    meta: Arc<dyn RootCommandPoller>,
    mut reject: impl FnMut(&rpc::meta::RootCommand) -> afs_error::Result<bool> + Send + 'static,
    stop: tokio::sync::watch::Receiver<bool>,
) -> Result<(), BoxError> {
    let shutdown = cancelled(stop.clone());
    tokio::pin!(shutdown);
    let mut after_revision = 0;
    let mut delay = Duration::ZERO;
    let mut last_success = tokio::time::Instant::now();
    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => return Ok(()),
            _ = tokio::time::sleep(delay) => {}
        }
        // The window is absolute: no poll starts after it expires, and a
        // response that outlives it cannot restart the clock.
        let deadline = last_success + META_FAIL_CLOSED_AFTER;
        if tokio::time::Instant::now() >= deadline {
            return Err(control_fatal(after_revision, control_window_expired()).into());
        }
        let deadline_std = deadline.into_std();
        let meta = meta.clone();
        // The wait and the poll share the same deadline. The blocking task
        // re-checks it after getting a thread, so a queued call does not reach
        // Meta after the window. Abandoning the call happens only on the fatal
        // path, so no later poll can duplicate it.
        let result = match tokio::time::timeout_at(
            deadline,
            crate::node::bridge::spawn_blocking(move || {
                // Blocking threads have no runtime clock; production uses real
                // time, where the tokio and std instants agree.
                let started_at = std::time::Instant::now();
                if started_at >= deadline_std {
                    return Err(control_window_expired());
                }
                meta.poll_commands(
                    after_revision,
                    deadline_std.saturating_duration_since(started_at),
                )
            }),
        )
        .await
        {
            Ok(joined) => joined?,
            Err(_) => {
                if *stop.borrow() {
                    return Ok(());
                }
                return Err(control_fatal(after_revision, control_window_expired()).into());
            }
        };
        if *stop.borrow() {
            return Ok(());
        }
        let received_at = tokio::time::Instant::now();
        if received_at >= deadline {
            return Err(control_fatal(after_revision, control_window_expired()).into());
        }
        // A Meta restart is a retryable transport failure, not an untrusted
        // control result: retry inside the heartbeat's fail-closed window.
        let batch = match result {
            Ok(batch) => {
                last_success = received_at;
                batch
            }
            Err(error) => {
                if meta_transport_error_is_retryable(&error) {
                    afs_logging::warn!("ownerfs.workspace_bind_root_command_control_retry";
                        "after_revision" => after_revision,
                        "since_last_success_ms" => u64::try_from(received_at.duration_since(last_success).as_millis()).unwrap_or(u64::MAX),
                        "error" => error.to_string());
                    delay = META_RETRY_INTERVAL.min(deadline.saturating_duration_since(received_at));
                    continue;
                }
                return Err(control_fatal(after_revision, error).into());
            }
        };
        // Untrusted control results and refusals stay fatal on the first poll.
        let next = process_workspace_bind_root_commands(after_revision, batch, &mut reject)
            .map_err(|error| control_fatal(after_revision, error))?;
        delay = if next > after_revision {
            Duration::ZERO
        } else {
            Duration::from_millis(250)
        };
        after_revision = next;
    }
}

#[cfg(all(test, feature = "ownerfs"))]
mod workspace_bind_root_command_tests {
    use super::*;
    use rpc::meta::{RootCommand, RootCommandBatch, RootCommandRecoveryReason, RootCommandType};

    fn command(id: &str, revision: u64) -> RootCommand {
        RootCommand {
            command_id: id.into(),
            command_type: RootCommandType::RevokeAccess,
            revision,
            root_id: "workspace".into(),
            root_epoch: 7,
            home_node_id: "node-a".into(),
            home_session_id: "session-a".into(),
            access_generation: 3,
        }
    }

    fn events(start: u64, next: u64, commands: Vec<RootCommand>) -> RootCommandBatch {
        RootCommandBatch::Events {
            start_revision: start,
            next_revision: next,
            commands,
        }
    }

    #[test]
    fn workspace_bind_root_command_filtered_and_idle_batches_preserve_full_progress() {
        let reject = |_: &RootCommand| panic!("no command should be applied");
        assert_eq!(
            process_workspace_bind_root_commands(10, events(10, 21, vec![]), reject).unwrap(),
            20
        );
        assert_eq!(
            process_workspace_bind_root_commands(20, events(20, 21, vec![]), reject).unwrap(),
            20
        );
    }

    #[test]
    fn workspace_bind_root_command_unrelated_tuples_are_processed_before_progress() {
        let mut seen = Vec::new();
        let next = process_workspace_bind_root_commands(
            10,
            events(10, 31, vec![command("old", 12), command("foreign", 20)]),
            |c| {
                seen.push(c.command_id.clone());
                Ok(false)
            },
        )
        .unwrap();
        assert_eq!(next, 30);
        assert_eq!(seen, ["old", "foreign"]);
    }

    #[test]
    fn workspace_bind_root_command_matching_refusal_stops_without_ack_or_further_admission() {
        let mut seen = Vec::new();
        let error = process_workspace_bind_root_commands(
            10,
            events(
                10,
                31,
                vec![
                    command("old", 12),
                    command("matching", 20),
                    command("later", 21),
                ],
            ),
            |c| {
                seen.push(c.command_id.clone());
                Ok(c.command_id == "matching")
            },
        )
        .unwrap_err();
        assert_eq!(seen, ["old", "matching"]);
        assert_eq!(error.code(), afs_error::NODE_OWNER_GRANT_UNAVAILABLE);
        assert!(
            error
                .to_string()
                .contains("Home admission rejected by Meta RootCommand")
        );
    }

    #[test]
    fn workspace_bind_root_command_invalid_or_replayed_cursor_cannot_touch_grants() {
        for (start, next) in [(9, 31), (10, 10), (10, 0)] {
            let error = process_workspace_bind_root_commands(
                10,
                events(start, next, vec![command("matching", 12)]),
                |_| panic!("invalid batch cannot touch a grant"),
            )
            .unwrap_err();
            assert_eq!(error.code(), afs_error::CLIENT_PROTOCOL_VIOLATION);
        }
    }

    #[test]
    fn workspace_bind_root_command_compaction_and_unsupported_do_not_restore_authority() {
        for batch in [
            RootCommandBatch::Compacted {
                requested_after: 10,
                compacted_to: 20,
                recovery_resume_after: 20,
                recovery_reason: RootCommandRecoveryReason::WatchCompacted,
            },
            RootCommandBatch::Unsupported {
                message: "not supported".into(),
            },
        ] {
            let error = process_workspace_bind_root_commands(10, batch, |_| {
                panic!("non-authorizing control result")
            })
            .unwrap_err();
            assert_eq!(error.code(), afs_error::NODE_OWNER_GRANT_UNAVAILABLE);
        }
    }

    #[test]
    fn workspace_bind_root_command_refusal_error_is_propagated_without_progress() {
        let error = process_workspace_bind_root_commands(
            10,
            events(10, 31, vec![command("matching", 12)]),
            |_| {
                Err(afs_error::Error::coded(
                    afs_error::NODE_OWNER_INVALID_GRANT,
                    "poisoned grant lock",
                ))
            },
        )
        .unwrap_err();
        assert_eq!(error.code(), afs_error::NODE_OWNER_INVALID_GRANT);
    }

    fn unavailable() -> afs_error::Error {
        afs_error::Error::coded(
            afs_error::IO_UNAVAILABLE,
            "Connection refused (os error 111)",
        )
    }

    fn malformed_reply() -> afs_error::Error {
        afs_error::Error::coded(
            afs_error::CLIENT_PROTOCOL_VIOLATION,
            "RootCommand has an unknown command type",
        )
    }

    fn node_session_rejected() -> afs_error::Error {
        afs_error::Error::coded(
            afs_error::META_CATALOG_INVALID_REQUEST,
            "node session is not current",
        )
    }

    fn remote_status(kind: afs_error::ErrorKind, message: &str) -> afs_error::Error {
        afs_error::Error::new(afs_error::CLIENT_REMOTE_STATUS, kind, message)
    }

    struct ScriptedPoller {
        script: std::sync::Mutex<std::collections::VecDeque<afs_error::Result<RootCommandBatch>>>,
        fail_on_idle: bool,
        calls: std::sync::atomic::AtomicUsize,
        cursors: std::sync::Mutex<Vec<u64>>,
    }

    impl ScriptedPoller {
        fn new(script: Vec<afs_error::Result<RootCommandBatch>>) -> Self {
            Self::with_idle(script, false)
        }

        fn failing_after(script: Vec<afs_error::Result<RootCommandBatch>>) -> Self {
            Self::with_idle(script, true)
        }

        fn always_failing() -> Self {
            Self::failing_after(Vec::new())
        }

        fn with_idle(script: Vec<afs_error::Result<RootCommandBatch>>, fail_on_idle: bool) -> Self {
            Self {
                script: std::sync::Mutex::new(script.into()),
                fail_on_idle,
                calls: std::sync::atomic::AtomicUsize::new(0),
                cursors: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(std::sync::atomic::Ordering::Acquire)
        }

        fn cursors(&self) -> Vec<u64> {
            self.cursors.lock().expect("cursor log").clone()
        }
    }

    impl RootCommandPoller for ScriptedPoller {
        fn poll_commands(
            &self,
            after_revision: u64,
            _budget: Duration,
        ) -> afs_error::Result<RootCommandBatch> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            self.cursors
                .lock()
                .expect("cursor log")
                .push(after_revision);
            let mut script = self.script.lock().expect("scripted poller lock");
            match script.pop_front() {
                Some(result) => result,
                None if self.fail_on_idle => Err(unavailable()),
                None => Ok(events(after_revision, after_revision + 1, vec![])),
            }
        }
    }

    struct SlowPoller {
        calls: std::sync::atomic::AtomicUsize,
        budget: std::sync::Mutex<Option<Duration>>,
        release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
    }

    impl SlowPoller {
        fn new() -> (Self, std::sync::mpsc::Sender<()>) {
            let (release, blocked) = std::sync::mpsc::channel();
            (
                Self {
                    calls: std::sync::atomic::AtomicUsize::new(0),
                    budget: std::sync::Mutex::new(None),
                    release: std::sync::Mutex::new(blocked),
                },
                release,
            )
        }

        fn calls(&self) -> usize {
            self.calls.load(std::sync::atomic::Ordering::Acquire)
        }

        fn budget(&self) -> Option<Duration> {
            *self.budget.lock().expect("budget log")
        }
    }

    impl RootCommandPoller for SlowPoller {
        fn poll_commands(
            &self,
            after_revision: u64,
            budget: Duration,
        ) -> afs_error::Result<RootCommandBatch> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            *self.budget.lock().expect("budget log") = Some(budget);
            // Hold the poll open until the test releases it.
            let _ = self
                .release
                .lock()
                .expect("release lock")
                .recv_timeout(Duration::from_secs(10));
            Ok(events(after_revision, after_revision + 1, vec![]))
        }
    }

    /// A retryable transport error must not end the loop; the retry resumes at
    /// the unchanged cursor and the command delivered after recovery is
    /// processed exactly once.
    #[tokio::test(start_paused = true)]
    async fn workspace_bind_root_command_transport_retry_keeps_control_alive() {
        let poller = Arc::new(ScriptedPoller::new(vec![
            // Advance the cursor to 1 so a reset to 0 would be visible below.
            Ok(events(0, 2, vec![])),
            Err(unavailable()),
            // start_revision < command.revision < next_revision
            Ok(events(1, 3, vec![command("after-retry", 2)])),
        ]));
        let applied = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen = applied.clone();
        let (stop, stop_rx) = tokio::sync::watch::channel(false);
        let handle = tokio::spawn(run_workspace_bind_root_commands(
            poller.clone(),
            move |command: &RootCommand| {
                seen.lock()
                    .expect("applied lock")
                    .push(command.command_id.clone());
                Ok(false)
            },
            stop_rx,
        ));

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while applied.lock().expect("applied lock").is_empty() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "control loop stopped on a retryable transport error"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        stop.send(true).expect("stop receiver must be alive");
        assert!(handle.await.expect("control loop task").is_ok());
        let applied = applied.lock().expect("applied lock");
        assert_eq!(applied.len(), 1);
        assert_eq!(applied[0], "after-retry");
        let cursors = poller.cursors();
        assert!(cursors.len() >= 3, "expected at least three polls");
        assert_eq!(&cursors[..3], [0, 1, 1], "the retry must reuse the cursor");
    }

    /// Retrying is bounded: the loop ends at the 25-second contract, not later.
    #[tokio::test(start_paused = true)]
    async fn workspace_bind_root_command_transport_failure_past_window_is_fatal() {
        let poller = Arc::new(ScriptedPoller::always_failing());
        let (_stop, stop_rx) = tokio::sync::watch::channel(false);
        let started = tokio::time::Instant::now();
        let result = tokio::time::timeout(
            Duration::from_secs(120),
            run_workspace_bind_root_commands(poller.clone(), |_: &RootCommand| Ok(false), stop_rx),
        )
        .await
        .expect("retryable transport failures must fail closed after the window");
        assert!(result.is_err());
        let elapsed = started.elapsed();
        assert!(
            elapsed >= Duration::from_secs(25),
            "the loop must not fail closed before the 25s window"
        );
        assert!(
            elapsed < Duration::from_secs(26),
            "the loop must end at the window instead of running past it"
        );
        assert!(
            poller.calls() > 1,
            "transport failures must be retried before fail-closed"
        );
    }

    /// A success restarts the fail-closed clock; failures after it get a fresh
    /// 25 seconds instead of inheriting the first failure's deadline.
    #[tokio::test(start_paused = true)]
    async fn workspace_bind_root_command_window_restarts_after_success() {
        let mut script = (0..10).map(|_| Err(unavailable())).collect::<Vec<_>>();
        script.push(Ok(events(0, 1, vec![])));
        let poller = Arc::new(ScriptedPoller::failing_after(script));
        let (_stop, stop_rx) = tokio::sync::watch::channel(false);
        let started = tokio::time::Instant::now();
        let result = tokio::time::timeout(
            Duration::from_secs(120),
            run_workspace_bind_root_commands(poller.clone(), |_: &RootCommand| Ok(false), stop_rx),
        )
        .await
        .expect("failures after the success must end the loop at the restarted window");
        assert!(result.is_err());
        let elapsed = started.elapsed();
        assert!(
            elapsed >= Duration::from_secs(35),
            "the success at about 10s must restart the 25s window"
        );
        assert!(
            elapsed < Duration::from_secs(36),
            "the loop must end 25s after the success, not at the first deadline"
        );
    }

    /// An in-flight poll that outlives the window must not delay the exit: the
    /// loop ends at the deadline while the poll is still blocked, and never
    /// issues a second poll.
    #[tokio::test(start_paused = true)]
    async fn workspace_bind_root_command_slow_poll_ends_at_deadline() {
        let (poller, release) = SlowPoller::new();
        let poller = Arc::new(poller);
        let (_stop, stop_rx) = tokio::sync::watch::channel(false);
        let started = tokio::time::Instant::now();
        let handle = tokio::spawn(run_workspace_bind_root_commands(
            poller.clone(),
            |_: &RootCommand| Ok(false),
            stop_rx,
        ));
        // Let the loop issue the poll without letting paused time advance.
        while poller.calls() == 0 {
            tokio::task::yield_now().await;
        }
        // The poll is blocked; move the clock past the window.
        tokio::time::advance(Duration::from_secs(25)).await;

        // The loop must end while the poll is still blocked. A loop that awaits
        // the join here would not finish before the poller is released.
        let not_waiting = std::time::Instant::now() + Duration::from_secs(2);
        while !handle.is_finished() {
            assert!(
                std::time::Instant::now() < not_waiting,
                "the loop waited for the blocked poll instead of ending at the window"
            );
            tokio::task::yield_now().await;
        }
        let result = handle.await.expect("control loop task");
        assert!(result.is_err());
        let elapsed = started.elapsed();
        assert!(
            elapsed >= Duration::from_secs(25),
            "the loop must not end before the window"
        );
        assert!(
            elapsed < Duration::from_secs(26),
            "the loop must end at the window even while the poll is in flight"
        );
        assert_eq!(poller.calls(), 1, "no second poll may be issued");
        let budget = poller.budget().expect("budget log");
        assert!(
            budget > Duration::from_secs(24) && budget <= Duration::from_secs(25),
            "the poll must receive the remaining window as its budget"
        );

        // Release the blocked poll so the blocking task can finish.
        release.send(()).expect("poller must still be blocked");
    }

    /// A malformed Meta reply decodes to an internal protocol error, not to a
    /// transport failure: it must stay fatal without retry.
    #[tokio::test(start_paused = true)]
    async fn workspace_bind_root_command_malformed_reply_is_fatal_without_retry() {
        let poller = Arc::new(ScriptedPoller::new(vec![Err(malformed_reply())]));
        let (_stop, stop_rx) = tokio::sync::watch::channel(false);
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            run_workspace_bind_root_commands(poller.clone(), |_: &RootCommand| Ok(false), stop_rx),
        )
        .await
        .expect("a malformed Meta reply must terminate the loop");
        assert!(result.is_err());
        assert_eq!(poller.calls(), 1);
    }

    /// A non-retryable RPC rejection is not a transport failure: the first poll
    /// must end the loop.
    #[tokio::test(start_paused = true)]
    async fn workspace_bind_root_command_rpc_rejection_is_fatal_without_retry() {
        let poller = Arc::new(ScriptedPoller::new(vec![Err(node_session_rejected())]));
        let (_stop, stop_rx) = tokio::sync::watch::channel(false);
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            run_workspace_bind_root_commands(poller.clone(), |_: &RootCommand| Ok(false), stop_rx),
        )
        .await
        .expect("a non-retryable RPC rejection must terminate the loop");
        assert!(result.is_err());
        assert_eq!(poller.calls(), 1);
    }

    /// A call broken after it was sent has no AFS detail and arrives as a
    /// framework status; it is still a transport failure and must be retried.
    #[tokio::test(start_paused = true)]
    async fn workspace_bind_root_command_inflight_disconnect_retries() {
        let poller = Arc::new(ScriptedPoller::new(vec![
            Err(remote_status(
                afs_error::ErrorKind::Internal,
                "h2 protocol error: stream reset",
            )),
            Ok(events(0, 1, vec![])),
        ]));
        let (stop, stop_rx) = tokio::sync::watch::channel(false);
        let handle = tokio::spawn(run_workspace_bind_root_commands(
            poller.clone(),
            |_: &RootCommand| Ok(false),
            stop_rx,
        ));

        while poller.calls() == 0 {
            tokio::task::yield_now().await;
        }
        // Sleeping advances the paused clock, so the retry delay can elapse.
        let not_retried = std::time::Instant::now() + Duration::from_secs(2);
        while poller.calls() < 2 {
            assert!(
                std::time::Instant::now() < not_retried,
                "an in-flight disconnect must be retried"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        stop.send(true).expect("stop receiver must be alive");
        assert!(handle.await.expect("control loop task").is_ok());
    }

    /// A framework status that rejects the request itself is not a transport
    /// failure and must stay fatal.
    #[tokio::test(start_paused = true)]
    async fn workspace_bind_root_command_remote_rejection_is_fatal() {
        let poller = Arc::new(ScriptedPoller::new(vec![Err(remote_status(
            afs_error::ErrorKind::InvalidArgument,
            "invalid request",
        ))]));
        let (_stop, stop_rx) = tokio::sync::watch::channel(false);
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            run_workspace_bind_root_commands(poller.clone(), |_: &RootCommand| Ok(false), stop_rx),
        )
        .await
        .expect("a remote rejection must terminate the loop");
        assert!(result.is_err());
        assert_eq!(poller.calls(), 1);
    }

    /// An untrusted control result stays fatal on the first poll, without retry.
    #[tokio::test(start_paused = true)]
    async fn workspace_bind_root_command_untrusted_batch_is_fatal_without_retry() {
        let poller = Arc::new(ScriptedPoller::new(vec![Ok(events(9, 31, vec![]))]));
        let (_stop, stop_rx) = tokio::sync::watch::channel(false);
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            run_workspace_bind_root_commands(poller.clone(), |_: &RootCommand| Ok(false), stop_rx),
        )
        .await
        .expect("an untrusted control result must terminate the loop");
        assert!(result.is_err());
        assert_eq!(poller.calls(), 1);
    }
}
