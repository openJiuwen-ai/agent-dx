//! Node assembly. `run_node` only orchestrates; each stage lives in the file
//! that owns it (`common`, `dfs`, `ownerfs`, `grpc`, `shutdown`).
//!
//! Startup order (a contract: no ingress before registration, no mount before
//! the local API, no service before the mounts):
//!
//! 1. Metrics; bind the gRPC and REST TCP listeners; session ID; Meta
//!    endpoint and advertised endpoint checks.
//! 2. DFS storage: read-work metrics, outboard budget, the local ChunkStore.
//! 3. The Node descriptor (capabilities, the ChunkStore's device).
//! 4. First Meta registration, bounded retry, cancelled by SIGINT/SIGTERM.
//! 5. DFS read authority and shutdown flag.
//! 6. The Peer connection pool.
//! 7. OwnerFs (RootManager, RDMA probe, MountManager reconciliation).
//! 8. DFS Meta adapter, RDMA pool, replica data plane, write buffers.
//! 9. The `DistributedFs` instance.
//! 10. Diagnostic storage, RDMA session tables, the local UDS API.
//! 11. FUSE mounts: OwnerFs, then DFS.
//! 12. Mount identities, `NodeReadiness`, the shared `Node`.
//! 13. Services, in this spawn order: T1 readiness sampler, T2 Meta heartbeat,
//!     T3 OwnerFs peer reaper, T4 GC reaper, T5 lease keeper, T6 DFS tick,
//!     T7 local API watch, T8 gRPC, T9 repair, T10 REST, T11 RDMA session
//!     sweep, T13 workspace bind-root commands (`background` lists them).
//! 14. Serve, then the shutdown sequence (`shutdown`).

pub(super) mod common;
#[cfg(feature = "dfs")]
pub(super) mod dfs;
mod grpc;
#[cfg(feature = "ownerfs")]
mod outlet;
#[cfg(feature = "ownerfs")]
mod ownerfs;
mod shutdown;

use super::{Node, NodeReadiness, background, readiness, registration};
use crate::{
    config::Config,
    runtime::{BoxError, Observability, Services},
};
use std::{sync::Arc, time::Duration};

/// 本机 SDK 的 DFS 命名空间。meta 适配器、DFS 实例与 local API 三处必须用同一个值，
/// 散写字符串会漂移成「SDK 连到另一个命名空间」。
const DEFAULT_DFS_NAMESPACE: &str = "default";

pub(super) async fn run_node(
    cfg: Config,
    obs: Observability,
    on_shutdown: impl FnOnce(),
) -> Result<(), BoxError> {
    // 1–2. Bind all TCP ingress before spawning services. A failed bind
    // cannot leave a half-ready Node. Storage opens before registration.
    let metrics = common::FsMetrics::register(&obs)?;
    let ingress = common::TcpIngress::bind(&cfg).await?;
    let startup = common::Startup::resolve(&cfg)?;
    let storage = common::NodeStorage::open(&cfg, &obs)?;
    // 3–4. No FUSE mount or local API exists yet.
    let node_descriptor = common::node_descriptor(&cfg, &startup, &storage);
    let registration =
        common::InitialRegistration::register(&cfg, &startup, node_descriptor.as_ref()).await?;
    // 5–9.
    let mut backends =
        common::Backends::assemble(&cfg, &obs, &startup, metrics, storage, &registration).await?;
    #[cfg(feature = "dfs")]
    let connector_routes = if let Some(dfs) = backends.dfs.instance.as_ref() {
        let backend: Arc<dyn super::vfs::Backend> = dfs.clone();
        let root = backend.root_inode();
        backends
            .dfs
            .connectors
            .http_routes(super::connectors::ConnectorContext { backend, root })?
    } else {
        axum::Router::new()
    };
    #[cfg(not(feature = "dfs"))]
    let connector_routes = axum::Router::new();
    // 10–12.
    let (endpoints, local) = common::LocalEndpoints::serve(&cfg, &obs, &backends).await?;
    let (mounts, local) = common::FuseMounts::mount(&cfg, &backends, local).await?;
    let (readiness, state) =
        common::node_state(&cfg, obs, &startup, &registration, &mounts, &mut backends);
    // 13. Services.
    let mut services = Services::new();
    // T13a/T13b: the outlet mirror. The mirror's startup gates run here,
    // before anything is spawned, so a misconfigured name layer aborts the
    // process instead of looping.
    #[cfg(feature = "ownerfs")]
    {
        outlet::start_mirror(&cfg, &backends.ownerfs, &mut services)?;
    }
    start_background(
        &cfg,
        &state,
        &readiness,
        node_descriptor,
        &registration,
        &backends,
        &mut services,
    );
    // local API 自己持有 JoinHandle；这里监控它，避免 UDS 已死而 TCP 健康检查仍成功。
    background::LocalApiWatch::new(
        local
            .abort_handle()
            .expect("local API task exists after startup"),
    )
    .start(&mut services);
    grpc::start(
        &cfg,
        ingress.grpc,
        &state,
        &endpoints,
        &backends,
        registration.epoch,
        &mut services,
    )?;
    #[cfg(feature = "dfs")]
    dfs::start_scrub(
        &cfg,
        &backends.dfs,
        &state.observability.registry,
        &mut services,
    )?;
    #[cfg(feature = "dfs")]
    dfs::start_repair(
        &cfg,
        &state.session_id,
        &mut backends.dfs,
        registration.epoch,
        &mut services,
    );
    background::RestServer::new(ingress.rest, state.clone(), connector_routes).start(&mut services);
    #[cfg(feature = "ownerfs")]
    if let Some(listener) = ingress.workspace {
        let control = backends.ownerfs.outlet_control.as_ref().ok_or_else(|| {
            afs_error::Error::coded(afs_error::CONFIG_INVALID, "Workspace API requires Meta")
        })?;
        let api = crate::node::api::workspace::WorkspaceApi::new(
            state.clone(),
            control.meta.clone(),
            backends.peer_connections.clone(),
        )?;
        let stop = services.stop.subscribe();
        services.spawn(async move {
            axum::serve(listener, api.router())
                .with_graceful_shutdown(crate::runtime::cancelled(stop))
                .await
                .map_err(Into::into)
        });
    }
    endpoints.into_sweeper().start(&mut services);
    // Multi-workspace bind mount is handled by MountManager via mkdir triggers.
    // No single-workspace startup worker is needed.
    #[cfg(feature = "ownerfs")]
    ownerfs::start_workspace_bind_root_commands(&mut backends.ownerfs, &mut services);
    // 14.
    let resources = shutdown::ShutdownResources {
        state: &state,
        backends,
        mounts,
        local,
    };
    shutdown::serve_until_shutdown(&cfg, services, resources, on_shutdown).await
}

/// T1–T6, in their original spawn order.
fn start_background(
    cfg: &Config,
    state: &Node,
    readiness: &Arc<NodeReadiness>,
    node_descriptor: Option<(&str, afs_protocol::meta::NodeDescriptor)>,
    registration: &common::InitialRegistration,
    backends: &common::Backends,
    services: &mut Services,
) {
    readiness::ReadinessSampler::new(readiness.clone(), cfg.data_dir.clone()).start(services);
    if let Some((endpoint, descriptor)) = node_descriptor {
        registration::MetaHeartbeat {
            endpoint: endpoint.to_owned(),
            descriptor,
            timeout: Duration::from_millis(cfg.timeout_ms),
            tls: cfg.tls_config(),
            readiness: readiness.clone(),
            registered_node_epoch: registration.epoch,
            #[cfg(feature = "dfs")]
            read_authority: backends.dfs.read.authority.clone(),
            #[cfg(feature = "dfs")]
            shutting_down: Arc::clone(&backends.dfs.read.shutting_down),
        }
        .start(services);
    }
    #[cfg(feature = "ownerfs")]
    if let Some(ownerfs) = state.ownerfs.clone() {
        ownerfs::OwnerPeerReaper::new(ownerfs).start(services);
    }
    #[cfg(feature = "dfs")]
    if let Some(dfs) = state.dfs.clone() {
        dfs::start_background(cfg, dfs, &backends.dfs, services);
    }
    #[cfg(not(any(feature = "ownerfs", feature = "dfs")))]
    let _ = state;
    #[cfg(not(feature = "dfs"))]
    let _ = backends;
}
