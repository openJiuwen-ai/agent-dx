//! Shared Node assembly stages and the value objects that carry their results
//! to later stages: metrics and TCP ingress (段 1), the Meta descriptor and
//! first registration (段 3–4), the backends (段 5–9, through `dfs` and
//! `ownerfs`), the diagnostic chain and local API (段 10), FUSE mounts (段 11)
//! and the shared `Node` state (段 12).

use super::DEFAULT_DFS_NAMESPACE;
#[cfg(feature = "dfs")]
use super::dfs;
#[cfg(feature = "ownerfs")]
use super::ownerfs;
#[cfg(any(feature = "ownerfs", feature = "dfs"))]
use crate::node::fuse;
use crate::{
    config::Config,
    node::{
        AfsMountIdentity, Node, NodeReadiness, api, background, diagnostics,
        registration::{
            initial_registration_budget, initial_shutdown_signal, retry_initial_registration,
        },
        rpc, vfs,
    },
    runtime::{BoxError, Observability},
};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

/// Metrics registered before any listener is bound (段 1).
pub(super) struct FsMetrics {
    #[cfg(feature = "ownerfs")]
    pub(super) owner_rpc: rpc::OwnerRpcMetrics,
    #[cfg(any(feature = "ownerfs", feature = "dfs"))]
    pub(super) fuse_requests: fuse::FuseRequestMetrics,
}

impl FsMetrics {
    pub(super) fn register(obs: &Observability) -> Result<Self, BoxError> {
        #[cfg(feature = "ownerfs")]
        let owner_rpc_metrics = rpc::OwnerRpcMetrics::register(&obs.registry)?;
        #[cfg(any(feature = "ownerfs", feature = "dfs"))]
        let fuse_request_metrics = fuse::FuseRequestMetrics::register(&obs.registry)?;
        #[cfg(not(any(feature = "ownerfs", feature = "dfs")))]
        let _ = obs;
        Ok(Self {
            #[cfg(feature = "ownerfs")]
            owner_rpc: owner_rpc_metrics,
            #[cfg(any(feature = "ownerfs", feature = "dfs"))]
            fuse_requests: fuse_request_metrics,
        })
    }
}

/// Both TCP listeners, bound before anything is spawned (段 1).
pub(super) struct TcpIngress {
    pub(super) grpc: tokio::net::TcpListener,
    pub(super) rest: tokio::net::TcpListener,
}

impl TcpIngress {
    pub(super) async fn bind(cfg: &Config) -> Result<Self, BoxError> {
        // Bind all TCP ingress before spawning services. A failed bind cannot leave a half-ready Node.
        let grpc = tokio::net::TcpListener::bind(cfg.grpc_listen).await?;
        let rest = tokio::net::TcpListener::bind(cfg.rest_listen).await?;
        Ok(Self { grpc, rest })
    }
}

/// Process session and Meta endpoints (段 1).
pub(super) struct Startup<'a> {
    pub(super) session_id: String,
    pub(super) timeout: Duration,
    pub(super) needs_meta: bool,
    pub(super) meta_endpoint: Option<&'a str>,
    pub(super) advertised: Option<String>,
}

impl<'a> Startup<'a> {
    pub(super) fn resolve(cfg: &'a Config) -> Result<Self, BoxError> {
        // OwnerFs 生产路径必须先建立 Meta 会话和本机普通文件后端，不能挂载
        // 这里的 session ID 来自 Linux 内核随机源，也用于隔离 DFS 操作身份。
        let session_id = std::fs::read_to_string("/proc/sys/kernel/random/uuid")?
            .trim()
            .to_owned();
        let timeout = std::time::Duration::from_millis(cfg.timeout_ms);
        let needs_meta = cfg.ownerfs || cfg.dfs;
        let meta_endpoint = if needs_meta {
            Some(cfg.meta_endpoint.as_deref().ok_or_else(|| {
                afs_error::Error::coded(
                    afs_error::CONFIG_INVALID,
                    "OwnerFs and DFS require meta_endpoint",
                )
            })?)
        } else {
            None
        };
        let advertised = if needs_meta {
            Some(match cfg.advertise_endpoint.clone() {
                Some(value) => value,
                None if !cfg.grpc_listen.ip().is_unspecified() => {
                    format!("http://{}", cfg.grpc_listen)
                }
                None => {
                    return Err(afs_error::Error::coded(
                        afs_error::CONFIG_INVALID,
                        "Node listening on an unspecified address requires advertise_endpoint",
                    )
                    .into());
                }
            })
        } else {
            None
        };
        Ok(Self {
            session_id,
            timeout,
            needs_meta,
            meta_endpoint,
            advertised,
        })
    }
}

/// Storage opened before registration (段 2).
pub(super) struct NodeStorage {
    #[cfg(feature = "dfs")]
    pub(super) dfs: dfs::DfsStorage,
}

impl NodeStorage {
    pub(super) fn open(cfg: &Config, obs: &Observability) -> Result<Self, BoxError> {
        #[cfg(not(feature = "dfs"))]
        let _ = (cfg, obs);
        Ok(Self {
            #[cfg(feature = "dfs")]
            dfs: dfs::DfsStorage::open(cfg, obs)?,
        })
    }

    fn storage_devices(&self) -> Vec<afs_protocol::meta::DfsStorageDevice> {
        #[cfg(feature = "dfs")]
        {
            self.dfs.storage_devices()
        }
        #[cfg(not(feature = "dfs"))]
        {
            Vec::new()
        }
    }
}

/// The descriptor registered with Meta (段 3); `None` when no backend needs
/// Meta. Storage devices are read only when a descriptor is built.
pub(super) fn node_descriptor<'a>(
    cfg: &Config,
    startup: &Startup<'a>,
    storage: &NodeStorage,
) -> Option<(&'a str, afs_protocol::meta::NodeDescriptor)> {
    let session_id = &startup.session_id;
    let advertised = &startup.advertised;
    startup.meta_endpoint.map(|endpoint| {
        let advertised = advertised
            .clone()
            .expect("needs_meta sets advertised endpoint");
        let mut capabilities = Vec::new();
        if cfg.ownerfs {
            capabilities.push("ownerfs".into());
        }
        if cfg.dfs {
            capabilities.push("dfs".into());
        }
        (
            endpoint,
            afs_protocol::meta::NodeDescriptor {
                node_id: cfg.id.clone(),
                endpoint: Some(afs_protocol::meta::NodeEndpoint {
                    grpc_addr: advertised.clone(),
                    data_addr: advertised,
                    rest_addr: format!("http://{}", cfg.rest_listen),
                }),
                labels: std::collections::HashMap::new(),
                capabilities,
                session_id: session_id.clone(),
                storage_devices: storage.storage_devices(),
            },
        )
    })
}

/// Result of the first registration (段 4).
pub(super) struct InitialRegistration {
    pub(super) epoch: u64,
    pub(super) at: Option<Instant>,
    pub(super) readiness: Option<rpc::meta::NodeRegistrationReadiness>,
}

impl InitialRegistration {
    pub(super) async fn register(
        cfg: &Config,
        startup: &Startup<'_>,
        node_descriptor: Option<&(&str, afs_protocol::meta::NodeDescriptor)>,
    ) -> Result<Self, BoxError> {
        let timeout = startup.timeout;
        let (registered_node_epoch, registered_node_at, registered_readiness) =
            if let Some((endpoint, descriptor)) = node_descriptor {
                // No FUSE mount or local API has been exposed yet. A bounded retry
                // absorbs the valid cold-start window in which Meta has been
                // spawned but is still opening its durable backend. Runtime
                // heartbeat/drain failures retain their stricter fail-closed path.
                let registration = retry_initial_registration(
                    || {
                        rpc::meta::register_node_with_readiness(
                            endpoint,
                            descriptor.clone(),
                            timeout,
                            cfg.tls_config(),
                        )
                    },
                    initial_shutdown_signal(),
                    initial_registration_budget(timeout),
                )
                .await?;
                (
                    registration.lease_epoch,
                    Some(Instant::now()),
                    Some(registration),
                )
            } else {
                (0, None, None)
            };
        Ok(Self {
            epoch: registered_node_epoch,
            at: registered_node_at,
            readiness: registered_readiness,
        })
    }
}

/// The filesystem backends and what they share (段 5–9).
pub(super) struct Backends {
    #[cfg(any(feature = "ownerfs", feature = "dfs"))]
    pub(super) metrics: FsMetrics,
    #[cfg(feature = "dfs")]
    pub(super) timeout: Duration,
    /// Held until the Node exits, as the pool was before the split; the
    /// backends dial through their own clones.
    #[cfg(any(feature = "ownerfs", feature = "dfs"))]
    _peer_connections: Arc<rpc::peer::PeerConnectionPool>,
    #[cfg(feature = "ownerfs")]
    pub(super) ownerfs: ownerfs::OwnerFsAssembly,
    #[cfg(feature = "dfs")]
    pub(super) dfs: dfs::DfsAssembly,
}

impl Backends {
    /// 段 5 DFS read admission, 段 6 Peer pool, 段 7 OwnerFs, 段 8–9 DFS.
    pub(super) async fn assemble(
        cfg: &Config,
        obs: &Observability,
        startup: &Startup<'_>,
        metrics: FsMetrics,
        storage: NodeStorage,
        registration: &InitialRegistration,
    ) -> Result<Self, BoxError> {
        #[cfg(feature = "dfs")]
        let dfs_read = dfs::DfsReadAdmission::new(cfg, registration)?;
        #[cfg(any(feature = "ownerfs", feature = "dfs"))]
        let peer_connections = peer_connections(cfg, startup, &storage)?;
        #[cfg(feature = "ownerfs")]
        let ownerfs =
            ownerfs::assemble(cfg, startup, &peer_connections, &metrics.owner_rpc).await?;
        #[cfg(feature = "dfs")]
        let dfs = dfs::DfsAssembly::assemble(
            cfg,
            obs,
            startup,
            storage.dfs,
            dfs_read,
            registration.epoch,
            &peer_connections,
        )
        .await?;
        #[cfg(not(feature = "dfs"))]
        let _ = (obs, storage, registration);
        #[cfg(not(any(feature = "ownerfs", feature = "dfs")))]
        let _ = (cfg, startup);
        #[cfg(not(any(feature = "ownerfs", feature = "dfs")))]
        let _ = metrics;
        Ok(Self {
            #[cfg(any(feature = "ownerfs", feature = "dfs"))]
            metrics,
            #[cfg(feature = "dfs")]
            timeout: startup.timeout,
            #[cfg(any(feature = "ownerfs", feature = "dfs"))]
            _peer_connections: peer_connections,
            #[cfg(feature = "ownerfs")]
            ownerfs,
            #[cfg(feature = "dfs")]
            dfs,
        })
    }
}

/// 段 6: the Peer connection pool both backends dial through.
#[cfg(any(feature = "ownerfs", feature = "dfs"))]
fn peer_connections(
    cfg: &Config,
    startup: &Startup<'_>,
    storage: &NodeStorage,
) -> Result<Arc<rpc::peer::PeerConnectionPool>, BoxError> {
    let timeout = startup.timeout;
    let peer_connections = rpc::peer::PeerConnectionPool::new(
        afs_transport::grpc::GrpcConfig {
            connect_timeout: timeout,
            request_timeout: timeout,
            ..Default::default()
        },
        cfg.tls_config(),
        64,
    )?;
    #[cfg(feature = "dfs")]
    let peer_connections = peer_connections
        .with_read_work_metrics(storage.dfs.read_work_metrics.clone())
        .with_read_crc_mode(cfg.dfs_read_crc_mode);
    #[cfg(not(feature = "dfs"))]
    let _ = storage;
    Ok(Arc::new(peer_connections))
}

/// Diagnostic chain and RDMA session tables (段 10); the local API itself is
/// returned beside it because shutdown consumes it.
pub(super) struct LocalEndpoints {
    pub(super) diagnostic_storage: Arc<diagnostics::DiagnosticStorage>,
    pub(super) sessions: rpc::control::RdmaSessionRegistry,
    #[cfg(feature = "ownerfs")]
    pub(super) owner_rdma_sessions: rpc::control::RdmaSessionRegistry,
}

impl LocalEndpoints {
    pub(super) async fn serve(
        cfg: &Config,
        obs: &Observability,
        backends: &Backends,
    ) -> Result<(Self, api::local::LocalApiServer), BoxError> {
        // diagnostics is a separate test object directory, not an OwnerFs or DFS data path.
        let diagnostic_storage = Arc::new(diagnostics::DiagnosticStorage::new(
            cfg.data_dir.join("diagnostics"),
        )?);
        let sessions = rpc::control::RdmaSessionRegistry::new(cfg.rdma_device.clone());
        #[cfg(feature = "ownerfs")]
        let owner_rdma_sessions = rpc::control::RdmaSessionRegistry::new(cfg.rdma_device.clone());
        // 本机 SDK 的数据面直接调用已装配的 DFS 后端；未启用 DFS 时保持 None，
        // 让 DfsLocalData 明确返回 UNIMPLEMENTED，而不是退回 diagnostics 目录。
        #[cfg(feature = "dfs")]
        let dfs_backend: Option<Arc<dyn vfs::Backend>> = backends.dfs.backend();
        #[cfg(not(feature = "dfs"))]
        let dfs_backend: Option<Arc<dyn vfs::Backend>> = None;
        let local = api::local::serve_local_api_with_options(
            diagnostic_storage.clone(),
            &cfg.uds_path,
            api::local::LocalApiOptions {
                metrics_registry: Some(obs.registry.clone()),
                dfs_backend,
                // 与上面 DFS 后端构造时使用的命名空间保持一致。
                dfs_namespace: Some(DEFAULT_DFS_NAMESPACE.to_owned()),
            },
        )
        .await?;
        #[cfg(not(feature = "dfs"))]
        let _ = backends;
        let endpoints = Self {
            diagnostic_storage,
            sessions,
            #[cfg(feature = "ownerfs")]
            owner_rdma_sessions,
        };
        Ok((endpoints, local))
    }

    /// T11 over both session tables; the tables are not used after it starts.
    pub(super) fn into_sweeper(self) -> background::RdmaSessionSweeper {
        background::RdmaSessionSweeper {
            sessions: self.sessions,
            #[cfg(feature = "ownerfs")]
            owner_sessions: self.owner_rdma_sessions,
        }
    }
}

/// FUSE sessions (段 11), joined by the shutdown sequence.
pub(super) struct FuseMounts {
    #[cfg(feature = "ownerfs")]
    pub(super) ownerfs: Option<fuse::MountedFuse>,
    #[cfg(feature = "dfs")]
    pub(super) dfs: Option<fuse::MountedFuse>,
}

impl FuseMounts {
    /// OwnerFs first, then DFS. A failed mount drops the mounts made so far and
    /// shuts the local API down before the error returns.
    pub(super) async fn mount(
        cfg: &Config,
        backends: &Backends,
        local: api::local::LocalApiServer,
    ) -> Result<(Self, api::local::LocalApiServer), BoxError> {
        #[cfg(feature = "ownerfs")]
        let (mounted_ownerfs, local) = ownerfs::mount_fuse(
            cfg,
            &backends.ownerfs.instance,
            &backends.metrics.fuse_requests,
            local,
        )
        .await?;
        #[cfg(feature = "dfs")]
        let fuse_request_metrics = &backends.metrics.fuse_requests;
        #[cfg(feature = "dfs")]
        let dfs_read_work_metrics = &backends.dfs.storage.read_work_metrics;
        #[cfg(feature = "dfs")]
        let dfs_instance = &backends.dfs.instance;
        #[cfg(feature = "dfs")]
        let mounted_dfs = match (&cfg.dfs_mount, &dfs_instance) {
            (Some(path), Some(dfs)) => {
                match fuse::mount_dfs_with_diagnostics_and_fd_bridge(
                    dfs.clone(),
                    path,
                    fuse_request_metrics.clone(),
                    dfs_read_work_metrics.clone(),
                    fuse::FuseMountOptions::from(cfg),
                    local.fd_bridge().ok_or_else(|| {
                        afs_error::Error::coded(
                            afs_error::NODE_VFS_UNAVAILABLE,
                            "DFS fd bridge missing",
                        )
                    })?,
                ) {
                    Ok(session) => Some(session),
                    Err(error) => {
                        #[cfg(feature = "ownerfs")]
                        drop(mounted_ownerfs);
                        local.shutdown().await?;
                        return Err(error.into());
                    }
                }
            }
            (Some(_), None) => {
                #[cfg(feature = "ownerfs")]
                drop(mounted_ownerfs);
                local.shutdown().await?;
                return Err(afs_error::Error::coded(
                    afs_error::CONFIG_INVALID,
                    "dfs_mount requires the DFS backend",
                )
                .into());
            }
            (None, _) => None,
        };
        #[cfg(not(any(feature = "ownerfs", feature = "dfs")))]
        let _ = (cfg, backends);
        let mounts = Self {
            #[cfg(feature = "ownerfs")]
            ownerfs: mounted_ownerfs,
            #[cfg(feature = "dfs")]
            dfs: mounted_dfs,
        };
        Ok((mounts, local))
    }
}

/// Mount identities, `NodeReadiness` and the shared `Node` (段 12). The
/// backend instances move from `backends` into `Node`.
pub(super) fn node_state(
    cfg: &Config,
    obs: Observability,
    startup: &Startup<'_>,
    registration: &InitialRegistration,
    mounts: &FuseMounts,
    backends: &mut Backends,
) -> (Arc<NodeReadiness>, Arc<Node>) {
    let needs_meta = startup.needs_meta;
    let registered_node_epoch = registration.epoch;
    let registered_node_at = registration.at;
    let registered_readiness = &registration.readiness;
    #[cfg(feature = "ownerfs")]
    let mounted_ownerfs = &mounts.ownerfs;
    #[cfg(feature = "dfs")]
    let mounted_dfs = &mounts.dfs;
    #[cfg(not(any(feature = "ownerfs", feature = "dfs")))]
    let _ = (mounts, &backends);
    #[cfg(feature = "ownerfs")]
    let ownerfs_mount_identity = mounted_ownerfs
        .as_ref()
        .map(|mounted| AfsMountIdentity::from_mounted(mounted, "afs-ownerfs"));
    #[cfg(not(feature = "ownerfs"))]
    let ownerfs_mount_identity: Option<AfsMountIdentity> = None;
    #[cfg(feature = "dfs")]
    let dfs_mount_identity = mounted_dfs
        .as_ref()
        .map(|mounted| AfsMountIdentity::from_mounted(mounted, "afs-dfs"));
    #[cfg(not(feature = "dfs"))]
    let dfs_mount_identity: Option<AfsMountIdentity> = None;
    let readiness = Arc::new(NodeReadiness::new_with_mount_identities(
        needs_meta,
        if needs_meta {
            Some(registered_node_epoch)
        } else {
            None
        },
        registered_node_at,
        ownerfs_mount_identity,
        dfs_mount_identity,
        &cfg.data_mode,
        cfg.rdma_device.as_deref(),
    ));
    readiness.set_allow_volatile_meta(cfg.allow_volatile_meta);
    if let Some(registration) = &registered_readiness {
        readiness.note_initial_registration_capability(registration);
    }

    let state = Arc::new(Node {
        config: cfg.clone(),
        observability: obs,
        session_id: startup.session_id.clone(),
        readiness: readiness.clone(),
        #[cfg(feature = "dfs")]
        read_work_metrics: backends.dfs.storage.read_work_metrics.clone(),
        #[cfg(feature = "ownerfs")]
        ownerfs: backends.ownerfs.instance.take(),
        #[cfg(feature = "dfs")]
        dfs: backends.dfs.instance.take(),
    });
    (readiness, state)
}
