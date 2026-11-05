//! DFS share of the Node assembly: storage opened before registration (段 2),
//! the read admission (段 5), the Meta adapter, data planes and write buffers
//! (段 8), the `DistributedFs` instance (段 9), and the DFS background and
//! repair services. Shutdown reads the handles kept here.

use super::{
    DEFAULT_DFS_NAMESPACE,
    common::{InitialRegistration, Startup},
};
use crate::{
    config::Config,
    node::{background, chunk, external_chunk, fault, gc, metrics, read, replication, rpc, vfs},
    runtime::{BoxError, Observability, Services},
};
use std::{
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};

/// DFS storage opened before Node registration (段 2): the descriptor carries
/// the local ChunkStore's device.
pub(super) struct DfsStorage {
    pub(super) read_work_metrics: metrics::work::ReadWorkMetrics,
    pub(super) outboard_budget: Arc<chunk::outboard::OutboardBudget>,
    /// Consumed by the repair service.
    pub(super) local_chunk_store: Option<Arc<chunk::LocalChunkStore>>,
    /// The same store, kept for the shutdown sequence.
    pub(super) chunk_store_for_shutdown: Option<Arc<chunk::LocalChunkStore>>,
}

impl DfsStorage {
    pub(super) fn open(cfg: &Config, obs: &Observability) -> Result<Self, BoxError> {
        let dfs_read_work_metrics = metrics::work::ReadWorkMetrics::register(&obs.registry)?
            .with_read_diagnostics(cfg.dfs_read_diagnostics_enabled);
        // A single metadata ledger follows outboards across builders, local
        // readers, Peer frames and the disposable disk cache. Payload and existing
        // transport windows remain separately bounded by their original limits.
        let dfs_outboard_budget = chunk::outboard::OutboardBudget::new_with_metrics_and_work_wait(
            usize::try_from(read::peer_cache::MAX_PEER_CACHE_PROOF_BYTES)?,
            dfs_read_work_metrics.clone(),
            std::time::Duration::from_millis(cfg.dfs_outboard_work_wait_ms),
        );
        let local_chunk_store = if cfg.dfs {
            Some(Arc::new(
                chunk::LocalChunkStore::open(cfg.data_dir.join("dfs"), cfg.id.clone())?
                    .with_outboard_budget(dfs_outboard_budget.clone())
                    .with_peer_serve_config(
                        cfg.dfs_peer_serve_resident_objects,
                        cfg.dfs_peer_serve_payload_bytes,
                    )?
                    .with_peer_serve_wait(std::time::Duration::from_millis(
                        cfg.dfs_peer_serve_wait_ms,
                    ))?
                    .with_read_work_metrics(dfs_read_work_metrics.clone())
                    .with_peer_read_crc_mode(cfg.dfs_read_crc_mode)
                    .with_sidecar_install_queue(cfg.dfs_sidecar_install_queue)?
                    .with_catalog_commit_queue(cfg.dfs_catalog_commit_queue)?,
            ))
        } else {
            None
        };
        let dfs_sidecar_store_for_shutdown = local_chunk_store.clone();
        Ok(Self {
            read_work_metrics: dfs_read_work_metrics,
            outboard_budget: dfs_outboard_budget,
            local_chunk_store,
            chunk_store_for_shutdown: dfs_sidecar_store_for_shutdown,
        })
    }

    pub(super) fn storage_devices(&self) -> Vec<afs_protocol::meta::DfsStorageDevice> {
        self.local_chunk_store
            .as_ref()
            .map(|store| store.device_descriptor())
            .transpose()
            .expect("local ChunkStore was opened before Node registration")
            .into_iter()
            .map(|device| afs_protocol::meta::DfsStorageDevice {
                device_id: device.device_id,
                device_epoch: device.device_epoch,
                catalog_revision: device.catalog_revision,
                failure_domain: device.failure_domain,
            })
            .collect()
    }
}

/// DFS read admission (段 5): the running read authority and the flag the
/// shutdown sequence raises before it stops the authority.
pub(super) struct DfsReadAdmission {
    pub(super) authority: Option<Arc<read::engine::LocalReadAuthority>>,
    pub(super) shutting_down: Arc<AtomicBool>,
}

impl DfsReadAdmission {
    pub(super) fn new(cfg: &Config, registration: &InitialRegistration) -> Result<Self, BoxError> {
        let registered_readiness = &registration.readiness;
        let dfs_read_authority = if cfg.dfs {
            let registration = registered_readiness.as_ref().ok_or_else(|| {
                afs_error::Error::coded(
                    afs_error::IO_PERMISSION_DENIED,
                    "DFS reads require a registered Node session",
                )
            })?;
            Some(Arc::new(read::engine::LocalReadAuthority::new(
                registration.lease_epoch,
                registration.session_deadline,
            )))
        } else {
            None
        };
        let dfs_shutting_down = Arc::new(AtomicBool::new(false));
        Ok(Self {
            authority: dfs_read_authority,
            shutting_down: dfs_shutting_down,
        })
    }
}

/// Data-mode resources the instance build shares with the replica plane.
struct DfsTransport {
    data_mode: rpc::peer::DataMode,
    rdma_pool: Option<Arc<rpc::peer::DfsRdmaPool>>,
}

/// Everything the DFS assembly hands to the later stages (段 8–9). The
/// `Option` handles stay `None` when `cfg.dfs` is off.
pub(super) struct DfsAssembly {
    pub(super) storage: DfsStorage,
    pub(super) read: DfsReadAdmission,
    pub(super) meta: Option<Arc<rpc::meta::GrpcDfsMeta>>,
    pub(super) replica_plane: Arc<dyn replication::ReplicaDataPlane>,
    pub(super) write_buffers: Arc<chunk::WriteBufferPool>,
    pub(super) external_store: Option<Arc<external_chunk::S3ChunkStore>>,
    pub(super) peer_cache: Option<Arc<read::peer_cache::DiskPeerCache>>,
    pub(super) fetcher: Option<Arc<read::object_fetch::ObjectFetcher>>,
    pub(super) gc_collector: Option<Arc<gc::Collector>>,
    /// Moved into `Node` once the mounts exist.
    pub(super) instance: Option<Arc<vfs::dfs::DistributedFs>>,
}

impl DfsAssembly {
    /// 段 8 (Meta adapter, RDMA pool, replica data plane, write buffers), then
    /// 段 9 when `cfg.dfs` is on.
    pub(super) async fn assemble(
        cfg: &Config,
        obs: &Observability,
        startup: &Startup<'_>,
        storage: DfsStorage,
        read: DfsReadAdmission,
        registered_node_epoch: u64,
        peer_connections: &Arc<rpc::peer::PeerConnectionPool>,
    ) -> Result<Self, BoxError> {
        let meta_endpoint = startup.meta_endpoint;
        let session_id = &startup.session_id;
        let timeout = startup.timeout;
        let dfs_read_work_metrics = &storage.read_work_metrics;
        let dfs_outboard_budget = &storage.outboard_budget;
        let dfs_meta = if cfg.dfs {
            Some(Arc::new(
                rpc::meta::GrpcDfsMeta::new(
                    meta_endpoint.expect("DFS checked meta_endpoint"),
                    cfg.id.clone(),
                    session_id.clone(),
                    crate::dfs::NamespaceId::new(DEFAULT_DFS_NAMESPACE),
                    timeout,
                    cfg.tls_config(),
                )?
                .with_read_work_metrics(dfs_read_work_metrics.clone()),
            ))
        } else {
            None
        };
        let dfs_data_mode = parse_data_mode(&cfg.data_mode);
        let dfs_rdma_pool = if cfg.dfs {
            if let Some(device) =
                dfs_rdma_startup_device(dfs_data_mode, cfg.rdma_device.as_deref())?
            {
                let peers = peer_connections.clone();
                Some(Arc::new(
                    crate::node::bridge::spawn_blocking(move || {
                        rpc::peer::DfsRdmaPool::new(peers, device, timeout)
                    })
                    .await??,
                ))
            } else {
                None
            }
        } else {
            None
        };
        let dfs_replica_plane = rpc::peer::make_replica_data_plane(
            if cfg.dfs {
                dfs_data_mode
            } else {
                rpc::peer::DataMode::Grpc
            },
            peer_connections.clone(),
            timeout,
            dfs_rdma_pool.clone(),
        )?;
        let dfs_write_buffer_idle_bytes = usize::try_from(cfg.dfs_write_buffer_idle_bytes)
            .map_err(|_| {
                afs_error::Error::coded(
                    afs_error::CONFIG_INVALID,
                    "write buffer idle baseline does not fit this platform",
                )
            })?;
        let dfs_write_buffers = if cfg.dfs_external_backend.is_some() {
            // S3 objects keep no local BLAKE3 outboard, so S3 buffers carry no
            // outboard budget.
            chunk::WriteBufferPool::new(dfs_write_buffer_idle_bytes)
        } else {
            chunk::WriteBufferPool::new_with_outboard_budget(
                dfs_write_buffer_idle_bytes,
                dfs_outboard_budget.clone(),
            )
        };
        dfs_write_buffers.register_metrics(&obs.registry)?;
        let mut assembly = Self {
            storage,
            read,
            meta: dfs_meta,
            replica_plane: dfs_replica_plane,
            write_buffers: dfs_write_buffers,
            external_store: None,
            peer_cache: None,
            fetcher: None,
            gc_collector: None,
            instance: None,
        };
        if cfg.dfs {
            let transport = DfsTransport {
                data_mode: dfs_data_mode,
                rdma_pool: dfs_rdma_pool,
            };
            assembly
                .build_instance(
                    cfg,
                    obs,
                    startup,
                    registered_node_epoch,
                    peer_connections,
                    transport,
                )
                .await?;
        }
        Ok(assembly)
    }

    /// 段 9: S3 external store, read engine, Peer cache, GC references and the
    /// `DistributedFs` with its write pipeline.
    async fn build_instance(
        &mut self,
        cfg: &Config,
        obs: &Observability,
        startup: &Startup<'_>,
        registered_node_epoch: u64,
        peer_connections: &Arc<rpc::peer::PeerConnectionPool>,
        transport: DfsTransport,
    ) -> Result<(), BoxError> {
        let session_id = &startup.session_id;
        let timeout = startup.timeout;
        let DfsTransport {
            data_mode: dfs_data_mode,
            rdma_pool: dfs_rdma_pool,
        } = transport;
        let dfs_read_work_metrics = &self.storage.read_work_metrics;
        let dfs_read_authority = &self.read.authority;
        let dfs_replica_plane = &self.replica_plane;
        let dfs_write_buffers = &self.write_buffers;
        let namespace = crate::dfs::NamespaceId::new(DEFAULT_DFS_NAMESPACE);
        let meta = self
            .meta
            .as_ref()
            .expect("DFS Meta adapter was constructed")
            .clone();
        let chunks = self
            .storage
            .local_chunk_store
            .as_ref()
            .expect("DFS local ChunkStore was opened before registration")
            .clone();
        // The external backend owns durability. Never request placement or
        // synthesize local ReplicaAck records for an external object.
        let external_store = open_external_store(cfg, obs, timeout).await?;
        self.external_store = external_store.clone();
        let chunk_store: Arc<dyn chunk::ChunkSink> = match &external_store {
            Some(store) => store.clone(),
            None => Arc::new(
                replication::DfsChunkSink::new_with_epoch(
                    cfg.id.clone(),
                    registered_node_epoch,
                    chunks.clone(),
                    meta.clone(),
                    dfs_replica_plane.clone(),
                )
                .with_gc_control(meta.clone()),
            ),
        };
        let read_config = read::engine::DfsReadConfig {
            max_ops_per_batch: cfg.dfs_read_max_ops_per_batch,
            max_inflight_bytes: cfg.dfs_read_max_inflight_bytes,
            source_cache_ttl: std::time::Duration::from_millis(cfg.dfs_source_cache_max_ms),
            source_lookahead_chunks: cfg.dfs_source_lookahead_chunks,
            source_cache_bytes: cfg.dfs_source_cache_bytes,
            readahead_max_bytes: cfg.dfs_readahead_max_bytes,
        };
        read_config.validate()?;
        let read_engine = read::engine::DfsReadEngine::new(
            namespace.clone(),
            cfg.id.clone(),
            chunks.clone(),
            meta.clone(),
            rpc::peer::make_chunk_transfer(
                dfs_data_mode,
                peer_connections.clone(),
                timeout,
                dfs_rdma_pool.clone(),
            )?,
            read_config,
        );
        let read_engine = match &dfs_read_authority {
            Some(authority) => read_engine.with_read_authority(authority.clone()),
            None => read_engine,
        };
        let read_engine = if let Some(cache) = &cfg.dfs_peer_cache {
            let cache = open_peer_cache(cfg, cache, &namespace, &self.storage).await?;
            self.peer_cache = Some(cache.clone());
            read_engine.with_peer_cache(cache)
        } else {
            read_engine
        };
        let gc_references = external_store
            .is_none()
            .then(|| gc::GcReferences::new(meta.clone()));
        let read_engine = read_engine.with_read_pipeline(cfg.dfs_read_pipeline)?;
        let read_engine = Arc::new(match external_store {
            Some(store) => read_engine.with_external_backend(store, meta.clone()),
            None => read_engine,
        });
        self.fetcher = read_engine.object_fetcher();
        self.gc_collector = gc_references.as_ref().map(|references| {
            Arc::new(gc::Collector::new(
                references.clone(),
                chunks,
                read_engine.clone(),
                cfg.id.clone(),
            ))
        });
        let remote_factory = Arc::new(rpc::peer::GrpcDfsOwnerFactory::new(
            peer_connections.clone(),
            timeout,
        ));
        let fs = vfs::dfs::DistributedFs::new(
            namespace,
            cfg.id.clone(),
            session_id.clone(),
            meta,
            chunk_store,
            read_engine,
        )
        .with_gc_references(gc_references)
        .with_remote_owner_factory(remote_factory)
        .with_version_cache_capacity(cfg.dfs_version_cache_entries)
        .with_version_cache_bytes(cfg.dfs_version_cache_bytes)
        .with_write_buffer_pool(dfs_write_buffers.clone())
        .with_inode_lock_metrics(&obs.registry)?;
        // Every DFS store, local or S3, writes through the native pipeline. S3 has
        // only `MAX_UPLOADS` concurrent PUTs; deeper native upload queues would wait
        // on those permits inside the request timeout.
        let upload_inflight_chunks = if cfg.dfs_external_backend.is_some() {
            cfg.dfs_upload_inflight_chunks
                .min(external_chunk::MAX_UPLOADS)
        } else {
            cfg.dfs_upload_inflight_chunks
        };
        let fs = fs.with_write_pipeline(
            chunk::DirtyBudget::new(usize::try_from(cfg.dfs_dirty_budget_bytes).map_err(|_| {
                afs_error::Error::coded(
                    afs_error::CONFIG_INVALID,
                    "native dirty budget does not fit this platform",
                )
            })?),
            vfs::dfs::WritePipelineConfig {
                unpublished_descriptors: cfg.dfs_unpublished_descriptors,
                upload_inflight_chunks,
            },
            tokio::runtime::Handle::current(),
            Some(dfs_read_work_metrics.clone()),
        )?;
        self.instance = Some(Arc::new(fs));
        Ok(())
    }

    /// The instance as the local SDK data-plane backend (段 10).
    pub(super) fn backend(&self) -> Option<Arc<dyn vfs::Backend>> {
        self.instance
            .as_ref()
            .map(|backend| Arc::clone(backend) as Arc<dyn vfs::Backend>)
    }
}

/// The S3 external backend of 段 9, when configured.
async fn open_external_store(
    cfg: &Config,
    obs: &Observability,
    timeout: Duration,
) -> Result<Option<Arc<external_chunk::S3ChunkStore>>, BoxError> {
    let external_store = if let Some(backend) = &cfg.dfs_external_backend {
        let s3_metrics = external_chunk::S3ChunkStoreMetrics::register(&obs.registry)?;
        let options = external_chunk::S3ChunkStoreOptions {
            endpoint: backend.endpoint.clone(),
            bucket: backend.bucket.clone(),
            region: backend.region.clone(),
            store_id: backend.store_id.clone(),
            generation: backend.generation.clone(),
            allow_http: backend.allow_http,
            unsigned_payload: backend.unsigned_payload,
            credentials: backend.credentials_file.clone().map_or(
                external_chunk::S3CredentialsSource::Environment,
                external_chunk::S3CredentialsSource::File,
            ),
            request_timeout: timeout,
            max_retries: 2,
            cache: backend
                .cache
                .as_ref()
                .map(|cache| external_chunk::S3ChunkCacheOptions {
                    directory: cache.directory.clone(),
                    capacity_bytes: cache.capacity_bytes,
                    pending_bytes: cache.pending_bytes,
                    cache_large_write: cache.cache_large_write,
                }),
            metrics: Some(s3_metrics),
        };
        Some(Arc::new(
            crate::node::bridge::spawn_blocking(move || external_chunk::S3ChunkStore::new(options))
                .await??,
        ))
    } else {
        None
    };
    Ok(external_store)
}

/// The disposable Peer cache of 段 9, when configured.
async fn open_peer_cache(
    cfg: &Config,
    cache: &crate::config::DfsPeerCacheConfig,
    namespace: &crate::dfs::NamespaceId,
    storage: &DfsStorage,
) -> Result<Arc<read::peer_cache::DiskPeerCache>, BoxError> {
    let dfs_read_work_metrics = &storage.read_work_metrics;
    let dfs_outboard_budget = &storage.outboard_budget;
    let options = read::peer_cache::PeerCacheOptions {
        directory: cache.directory.clone(),
        capacity_bytes: cache.capacity_bytes,
        namespace_id: namespace.clone(),
    };
    let cache_metrics = dfs_read_work_metrics.clone();
    let outboard_budget = dfs_outboard_budget.clone();
    let max_inflight_objects = cfg.dfs_fetch_max_inflight_objects;
    let cache = Arc::new(
        crate::node::bridge::spawn_blocking(move || {
            read::peer_cache::DiskPeerCache::open_with_outboard_budget(options, outboard_budget)
                .and_then(|cache| cache.with_max_inflight_objects(max_inflight_objects))
                .map(|cache| cache.with_read_work_metrics(cache_metrics))
        })
        .await??,
    );
    Ok(cache)
}

/// T4 `Reaper`, T5 `LeaseKeeper` and T6 `DfsBackgroundTick`, in this order.
pub(super) fn start_background(
    cfg: &Config,
    dfs: Arc<vfs::dfs::DistributedFs>,
    assembly: &DfsAssembly,
    services: &mut Services,
) {
    fault::reaper::Reaper::new(assembly.gc_collector.clone()).start(services);
    let renewing = dfs.clone();
    fault::lease_keeper::LeaseKeeper::new(move || renewing.renew_active_write_leases())
        .start(services);
    background::tick::DfsBackgroundTick::new(
        dfs,
        std::time::Duration::from_millis(cfg.dfs_background_commit_ms),
    )
    .start(services);
}

/// T9 `RepairService`, only for a local (non-external) backend. Takes the
/// Meta adapter and the local ChunkStore, which nothing after it uses.
pub(super) fn start_repair(
    cfg: &Config,
    session_id: &str,
    assembly: &mut DfsAssembly,
    registered_node_epoch: u64,
    services: &mut Services,
) {
    if let (Some(meta), Some(local), None) = (
        assembly.meta.take(),
        assembly.storage.local_chunk_store.take(),
        cfg.dfs_external_backend.as_ref(),
    ) {
        fault::repair::RepairService::new(replication::ReplicationWorker::new(
            cfg.id.clone(),
            registered_node_epoch,
            session_id.to_owned(),
            local,
            meta,
            assembly.replica_plane.clone(),
        ))
        .start(services);
    }
}

#[cfg(feature = "dfs")]
fn parse_data_mode(value: &str) -> rpc::peer::DataMode {
    match value {
        "grpc" => rpc::peer::DataMode::Grpc,
        "rdma" => rpc::peer::DataMode::Rdma,
        _ => rpc::peer::DataMode::Auto,
    }
}

#[cfg(feature = "dfs")]
fn dfs_rdma_startup_device(
    mode: rpc::peer::DataMode,
    device: Option<&str>,
) -> afs_error::Result<Option<String>> {
    match mode {
        rpc::peer::DataMode::Grpc => Ok(None),
        rpc::peer::DataMode::Rdma => {
            device
                .map(|value| value.to_owned())
                .map(Some)
                .ok_or_else(|| {
                    afs_error::Error::coded(
                        afs_error::CONFIG_INVALID,
                        "DFS RDMA requires rdma_device",
                    )
                })
        }
        rpc::peer::DataMode::Auto => {
            #[cfg(feature = "rdma")]
            {
                if let Some(value) = device {
                    Ok(Some(value.to_owned()))
                } else {
                    afs_logging::warn!(
                        "dfs.rdma_auto_disabled";
                        "reason" => "rdma_device is not configured"
                    );
                    Ok(None)
                }
            }
            #[cfg(not(feature = "rdma"))]
            {
                let _ = device;
                afs_logging::warn!(
                    "dfs.rdma_auto_disabled";
                    "reason" => "rdma feature is not compiled"
                );
                Ok(None)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "dfs")]
    #[test]
    fn dfs_auto_rdma_startup_uses_device_only_when_available() {
        assert_eq!(
            super::dfs_rdma_startup_device(crate::node::rpc::peer::DataMode::Grpc, Some("rxe0"))
                .unwrap(),
            None
        );
        assert!(
            super::dfs_rdma_startup_device(crate::node::rpc::peer::DataMode::Rdma, None).is_err()
        );

        #[cfg(feature = "rdma")]
        {
            assert_eq!(
                super::dfs_rdma_startup_device(
                    crate::node::rpc::peer::DataMode::Auto,
                    Some("rxe0")
                )
                .unwrap(),
                Some("rxe0".to_owned())
            );
            assert_eq!(
                super::dfs_rdma_startup_device(crate::node::rpc::peer::DataMode::Auto, None)
                    .unwrap(),
                None
            );
        }

        #[cfg(not(feature = "rdma"))]
        {
            assert_eq!(
                super::dfs_rdma_startup_device(
                    crate::node::rpc::peer::DataMode::Auto,
                    Some("rxe0")
                )
                .unwrap(),
                None
            );
        }
    }
}
