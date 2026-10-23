//! afs-meta holds coarse authority for node sessions and OwnerFs root grants.
//!
//! `Meta` 是 gRPC/REST 共享的业务对象；`rpc::MetaRpc` 只转换协议并调用它。
//! 当前 Ping 不修改业务状态，所以没有为它引入 Mutex 或 actor。
//! 将来根归属/授权状态应在这里所属的领域模块实现，不能各在 REST 和 gRPC 存一份。
//! Meta 通过统一 Store 先持久化确定变化，再发布可见权威。
//! local-file 与显式易失 memory 使用同一操作日志合同；etcd/Redis 旧后端已退出当前交付。
//! 没有 store 时 RPC 必须失败关闭，避免把进程内判断误写成授权事实。

pub mod dfs;
pub mod owner_roots;
pub mod rest;
pub mod rpc;
pub mod store;
mod watch;
use crate::{
    config::{Config, MetaStoreBackend},
    runtime::{BoxError, Observability, Services, cancelled},
};
use std::{collections::HashMap, sync::Arc};
#[derive(Clone)]
pub struct Meta {
    pub id: String,
    pub observability: Observability,
    pub store: Option<Arc<dyn store::MetaStore>>,
    pub owner_roots: Arc<dyn owner_roots::OwnerRootAuthority>,
    pub dfs: Option<dfs::DfsService>,
    /// Exact leaf certificate DER -> trusted AFS node id.
    ///
    /// The map is populated only by production startup from `trusted_node_certs`;
    /// tests that construct `Meta` directly leave it empty and do not enforce
    /// transport identity. Runtime authority RPCs must use this map instead of
    /// trusting node ids carried in request bodies.
    pub trusted_nodes_by_der: Arc<HashMap<Vec<u8>, String>>,
    pub enforce_peer_identity: bool,
    watch_budget: Arc<watch::Budget>,
}
impl Meta {
    pub fn new(id: String, observability: Observability) -> Self {
        Self {
            id,
            observability,
            store: None,
            owner_roots: Arc::new(owner_roots::MissingOwnerRootAuthority),
            dfs: None,
            watch_budget: watch::Budget::standard(),
            trusted_nodes_by_der: Arc::new(HashMap::new()),
            enforce_peer_identity: false,
        }
    }

    pub fn with_store(
        id: String,
        observability: Observability,
        store: Arc<dyn store::MetaStore>,
    ) -> Self {
        let owner_roots = Arc::new(owner_roots::StoreOwnerRootAuthority::new(store.clone()));
        let dfs = Some(dfs::DfsService::new(store.clone()));
        Self {
            id,
            observability,
            store: Some(store),
            owner_roots,
            dfs,
            watch_budget: watch::Budget::standard(),
            trusted_nodes_by_der: Arc::new(HashMap::new()),
            enforce_peer_identity: false,
        }
    }

    pub fn with_store_and_peer_identity(
        id: String,
        observability: Observability,
        store: Arc<dyn store::MetaStore>,
        trusted_nodes_by_der: HashMap<Vec<u8>, String>,
        enforce_peer_identity: bool,
    ) -> Self {
        let owner_roots = Arc::new(owner_roots::StoreOwnerRootAuthority::new(store.clone()));
        let dfs = Some(dfs::DfsService::new(store.clone()));
        Self {
            id,
            observability,
            store: Some(store),
            owner_roots,
            dfs,
            watch_budget: watch::Budget::standard(),
            trusted_nodes_by_der: Arc::new(trusted_nodes_by_der),
            enforce_peer_identity,
        }
    }

    pub fn ping(&self, node_id: &str) -> Result<String, afs_error::Error> {
        if node_id.len() > 128 {
            self.observability.record("meta", "ping", false);
            return Err(afs_error::Error::coded(
                afs_error::META_CATALOG_INVALID_REQUEST,
                "node_id exceeds 128 bytes",
            ));
        }
        self.observability.record("meta", "ping", true);
        afs_logging::info!("meta.ping";"caller"=>node_id,"instance"=>&self.id);
        Ok(format!("pong from {}", self.id))
    }

    /// Node registration is a business transition, not a tonic callback.
    /// Store records both the session and its replayable outcome before this
    /// method returns a session that other Nodes may discover.
    pub async fn register_node(
        &self,
        request: store::RequestKey,
        lease: store::NodeSessionLease,
    ) -> afs_error::Result<store::NodeSession> {
        let store = self
            .store
            .as_deref()
            .ok_or_else(store::unavailable_meta_store)?;
        let result = store.register_node_session(request, lease).await?;
        let outcome = match result {
            store::TxnOutcome::Committed { outcome, .. }
            | store::TxnOutcome::ConditionFailed {
                existing_outcome: Some(outcome),
                ..
            } if outcome.operation == store::StoreOperation::RegisterNode => outcome.result,
            _ => {
                return Err(afs_error::Error::coded(
                    afs_error::META_CATALOG_INVALID_REQUEST,
                    "node registration request could not be committed",
                ));
            }
        };
        match outcome {
            store::OperationResult::NodeSession(session) => Ok(session),
            _ => Err(afs_error::Error::coded(
                afs_error::META_CATALOG_INVALID_REQUEST,
                "node registration replay returned wrong result type",
            )),
        }
    }

    /// The visible Store snapshot contains only backend-ACKed sessions.
    pub async fn lookup_node(
        &self,
        node_id: &str,
    ) -> afs_error::Result<Option<store::NodeSession>> {
        let store = self
            .store
            .as_deref()
            .ok_or_else(store::unavailable_meta_store)?;
        let snapshot = store
            .read(store::MetaRead::CurrentNodeSession {
                node_id: node_id.to_owned(),
            })
            .await?;
        Ok(match snapshot.entity {
            Some(store::MetaEntity::NodeSession(session))
                if session.is_live_at_unix_ms(store::now_unix_ms()) =>
            {
                Some(session)
            }
            _ => None,
        })
    }
}
/// 先恢复权威并初始化，再绑定两个端口并启动共享 Meta 的 gRPC 和 REST。
pub async fn run(cfg: Config, obs: Observability) -> Result<(), BoxError> {
    // Validate the transport identity policy before opening listeners or
    // touching a configured persistent backend. A filesystem authority must
    // never become partially available with CA-only authentication.
    let (trusted_nodes_by_der, enforce_peer_identity) = peer_identity_policy(&cfg)?;
    let concrete_store = open_configured_store(&cfg).await?;
    if let Some(store) = &concrete_store {
        store.register_metrics(&obs.registry)?;
    }
    let store = concrete_store.map(|store| store as Arc<dyn store::MetaStore>);
    let owner_roots = store.as_ref().map_or_else(
        || {
            Arc::new(owner_roots::MissingOwnerRootAuthority)
                as Arc<dyn owner_roots::OwnerRootAuthority>
        },
        |store| Arc::new(owner_roots::StoreOwnerRootAuthority::new(store.clone())),
    );
    let dfs = if let Some(store) = store.as_ref() {
        let local_copy = match cfg.dfs_local_copy.as_str() {
            "required" => crate::dfs::LocalCopyPolicy::Required,
            "preferred" => crate::dfs::LocalCopyPolicy::Preferred,
            "none" => crate::dfs::LocalCopyPolicy::NotRequired,
            _ => unreachable!("Config validates dfs_local_copy"),
        };
        let service = dfs::DfsService::with_durability_policy(
            store.clone(),
            crate::dfs::ReplicationConfig {
                desired_copies: cfg.dfs_desired_copies,
                sync_required_copies: cfg.dfs_sync_required_copies,
                min_distinct_nodes: cfg.dfs_min_distinct_nodes,
                min_distinct_failure_domains: cfg.dfs_min_distinct_failure_domains,
                local_copy,
            },
            cfg.dfs_external_backend.as_ref().map_or(
                crate::dfs::DfsDurabilityPolicy::NodeReplicated,
                |backend| crate::dfs::DfsDurabilityPolicy::ExternalBackend {
                    store_id: backend.store_id.clone(),
                    generation: backend.generation.clone(),
                },
            ),
        );
        if cfg.dfs {
            service.initialize_replication_config().await?;
        }
        Some(service)
    } else {
        None
    };
    let state = Arc::new(Meta {
        id: cfg.id.clone(),
        observability: obs,
        store,
        owner_roots,
        dfs,
        watch_budget: watch::Budget::standard(),
        trusted_nodes_by_der: Arc::new(trusted_nodes_by_der),
        enforce_peer_identity,
    });
    let grpc = tokio::net::TcpListener::bind(cfg.grpc_listen).await?;
    let rest = tokio::net::TcpListener::bind(cfg.rest_listen).await?;
    let mut services = Services::new();
    let stop = services.stop.subscribe();
    let grpc_config = afs_transport::grpc::GrpcConfig::default();
    let incoming =
        grpc_config.configure_tcp_incoming(tonic::transport::server::TcpIncoming::from(grpc));
    let meta_service =
        afs_protocol::meta::meta_server::MetaServer::new(rpc::MetaRpc(state.clone()));
    let owner_service = afs_protocol::meta::owner_roots_server::OwnerRootsServer::new(
        rpc::OwnerRootsRpc(state.clone()),
    );
    let dfs_service =
        afs_protocol::meta::dfs_meta_server::DfsMetaServer::new(rpc::DfsMetaRpc(state.clone()));
    let security = afs_transport::grpc::SecurityManager::new(cfg.tls_config())?;
    let server = security
        .configure_server(grpc_config.configure_server(tonic::transport::Server::builder()))?;
    services.spawn(async move {
        server
            .layer(afs_tracing::GrpcServerTraceLayer::default())
            .add_service(meta_service)
            .add_service(owner_service)
            .add_service(dfs_service)
            .serve_with_incoming_shutdown(incoming, cancelled(stop))
            .await
            .map_err(Into::into)
    });
    let stop = services.stop.subscribe();
    services.spawn(async move {
        axum::serve(rest, rest::router(state))
            .with_graceful_shutdown(cancelled(stop))
            .await
            .map_err(Into::into)
    });
    afs_logging::info!("meta.ready";"grpc"=>cfg.grpc_listen.to_string(),"rest"=>cfg.rest_listen.to_string());
    services.run().await
}

/// Open and recover the configured authority before binding services.
/// Returns no authority when both filesystem modes are disabled. Configuration,
/// identity, corruption or persistence failures are returned without fallback.
/// Test launchers use this factory so they cannot select a different backend.
pub async fn open_configured_store(cfg: &Config) -> Result<Option<Arc<store::Store>>, BoxError> {
    if !cfg.ownerfs && !cfg.dfs {
        return Ok(None);
    }
    let limits = cfg.meta_log_limits()?;
    let maintenance = cfg.meta_maintenance_policy()?;
    let history = cfg.meta_history_limits()?;
    let log: Arc<dyn store::journal::MetaLog> = match cfg.meta_store {
        MetaStoreBackend::InMemory => {
            afs_logging::warn!("meta.store.memory_volatile"; "instance" => &cfg.id);
            Arc::new(store::journal::MemoryLog::with_policy(limits, maintenance)?)
        }
        MetaStoreBackend::LocalFile => {
            let path = cfg.data_dir.join("meta-store");
            Arc::new(
                tokio::task::spawn_blocking(move || {
                    store::local_log::LocalLog::open_with_policies(
                        path,
                        limits,
                        store::journal::CheckpointLimits::default(),
                        maintenance,
                    )
                })
                .await??,
            )
        }
    };
    Ok(Some(Arc::new(
        store::Store::open_log_with_history(log, history).await?,
    )))
}

fn peer_identity_policy(
    cfg: &Config,
) -> Result<(HashMap<Vec<u8>, String>, bool), afs_error::Error> {
    let trusted_nodes_by_der = load_trusted_node_certs(&cfg.trusted_node_certs)?;
    let enforce_peer_identity = cfg.ownerfs || cfg.dfs;
    if enforce_peer_identity && trusted_nodes_by_der.is_empty() {
        return Err(config_invalid(
            "meta filesystem authority requires trusted_node_certs for mTLS node identity binding",
        ));
    }
    if enforce_peer_identity
        && !matches!(
            cfg.tls_config(),
            afs_transport::grpc::TlsConfig::MutualTls { .. }
        )
    {
        return Err(config_invalid(
            "meta filesystem authority requires mTLS transport as well as trusted_node_certs",
        ));
    }
    Ok((trusted_nodes_by_der, enforce_peer_identity))
}

fn load_trusted_node_certs(
    certs: &HashMap<String, std::path::PathBuf>,
) -> Result<HashMap<Vec<u8>, String>, afs_error::Error> {
    let mut out = HashMap::new();
    for (node_id, path) in certs {
        if node_id.is_empty() {
            return Err(config_invalid(
                "trusted_node_certs contains an empty node id",
            ));
        }
        let cert = crate::config::read_certificate_der(path)?;
        if let Some(previous) = out.insert(cert, node_id.clone()) {
            return Err(config_invalid(format!(
                "trusted_node_certs maps the same certificate to both {previous} and {node_id}"
            )));
        }
    }
    Ok(out)
}

fn config_invalid(message: impl Into<String>) -> afs_error::Error {
    afs_error::Error::coded(afs_error::CONFIG_INVALID, message)
}

#[cfg(test)]
mod tests {
    use super::store::MetaStore;
    use super::*;
    use crate::config::{Cli, Role};
    use clap::Parser;
    use std::{sync::Arc, time::Duration};

    #[tokio::test]
    async fn p3_settings_factory_enforces_history_count_and_idle_expiry() {
        for backend in [MetaStoreBackend::LocalFile, MetaStoreBackend::InMemory] {
            let dir = tempfile::tempdir().unwrap();
            let mut cfg = p3_config(dir.path(), backend);
            cfg.meta_request_outcomes_max = 1;
            cfg.meta_watch_events_max = 2;
            cfg.meta_request_retention_ms = 100;
            cfg.meta_watch_retention_ms = 100;
            let store = open_configured_store(&cfg).await.unwrap().unwrap();
            let first = store::RequestKey::new("settings", "first");
            let second = store::RequestKey::new("settings", "second");
            for key in [first.clone(), second.clone()] {
                store.register_node_session(key, p3_lease()).await.unwrap();
            }
            assert!(
                store
                    .read(store::MetaRead::RequestOutcome(first))
                    .await
                    .unwrap()
                    .request_outcome
                    .is_none()
            );
            assert!(
                store
                    .read(store::MetaRead::RequestOutcome(second.clone()))
                    .await
                    .unwrap()
                    .request_outcome
                    .is_some()
            );
            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    if store
                        .read(store::MetaRead::RequestOutcome(second.clone()))
                        .await
                        .unwrap()
                        .request_outcome
                        .is_none()
                        && matches!(
                            store.watch(store::StoreRevision::ZERO, 1).await.unwrap(),
                            store::WatchBatch::Compacted { .. }
                        )
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect(
                "configured history policy must affect the actual factory, including idle cleanup",
            );
            // Authority is not history: expiring replies/events must not erase the live session.
            assert!(
                store
                    .read(store::MetaRead::CurrentNodeSession {
                        node_id: "node-a".into()
                    })
                    .await
                    .unwrap()
                    .entity
                    .is_some()
            );
            drop(store);
            if backend == MetaStoreBackend::LocalFile {
                let reopened = p3_reopen_after_drop(&cfg).await;
                assert!(
                    reopened
                        .read(store::MetaRead::CurrentNodeSession {
                            node_id: "node-a".into()
                        })
                        .await
                        .unwrap()
                        .entity
                        .is_some()
                );
                assert!(
                    reopened
                        .read(store::MetaRead::RequestOutcome(second))
                        .await
                        .unwrap()
                        .request_outcome
                        .is_none()
                );
            }
        }
    }

    #[tokio::test]
    async fn p3_settings_factory_uses_checkpoint_schedule_and_segment_target() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = p3_config(dir.path(), MetaStoreBackend::LocalFile);
        cfg.meta_checkpoint_interval_ms = 25;
        cfg.meta_checkpoint_wal_records = 1;
        cfg.meta_wal_segment_bytes = 8192;
        cfg.meta_record_max_bytes = 4096;
        cfg.meta_batch_max_bytes = 8192;
        cfg.meta_wal_max_bytes = 128 * 1024;
        let store = open_configured_store(&cfg).await.unwrap().unwrap();
        let root = cfg.data_dir.join("meta-store");
        fn read_current(root: &std::path::Path) -> Option<Vec<u8>> {
            match std::fs::read(root.join("CURRENT")) {
                Ok(bytes) => Some(bytes),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => panic!("cannot inspect CURRENT: {error}"),
            }
        }
        // An empty log needs no CURRENT until its first checkpoint/rotation.
        let initial_current = read_current(&root);
        store
            .register_node_session(store::RequestKey::new("settings", "checkpoint"), p3_lease())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if read_current(&root).is_some_and(|current| Some(current) != initial_current)
                    && std::fs::read_dir(&root)
                        .unwrap()
                        .filter_map(|entry| entry.ok())
                        .any(|entry| {
                            entry.file_name().to_str().is_some_and(|name| {
                                name.starts_with("checkpoint-") && name.ends_with(".bin")
                            })
                        })
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("factory must use configured sub-second checkpoint schedule, not the default 60s");
        // Each framed operation is below the configured record limit. The small
        // segment target must rotate under a bounded series of real commits.
        for id in 0..32 {
            store
                .register_node_session(
                    store::RequestKey::new("settings", format!("rotate-{id}")),
                    p3_lease(),
                )
                .await
                .unwrap();
        }
        assert!(
            std::fs::read_dir(&root)
                .unwrap()
                .filter_map(|entry| entry.ok())
                .any(|entry| {
                    entry.file_name().to_str().is_some_and(|name| {
                        name.starts_with("wal-") && name != "wal-00000000000000000001.log"
                    })
                }),
            "actual configured LocalLog must rotate, not only print a different setting"
        );
    }

    #[tokio::test]
    async fn p3_configured_local_store_uses_canonical_log() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = Config::resolve(Role::Meta, Cli::parse_from(["afs-meta"])).unwrap();
        cfg.dfs = true;
        cfg.meta_store = MetaStoreBackend::LocalFile;
        cfg.data_dir = dir.path().to_owned();
        let store = open_configured_store(&cfg).await.unwrap().unwrap();
        assert!(
            cfg.data_dir.join("meta-store/IDENTITY").is_file(),
            "production local-file startup must select canonical operation log"
        );
        assert!(!cfg.data_dir.join("meta-store/snapshot").exists());
        drop(store);
    }

    fn p3_config(dir: &std::path::Path, backend: MetaStoreBackend) -> Config {
        let mut cfg = Config::resolve(Role::Meta, Cli::parse_from(["afs-meta"])).unwrap();
        cfg.dfs = true;
        cfg.meta_store = backend;
        cfg.data_dir = dir.to_owned();
        cfg
    }

    fn p3_lease() -> store::NodeSessionLease {
        store::NodeSessionLease {
            node_id: "node-a".into(),
            session_id: "session-a".into(),
            grpc_addr: "http://node-a:7400".into(),
            data_addr: "http://node-a:7500".into(),
            rest_addr: "http://node-a:7600".into(),
            storage_devices: Vec::new(),
            lease_ttl: Duration::from_secs(60),
        }
    }

    async fn p3_reopen_after_drop(cfg: &Config) -> Arc<dyn store::MetaStore> {
        // Store's existing commit task owns the log until it observes channel close.
        // Retry only that known lock-release race; never hide recovery failures.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match open_configured_store(cfg).await {
                    Ok(store) => break store.unwrap() as Arc<dyn store::MetaStore>,
                    Err(error) if error.to_string().contains("Meta log already locked") => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    Err(error) => panic!("unexpected recovery error: {error}"),
                }
            }
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn p3_configured_local_restart_preserves_identity_session_and_original_reply() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = p3_config(dir.path(), MetaStoreBackend::LocalFile);
        let store = open_configured_store(&cfg).await.unwrap().unwrap();
        let meta = Meta::with_store(
            "restart".into(),
            Observability::new().unwrap(),
            store.clone(),
        );
        let request = store::RequestKey::new("node-a", "register-a");
        let first = meta
            .register_node(request.clone(), p3_lease())
            .await
            .unwrap();
        let root = cfg.data_dir.join("meta-store");
        let identity = std::fs::read(root.join("IDENTITY")).unwrap();
        let wal = root.join("wal-00000000000000000001.log");
        let bytes = std::fs::read(&wal).unwrap();
        assert!(bytes.len() > store::journal::LOG_HEADER_BYTES as usize);
        assert!(!root.join("wal").exists());
        assert!(!root.join("snapshot").exists());
        drop(meta);
        drop(store);
        let reopened = p3_reopen_after_drop(&cfg).await;
        assert!(reopened.backend_readiness().await.unwrap().persistent_ready);
        let meta = Meta::with_store("restart".into(), Observability::new().unwrap(), reopened);
        assert_eq!(
            meta.lookup_node("node-a").await.unwrap(),
            Some(first.clone())
        );
        assert_eq!(
            meta.register_node(request, p3_lease()).await.unwrap(),
            first
        );
        assert_eq!(std::fs::read(root.join("IDENTITY")).unwrap(), identity);
        assert_eq!(
            std::fs::read(wal).unwrap(),
            bytes,
            "retry must not append or renew"
        );
    }

    #[tokio::test]
    async fn p3_configured_memory_is_volatile_and_uses_framed_record_limit() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = p3_config(dir.path(), MetaStoreBackend::InMemory);
        cfg.meta_record_max_bytes = 256;
        cfg.meta_batch_max_bytes = 256;
        let store = open_configured_store(&cfg).await.unwrap().unwrap();
        let mut lease = p3_lease();
        lease.grpc_addr = "x".repeat(1024);
        let error = store
            .register_node_session(store::RequestKey::new("a", "too-large"), lease)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("byte limit"), "{error}");
        let readiness = store.backend_readiness().await.unwrap();
        assert!(readiness.healthy);
        assert_eq!(readiness.persistence, store::BackendPersistence::Volatile);
        assert!(!readiness.persistent_ready);
        assert!(
            store
                .read(store::MetaRead::CurrentNodeSession {
                    node_id: "node-a".into()
                })
                .await
                .unwrap()
                .entity
                .is_none()
        );
        drop(store);
        cfg.meta_record_max_bytes = 4096;
        cfg.meta_batch_max_bytes = 4096;
        let store = open_configured_store(&cfg).await.unwrap().unwrap();
        store
            .register_node_session(store::RequestKey::new("a", "accepted"), p3_lease())
            .await
            .unwrap();
        drop(store);
        let store = open_configured_store(&cfg).await.unwrap().unwrap();
        assert!(
            store
                .read(store::MetaRead::CurrentNodeSession {
                    node_id: "node-a".into()
                })
                .await
                .unwrap()
                .entity
                .is_none()
        );
        assert!(!cfg.data_dir.join("meta-store").exists());
    }

    #[tokio::test]
    async fn p3_configured_recovery_rejects_complete_corruption_without_rewriting_it() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = p3_config(dir.path(), MetaStoreBackend::LocalFile);
        let root = cfg.data_dir.join("meta-store");
        let log = Arc::new(
            store::local_log::LocalLog::open(&root, cfg.meta_log_limits().unwrap()).unwrap(),
        );
        let store = store::Store::open_log(log.clone()).await.unwrap();
        store::MetaStore::register_node_session(
            &store,
            store::RequestKey::new("a", "corrupt"),
            p3_lease(),
        )
        .await
        .unwrap();
        drop(store);
        tokio::time::timeout(Duration::from_secs(5), async {
            while Arc::strong_count(&log) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(log);
        let identity = std::fs::read(root.join("IDENTITY")).unwrap();
        let wal = root.join("wal-00000000000000000001.log");
        let mut bytes = std::fs::read(&wal).unwrap();
        bytes[store::journal::LOG_HEADER_BYTES as usize + 8] ^= 1;
        std::fs::write(&wal, &bytes).unwrap();
        let error = match open_configured_store(&cfg).await {
            Err(error) => error,
            Ok(_) => panic!("corrupt authority was accepted"),
        };
        assert!(error.to_string().contains("offset 4096"), "{error}");
        assert_eq!(std::fs::read(wal).unwrap(), bytes);
        assert_eq!(std::fs::read(root.join("IDENTITY")).unwrap(), identity);
    }

    #[tokio::test]
    async fn p3_production_recovery_rejects_legacy_before_binding_listeners() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = p3_config(dir.path(), MetaStoreBackend::LocalFile);
        let root = cfg.data_dir.join("meta-store");
        std::fs::create_dir(&root).unwrap();
        std::fs::write(root.join("snapshot"), b"preserved old-format bytes").unwrap();
        let cert = dir.path().join("node.der");
        std::fs::write(&cert, [1, 2, 3, 4]).unwrap();
        cfg.trusted_node_certs.insert("node-a".into(), cert.clone());
        cfg.tls_ca_certificate = Some(cert.clone());
        cfg.tls_identity_certificate = Some(cert.clone());
        cfg.tls_identity_private_key = Some(cert);
        cfg.tls_server_name = Some("afs-cluster".into());
        let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        cfg.grpc_listen = occupied.local_addr().unwrap();
        let error = run(cfg, Observability::new().unwrap()).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("nonempty directory without IDENTITY"),
            "{error}"
        );
        assert_eq!(
            std::fs::read(root.join("snapshot")).unwrap(),
            b"preserved old-format bytes"
        );
        assert!(!root.join("IDENTITY").exists());
        assert!(!root.join("wal-00000000000000000001.log").exists());
    }

    #[test]
    fn dfs_authority_requires_exact_node_certificate_bindings() {
        let mut cfg = Config::resolve(Role::Meta, Cli::parse_from(["afs-meta"])).unwrap();
        cfg.ownerfs = false;
        cfg.dfs = true;
        cfg.trusted_node_certs.clear();

        let error = peer_identity_policy(&cfg).unwrap_err();
        assert_eq!(error.code(), afs_error::CONFIG_INVALID);
        assert!(error.to_string().contains("trusted_node_certs"));

        cfg.dfs = false;
        let (trusted, enforced) = peer_identity_policy(&cfg).unwrap();
        assert!(trusted.is_empty());
        assert!(!enforced);
    }

    #[tokio::test]
    async fn production_dfs_startup_rejects_ca_only_identity_before_backend_or_listeners() {
        let mut cfg = Config::resolve(Role::Meta, Cli::parse_from(["afs-meta"])).unwrap();
        cfg.ownerfs = false;
        cfg.dfs = true;
        cfg.trusted_node_certs.clear();

        let error = run(cfg, Observability::new().unwrap()).await.unwrap_err();
        assert!(error.to_string().contains("trusted_node_certs"));
    }

    #[tokio::test]
    async fn filesystem_authority_rejects_disabled_tls_before_backend_or_listeners() {
        let dir = tempfile::tempdir().unwrap();
        let cert = dir.path().join("node.der");
        // Identity-table fixtures do not perform a TLS handshake.
        std::fs::write(&cert, [1, 2, 3, 4]).unwrap();
        let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        for (ownerfs, dfs) in [(true, false), (false, true)] {
            let mut cfg = Config::resolve(Role::Meta, Cli::parse_from(["afs-meta"])).unwrap();
            cfg.ownerfs = ownerfs;
            cfg.dfs = dfs;
            cfg.grpc_listen = occupied.local_addr().unwrap();
            cfg.meta_store = MetaStoreBackend::LocalFile;
            cfg.data_dir = dir.path().join("must-not-be-created");
            cfg.trusted_node_certs.insert("node-a".into(), cert.clone());

            let error = run(cfg, Observability::new().unwrap()).await.unwrap_err();
            let coded = error.downcast_ref::<afs_error::Error>().unwrap();
            assert_eq!(coded.code(), afs_error::CONFIG_INVALID);
            assert!(coded.to_string().contains("mTLS transport"));
            assert!(!dir.path().join("must-not-be-created").exists());
        }
    }

    #[test]
    fn filesystem_authority_accepts_mutual_tls_with_exact_node_bindings() {
        let dir = tempfile::tempdir().unwrap();
        let cert = dir.path().join("node.der");
        std::fs::write(&cert, [1, 2, 3, 4]).unwrap();
        for (ownerfs, dfs) in [(true, false), (false, true)] {
            let mut cfg = Config::resolve(Role::Meta, Cli::parse_from(["afs-meta"])).unwrap();
            cfg.ownerfs = ownerfs;
            cfg.dfs = dfs;
            cfg.trusted_node_certs.insert("node-a".into(), cert.clone());
            cfg.tls_ca_certificate = Some(dir.path().join("ca.crt"));
            cfg.tls_identity_certificate = Some(dir.path().join("meta.crt"));
            cfg.tls_identity_private_key = Some(dir.path().join("meta.key"));
            cfg.tls_server_name = Some("afs-cluster".into());

            let (trusted, enforced) = peer_identity_policy(&cfg).unwrap();
            assert!(enforced);
            assert_eq!(
                trusted.get([1, 2, 3, 4].as_slice()).map(String::as_str),
                Some("node-a")
            );
        }
    }

    #[tokio::test]
    async fn lookup_node_treats_expired_current_session_as_absent() {
        let store = Arc::new(
            store::Store::open_log(Arc::new(
                store::journal::MemoryLog::new(Default::default()).unwrap(),
            ))
            .await
            .unwrap(),
        );
        let meta = Meta::with_store(
            "meta-test".into(),
            Observability::new().unwrap(),
            store.clone(),
        );
        meta.register_node(
            store::RequestKey::new("node-a", "register-1"),
            store::NodeSessionLease {
                node_id: "node-a".into(),
                session_id: "session-1".into(),
                grpc_addr: "http://node-a:7400".into(),
                data_addr: "http://node-a:7500".into(),
                rest_addr: "http://node-a:7600".into(),
                storage_devices: Vec::new(),
                lease_ttl: Duration::from_nanos(1),
            },
        )
        .await
        .unwrap();

        assert!(meta.lookup_node("node-a").await.unwrap().is_none());
    }
}
