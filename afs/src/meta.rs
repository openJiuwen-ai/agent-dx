//! afs-meta holds coarse authority for node sessions and OwnerFs root grants.
//!
//! `Meta` 是 gRPC/REST 共享的业务对象；`rpc::MetaRpc` 只转换协议并调用它。
//! 当前 Ping 不修改业务状态，所以没有为它引入 Mutex 或 actor。
//! 将来根归属/授权状态应在这里所属的领域模块实现，不能各在 REST 和 gRPC 存一份。
//! 所有 Meta 后端使用统一 Store：先提交完整状态，确认后才发布可见权威。
//! 没有 store 时 RPC 必须失败关闭，避免把进程内判断误写成授权事实。

pub mod dfs;
pub mod owner_roots;
pub mod rest;
pub mod rpc;
pub mod store;
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
}
impl Meta {
    pub fn new(id: String, observability: Observability) -> Self {
        Self {
            id,
            observability,
            store: None,
            owner_roots: Arc::new(owner_roots::MissingOwnerRootAuthority),
            dfs: None,
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
/// 先成功绑定两个端口，再并发启动 gRPC 和 REST；它们共享同一个 Arc<Meta>。
pub async fn run(cfg: Config, obs: Observability) -> Result<(), BoxError> {
    let grpc = tokio::net::TcpListener::bind(cfg.grpc_listen).await?;
    let rest = tokio::net::TcpListener::bind(cfg.rest_listen).await?;
    let store = if cfg.ownerfs || cfg.dfs {
        Some(match cfg.meta_store {
            MetaStoreBackend::Etcd => {
                let endpoint = cfg.etcd_endpoint.clone().ok_or_else(|| {
                    afs_error::Error::coded(
                        afs_error::CONFIG_INVALID,
                        "meta filesystem services with meta_store=etcd require --etcd-endpoint",
                    )
                })?;
                let backend = store::etcd::EtcdBackend::connect(endpoint).await?;
                Arc::new(store::Store::open(Arc::new(backend)).await?) as Arc<dyn store::MetaStore>
            }
            MetaStoreBackend::Redis => {
                let endpoint = cfg.redis_endpoint.clone().ok_or_else(|| {
                    afs_error::Error::coded(
                        afs_error::CONFIG_INVALID,
                        "meta filesystem services with meta_store=redis require --redis-endpoint",
                    )
                })?;
                let backend = store::redis::RedisBackend::connect(endpoint).await?;
                Arc::new(store::Store::open(Arc::new(backend)).await?) as Arc<dyn store::MetaStore>
            }
            MetaStoreBackend::InMemory => {
                afs_logging::warn!("meta.store.memory_volatile"; "instance" => &cfg.id);
                Arc::new(
                    store::Store::open(Arc::new(store::memory::MemoryBackend::default())).await?,
                ) as Arc<dyn store::MetaStore>
            }
            MetaStoreBackend::LocalFile => {
                let backend =
                    store::local_file::LocalFileBackend::open(cfg.data_dir.join("meta-store"))?;
                Arc::new(store::Store::open(Arc::new(backend)).await?) as Arc<dyn store::MetaStore>
            }
        })
    } else {
        None
    };
    let trusted_nodes_by_der = load_trusted_node_certs(&cfg.trusted_node_certs)?;
    if cfg.ownerfs && trusted_nodes_by_der.is_empty() {
        return Err(afs_error::Error::coded(
            afs_error::CONFIG_INVALID,
            "meta ownerfs requires trusted_node_certs for mTLS node identity binding",
        )
        .into());
    }
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
        let service = dfs::DfsService::with_replication_config(
            store.clone(),
            crate::dfs::ReplicationConfig {
                desired_copies: cfg.dfs_desired_copies,
                sync_required_copies: cfg.dfs_sync_required_copies,
                min_distinct_nodes: cfg.dfs_min_distinct_nodes,
                min_distinct_failure_domains: cfg.dfs_min_distinct_failure_domains,
                local_copy,
            },
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
        trusted_nodes_by_der: Arc::new(trusted_nodes_by_der),
        enforce_peer_identity: cfg.ownerfs,
    });
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
    use super::*;
    use std::{sync::Arc, time::Duration};

    #[tokio::test]
    async fn lookup_node_treats_expired_current_session_as_absent() {
        let store = Arc::new(
            store::Store::open(Arc::new(store::memory::MemoryBackend::default()))
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
