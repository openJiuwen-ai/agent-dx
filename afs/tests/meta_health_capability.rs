use afs::meta::store::{
    journal::{AppendLimits, CheckpointLimits, LogCursor, LogStatus, LogTip, RecoveredLog},
    log::Record,
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use afs::{
    meta::{
        Meta, rest,
        rpc::MetaRpc,
        store::{
            BackendPersistence, MetaFuture, MetaLog, Store, journal::MemoryLog, local_log::LocalLog,
        },
    },
    node::rpc::meta::{MetaPersistenceCapability, registration_readiness_from_reply},
    runtime::Observability,
};
use afs_protocol::meta::{
    MetaBackendPersistence, NodeDescriptor, NodeEndpoint, RegisterNodeReply, RegisterNodeRequest,
    meta_server::Meta as MetaService,
};
use axum::{
    body::to_bytes,
    http::{Request, StatusCode},
};
use serde_json::Value;
use tonic::Request as GrpcRequest;
use tower::ServiceExt;

struct SwitchableBackend {
    inner: MemoryLog,
    reject_load: AtomicBool,
    persistence: Option<BackendPersistence>,
}

impl SwitchableBackend {
    fn persistent() -> Self {
        Self {
            persistence: Some(BackendPersistence::Persistent),
            ..Self::default()
        }
    }

    fn reject_load(&self) {
        self.reject_load.store(true, Ordering::Release);
    }
}

impl MetaLog for SwitchableBackend {
    fn persistence(&self) -> afs::meta::store::BackendPersistence {
        self.persistence
            .unwrap_or(afs::meta::store::BackendPersistence::Unknown)
    }
    fn limits(&self) -> AppendLimits {
        self.inner.limits()
    }
    fn recover(&self) -> MetaFuture<'_, RecoveredLog> {
        self.inner.recover()
    }
    fn checkpoint_limits(&self) -> CheckpointLimits {
        self.inner.checkpoint_limits()
    }
    /// This wrapper does not forward checkpoints, so Store schedules none.
    fn supports_checkpoint(&self) -> bool {
        false
    }
    fn status(&self) -> MetaFuture<'_, LogStatus> {
        self.inner.status()
    }
    fn can_append(&self, expected: LogCursor, bytes: usize) -> MetaFuture<'_, bool> {
        self.inner.can_append(expected, bytes)
    }
    fn install_checkpoint(&self, _: LogTip, _: Vec<u8>) -> MetaFuture<'_, ()> {
        Box::pin(async {
            Err(afs_error::Error::coded(
                afs_error::IO_UNAVAILABLE,
                "test wrapper does not install checkpoints",
            ))
        })
    }
    fn append_and_sync(&self, expected: LogCursor, records: Vec<Record>) -> MetaFuture<'_, LogTip> {
        self.inner.append_and_sync(expected, records)
    }
    fn health(&self) -> MetaFuture<'_, ()> {
        Box::pin(async move {
            if self.reject_load.load(Ordering::Acquire) {
                Err(afs_error::Error::coded(
                    afs_error::IO_UNAVAILABLE,
                    "injected backend read failure",
                ))
            } else {
                self.inner.health().await
            }
        })
    }
}

async fn meta_with_backend(backend: Arc<dyn MetaLog>) -> Arc<Meta> {
    Arc::new(Meta::with_store(
        "meta-capability-test".into(),
        Observability::new().unwrap(),
        Arc::new(Store::open_log(backend).await.unwrap()),
    ))
}

fn request(session_id: &str) -> RegisterNodeRequest {
    RegisterNodeRequest {
        request_id: format!("register-{session_id}"),
        node: Some(NodeDescriptor {
            node_id: "node-a".into(),
            endpoint: Some(NodeEndpoint {
                grpc_addr: "http://node-a:7400".into(),
                data_addr: "http://node-a:7500".into(),
                rest_addr: "http://node-a:7600".into(),
                nfs_addr: String::new(),
            }),
            labels: Default::default(),
            capabilities: Vec::new(),
            session_id: session_id.into(),
            storage_devices: Vec::new(),
        }),
        lease_seconds: 30,
        protocol_version: afs::format::version::NODE_META_PROTOCOL,
    }
}

async fn register(meta: Arc<Meta>, session_id: &str) -> RegisterNodeReply {
    MetaRpc(meta)
        .register_node(GrpcRequest::new(request(session_id)))
        .await
        .unwrap()
        .into_inner()
}

async fn get_json(meta: Arc<Meta>, path: &str) -> (StatusCode, Value) {
    let response = rest::router(meta)
        .oneshot(
            Request::builder()
                .uri(path)
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), 16 * 1024).await.unwrap();
    let json = serde_json::from_slice(&body).unwrap();
    (status, json)
}

/// K4: Meta admits only Nodes speaking its Node-Meta protocol version. A
/// missing field (0, an older Node) and any other version are refused with a
/// structured `META_NODE_PROTOCOL_VERSION` before a session is written.
#[tokio::test]
async fn register_node_refuses_other_protocol_versions_before_writing() {
    let store = Arc::new(
        Store::open_log(Arc::new(MemoryLog::new(Default::default()).unwrap()))
            .await
            .unwrap(),
    );
    let meta = Arc::new(Meta::with_store(
        "meta-protocol-test".into(),
        Observability::new().unwrap(),
        store.clone(),
    ));
    let supported = afs::format::version::NODE_META_PROTOCOL;
    assert_eq!(
        supported, 8,
        "generation 7 lacks caller-attested workspace creation"
    );
    for found in [0, 1, supported - 1, supported + 1] {
        let mut request = request("session-old");
        request.protocol_version = found;
        let status = MetaRpc(meta.clone())
            .register_node(GrpcRequest::new(request))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        let error = afs_transport::grpc::error_status::status_to_error(status);
        assert_eq!(error.code(), afs_error::META_NODE_PROTOCOL_VERSION);
        for part in [
            "Node-Meta protocol".to_owned(),
            "node-a".to_owned(),
            format!("version {found}"),
            format!("supports version {supported}"),
        ] {
            assert!(error.message().contains(&part), "{part}: {error}");
        }
    }
    let session = afs::meta::store::MetaStore::read(
        store.as_ref(),
        afs::meta::store::MetaRead::CurrentNodeSession {
            node_id: "node-a".into(),
        },
    )
    .await
    .unwrap();
    assert!(session.entity.is_none(), "a refused Node has no session");
    assert!(register(meta, "session-current").await.lease_remaining_ms > 0);
}

/// Node ids become path segments on consumers (outlet import mount points and
/// public symlink targets). Meta must reject ids that could escape a
/// configured root before a session is stored, so the snapshot never carries
/// an unsafe home id (review: mount/name-layer boundary).
#[tokio::test]
async fn register_node_refuses_path_unsafe_node_ids() {
    let store = Arc::new(
        Store::open_log(Arc::new(MemoryLog::new(Default::default()).unwrap()))
            .await
            .unwrap(),
    );
    let meta = Arc::new(Meta::with_store(
        "meta-safe-id-test".into(),
        Observability::new().unwrap(),
        store.clone(),
    ));
    for unsafe_id in ["../escape", "/etc", "with.dot", "with/slash", ""] {
        let mut request = request("session-unsafe");
        if let Some(node) = request.node.as_mut() {
            node.node_id = unsafe_id.into();
        }
        let status = MetaRpc(meta.clone())
            .register_node(GrpcRequest::new(request))
            .await
            .unwrap_err();
        assert_eq!(
            status.code(),
            tonic::Code::InvalidArgument,
            "{unsafe_id}: must be refused"
        );
        let session = afs::meta::store::MetaStore::read(
            store.as_ref(),
            afs::meta::store::MetaRead::CurrentNodeSession {
                node_id: unsafe_id.into(),
            },
        )
        .await
        .unwrap();
        assert!(session.entity.is_none(), "{unsafe_id}: no session written");
    }
}

#[tokio::test]
async fn memory_backend_is_healthy_but_not_persistent_ready() {
    let meta = meta_with_backend(Arc::new(MemoryLog::new(Default::default()).unwrap())).await;

    let reply = register(meta.clone(), "session-memory").await;
    assert!(reply.lease_remaining_ms > 0);
    assert!(reply.lease_remaining_ms <= 30_000);
    assert_eq!(
        MetaBackendPersistence::try_from(reply.meta_backend_persistence).unwrap(),
        MetaBackendPersistence::Volatile
    );
    assert!(reply.meta_backend_healthy);
    assert!(!reply.meta_persistence_ready);

    let (status, body) = get_json(meta, "/health").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["backend_persistence"], "volatile");
    assert_eq!(body["backend_health"], "healthy");
    assert_eq!(body["persistent_ready"], false);
    assert_eq!(body["physical_power_loss_proven"], false);
}

#[tokio::test]
async fn local_file_backend_reports_persistent_ready_after_probe() {
    let dir = tempfile::tempdir().unwrap();
    let meta = meta_with_backend(Arc::new(
        LocalLog::open(dir.path(), Default::default()).unwrap(),
    ))
    .await;

    let reply = register(meta.clone(), "session-local-file").await;
    assert_eq!(
        MetaBackendPersistence::try_from(reply.meta_backend_persistence).unwrap(),
        MetaBackendPersistence::Persistent
    );
    assert!(reply.meta_backend_healthy);
    assert!(reply.meta_persistence_ready);

    let (status, body) = get_json(meta, "/health").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["backend_persistence"], "persistent");
    assert_eq!(body["persistent_ready"], true);
    assert_eq!(body["physical_power_loss_proven"], false);
}

#[tokio::test]
async fn backend_health_failure_prevents_persistent_ready_ack() {
    let backend = Arc::new(SwitchableBackend::persistent());
    let meta = meta_with_backend(backend.clone()).await;
    backend.reject_load();

    let reply = register(meta.clone(), "session-unhealthy").await;
    assert_eq!(
        MetaBackendPersistence::try_from(reply.meta_backend_persistence).unwrap(),
        MetaBackendPersistence::Persistent
    );
    assert!(!reply.meta_backend_healthy);
    assert!(!reply.meta_persistence_ready);
    assert!(
        reply
            .meta_persistence_detail
            .contains("health probe failed")
    );

    let (status, body) = get_json(meta, "/health").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["error"]["kind"], "Unavailable");
}

#[tokio::test]
async fn unclassified_backend_defaults_to_unknown_and_cannot_claim_persistence() {
    let meta = meta_with_backend(Arc::new(SwitchableBackend::default())).await;

    let reply = register(meta, "session-unknown").await;
    assert_eq!(
        MetaBackendPersistence::try_from(reply.meta_backend_persistence).unwrap(),
        MetaBackendPersistence::Unknown
    );
    assert!(reply.meta_backend_healthy);
    assert!(!reply.meta_persistence_ready);
}

#[tokio::test]
async fn old_proto_defaults_fail_closed_for_persistence_readiness() {
    let old_shape = RegisterNodeReply {
        node_id: "node-a".into(),
        lease_epoch: 7,
        expires_at_unix_ms: 123,
        ..Default::default()
    };

    assert_eq!(
        MetaBackendPersistence::try_from(old_shape.meta_backend_persistence).unwrap(),
        MetaBackendPersistence::Unspecified
    );
    assert!(!old_shape.meta_backend_healthy);
    assert!(!old_shape.meta_persistence_ready);
    assert!(old_shape.meta_persistence_detail.is_empty());

    let decoded = registration_readiness_from_reply(old_shape);
    assert_eq!(
        decoded.meta_backend_persistence,
        MetaPersistenceCapability::Unknown
    );
    assert!(!decoded.meta_backend_healthy);
    assert!(!decoded.meta_persistence_ready);
}

#[test]
fn client_normalizes_contradictory_register_readiness_fail_closed() {
    let decode = |persistence: i32, healthy: bool, ready: bool| {
        registration_readiness_from_reply(RegisterNodeReply {
            node_id: "node-a".into(),
            lease_epoch: 7,
            expires_at_unix_ms: 123,
            lease_remaining_ms: 30_000,
            meta_backend_persistence: persistence,
            meta_backend_healthy: healthy,
            meta_persistence_ready: ready,
            meta_persistence_detail: "test".into(),
        })
    };

    let persistent = MetaBackendPersistence::Persistent as i32;
    assert!(decode(persistent, true, true).meta_persistence_ready);
    assert!(!decode(persistent, false, true).meta_persistence_ready);
    assert!(!decode(persistent, true, false).meta_persistence_ready);

    for persistence in [
        MetaBackendPersistence::Volatile as i32,
        MetaBackendPersistence::Unknown as i32,
        MetaBackendPersistence::Unspecified as i32,
        i32::MAX,
    ] {
        let decoded = decode(persistence, true, true);
        assert!(!decoded.meta_persistence_ready);
        if persistence == MetaBackendPersistence::Volatile as i32 {
            assert_eq!(
                decoded.meta_backend_persistence,
                MetaPersistenceCapability::Volatile
            );
        } else {
            assert_eq!(
                decoded.meta_backend_persistence,
                MetaPersistenceCapability::Unknown
            );
        }
    }
}

impl Default for SwitchableBackend {
    fn default() -> Self {
        Self {
            inner: MemoryLog::new(Default::default()).unwrap(),
            reject_load: AtomicBool::new(false),
            persistence: None,
        }
    }
}
