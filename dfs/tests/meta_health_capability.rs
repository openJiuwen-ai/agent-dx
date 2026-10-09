use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use afs::{
    meta::{
        Meta, rest,
        rpc::MetaRpc,
        store::{
            BackendPersistence, MetaFuture, Store, StoreBackend, local_file::LocalFileBackend,
            memory::MemoryBackend,
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

#[derive(Default)]
struct SwitchableBackend {
    inner: MemoryBackend,
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

impl StoreBackend for SwitchableBackend {
    fn persistence(&self) -> BackendPersistence {
        self.persistence.unwrap_or(BackendPersistence::Unknown)
    }

    fn load(&self) -> MetaFuture<'_, Option<(u64, Vec<u8>)>> {
        Box::pin(async move {
            if self.reject_load.load(Ordering::Acquire) {
                Err(afs_error::Error::coded(
                    afs_error::IO_UNAVAILABLE,
                    "injected backend load failure",
                ))
            } else {
                self.inner.load().await
            }
        })
    }

    fn commit(&self, expected_version: u64, bytes: Vec<u8>) -> MetaFuture<'_, u64> {
        self.inner.commit(expected_version, bytes)
    }
}

async fn meta_with_backend(backend: Arc<dyn StoreBackend>) -> Arc<Meta> {
    Arc::new(Meta::with_store(
        "meta-capability-test".into(),
        Observability::new().unwrap(),
        Arc::new(Store::open(backend).await.unwrap()),
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
            }),
            labels: Default::default(),
            capabilities: Vec::new(),
            session_id: session_id.into(),
            storage_devices: Vec::new(),
        }),
        lease_seconds: 30,
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

#[tokio::test]
async fn memory_backend_is_healthy_but_not_persistent_ready() {
    let meta = meta_with_backend(Arc::new(MemoryBackend::default())).await;

    let reply = register(meta.clone(), "session-memory").await;
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
    let meta = meta_with_backend(Arc::new(LocalFileBackend::open(dir.path()).unwrap())).await;

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
