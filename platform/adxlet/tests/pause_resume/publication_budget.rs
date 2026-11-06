//! Real loopback gRPC checks for the actor's snapshot publication budget.
use super::*;
use adx_protocol::control as pb;
use adxlet::{checkpoint::SnapshotRequest, rpc::CoordinatorStateSink};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{transport::Server, Request, Response, Status};

#[derive(Clone)]
struct DelayedCatalog {
    delay_ms: Arc<AtomicU64>,
    publications: Arc<AtomicUsize>,
}

#[tonic::async_trait]
impl pb::snapshot_service_server::SnapshotService for DelayedCatalog {
    async fn publish_snapshot(
        &self,
        request: Request<pb::PublishSnapshotRequest>,
    ) -> std::result::Result<Response<pb::ReusableSnapshot>, Status> {
        self.publications.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(self.delay_ms.load(Ordering::SeqCst))).await;
        Ok(Response::new(request.into_inner().snapshot.ok_or_else(
            || Status::invalid_argument("snapshot missing"),
        )?))
    }

    async fn get_snapshot(
        &self,
        _: Request<pb::GetSnapshotRequest>,
    ) -> std::result::Result<Response<pb::ReusableSnapshot>, Status> {
        Err(Status::not_found("not published"))
    }

    async fn list_snapshots(
        &self,
        _: Request<pb::ListSnapshotsRequest>,
    ) -> std::result::Result<Response<pb::ListSnapshotsResponse>, Status> {
        Err(Status::unimplemented("not used by this fixture"))
    }

    async fn delete_snapshot(
        &self,
        _: Request<pb::DeleteSnapshotRequest>,
    ) -> std::result::Result<Response<pb::ReusableSnapshot>, Status> {
        Err(Status::unimplemented("not used by this fixture"))
    }
}

struct CatalogServer {
    service: DelayedCatalog,
    task: JoinHandle<std::result::Result<(), tonic::transport::Error>>,
}

impl CatalogServer {
    async fn start(delay_ms: u64) -> (Self, CoordinatorStateSink) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let service = DelayedCatalog {
            delay_ms: Arc::new(AtomicU64::new(delay_ms)),
            publications: Arc::new(AtomicUsize::new(0)),
        };
        let task = tokio::spawn(
            Server::builder()
                .add_service(pb::snapshot_service_server::SnapshotServiceServer::new(
                    service.clone(),
                ))
                .serve_with_incoming(TcpListenerStream::new(listener)),
        );
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let sink = CoordinatorStateSink::new(channel, Duration::from_millis(10))
            .unwrap()
            .with_session("node-session".into());
        (Self { service, task }, sink)
    }
}

impl Drop for CatalogServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[tokio::test]
async fn publication_uses_remaining_snapshot_budget_instead_of_general_rpc_timeout() {
    let (server, sink) = CatalogServer::start(100).await;
    let (_temp, backend, node, spec, assignment) = fixture();
    let node = node.with_snapshot_catalog(Arc::new(sink)).unwrap();
    let handle = node.environment(spec, assignment).unwrap();
    let running = handle.create().await.unwrap();
    let result = handle
        .snapshot(SnapshotRequest {
            kind: adx_core::snapshots::SnapshotKind::Regular,
            operation_id: "publication-budget".into(),
            expected_revision: running.record.revision,
            names: vec!["publication-budget".into()],
            timeout_seconds: 3,
        })
        .await;
    assert!(result.is_ok(), "snapshot budget was cut short: {result:?}");
    assert_eq!(server.service.publications.load(Ordering::SeqCst), 1);
    assert_eq!(
        backend
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| *e == "checkpoint-running")
            .count(),
        1
    );
}

#[tokio::test]
async fn publication_still_stops_at_snapshot_deadline_and_retains_same_capture() {
    let (server, sink) = CatalogServer::start(1500).await;
    let (_temp, backend, node, spec, assignment) = fixture();
    let node = node.with_snapshot_catalog(Arc::new(sink)).unwrap();
    let handle = node.environment(spec, assignment).unwrap();
    let running = handle.create().await.unwrap();
    let request = SnapshotRequest {
        kind: adx_core::snapshots::SnapshotKind::Regular,
        operation_id: "publication-deadline".into(),
        expected_revision: running.record.revision,
        names: vec!["publication-deadline".into()],
        timeout_seconds: 1,
    };
    let began = tokio::time::Instant::now();
    assert!(handle.snapshot(request.clone()).await.is_err());
    assert!(began.elapsed() >= Duration::from_millis(500));
    assert!(began.elapsed() < Duration::from_secs(2));
    assert_eq!(server.service.publications.load(Ordering::SeqCst), 1);
    assert_eq!(
        handle.sync().await.unwrap().record.state,
        EnvironmentState::Running
    );
    server.service.delay_ms.store(0, Ordering::SeqCst);
    let restored_publication = handle.snapshot(request).await.unwrap();
    assert_eq!(
        restored_publication.environment.record.state,
        EnvironmentState::Running
    );
    assert_eq!(server.service.publications.load(Ordering::SeqCst), 2);
    assert_eq!(
        backend
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| *e == "checkpoint-running")
            .count(),
        1
    );
}
