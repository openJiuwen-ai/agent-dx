//! Opt-in native backend qualification. Use a dedicated, empty sandboxd/main
//! instance; ADX_SANDBOXD_TEST_SOCKET may be an SSH-forwarded Unix socket.
#[path = "common/authority.rs"]
mod authority;
use adx_core::EnvironmentSpec;
use adxlet::{
    sandboxd::{proto::*, Config, Sandboxd},
    RuntimeDriver,
};
use hyper_util::rt::TokioIo;
use sandbox_service_client::SandboxServiceClient;
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::net::{UnixListener, UnixStream};
use tokio_stream::wrappers::UnixListenerStream;
use tonic::{
    transport::{Channel, Endpoint},
    Request, Response, Status,
};
use tower::service_fn;

#[derive(Clone)]
struct Proxy {
    client: SandboxServiceClient<Channel>,
    delayed: bool,
    request: Arc<Mutex<Option<StartRequest>>>,
}
#[tonic::async_trait]
impl sandbox_service_server::SandboxService for Proxy {
    async fn start(
        &self,
        request: Request<StartRequest>,
    ) -> Result<Response<StartResponse>, Status> {
        let request = request.into_inner();
        *self.request.lock().unwrap() = Some(request.clone());
        if self.delayed {
            // Hold delivery past the client's failed outcome and empty inventory.
            return Err(Status::deadline_exceeded(
                "injected Start transport deadline",
            ));
        }
        self.client.clone().start(request).await?;
        Err(Status::unavailable(
            "injected response loss after real Start",
        ))
    }
    async fn checkpoint(
        &self,
        request: Request<CheckpointRequest>,
    ) -> Result<Response<CheckpointResponse>, Status> {
        self.client.clone().checkpoint(request).await
    }
    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        self.client.clone().delete(request).await
    }
    async fn wait(&self, request: Request<WaitRequest>) -> Result<Response<WaitResponse>, Status> {
        self.client.clone().wait(request).await
    }
    async fn list(
        &self,
        request: Request<ListSandboxesRequest>,
    ) -> Result<Response<ListSandboxesResponse>, Status> {
        self.client.clone().list(request).await
    }
    async fn stats(
        &self,
        request: Request<StatsRequest>,
    ) -> Result<Response<StatsResponse>, Status> {
        self.client.clone().stats(request).await
    }
    async fn list_available_runtimes(
        &self,
        request: Request<ListAvailableRuntimesRequest>,
    ) -> Result<Response<ListAvailableRuntimesResponse>, Status> {
        self.client.clone().list_available_runtimes(request).await
    }
    async fn set_network_policy(
        &self,
        request: Request<SetNetworkPolicyRequest>,
    ) -> Result<Response<SetNetworkPolicyResponse>, Status> {
        self.client.clone().set_network_policy(request).await
    }
}

async fn cycle(authority: &mut authority::Fixture, node: &adxlet::Adxlet) -> Result<(), Status> {
    adxlet::node_reconciliation::reconcile_node(
        &mut authority.client,
        &mut authority.registration,
        node,
        None,
        Duration::from_secs(30),
        Duration::from_secs(1),
    )
    .await
}

#[tokio::test]
#[ignore = "requires a dedicated empty sandboxd/main and reachable OCI image"]
async fn main_backend_timeout_late_start_lost_response_and_restart_converge() {
    let socket = PathBuf::from(std::env::var("ADX_SANDBOXD_TEST_SOCKET").unwrap());
    let image = std::env::var("ADX_SANDBOXD_TEST_IMAGE").unwrap();
    let runtime = std::env::var("ADX_SANDBOXD_TEST_RUNTIME").unwrap_or_else(|_| "runc".into());
    let channel = Endpoint::from_static("http://localhost")
        .connect_with_connector(service_fn(move |_| {
            let socket = socket.clone();
            async move { UnixStream::connect(socket).await.map(TokioIo::new) }
        }))
        .await
        .unwrap();
    let mut client = SandboxServiceClient::new(channel);
    assert!(
        client
            .list(ListSandboxesRequest::default())
            .await
            .unwrap()
            .into_inner()
            .sandboxes
            .is_empty(),
        "use an empty dedicated backend"
    );
    let config = Config {
        command: vec!["/bin/sh".into(), "-c".into(), "sleep 300".into()],
        rpc_timeout: Duration::from_secs(120),
        ..Default::default()
    };
    let mut spec: EnvironmentSpec = serde_json::from_value(serde_json::json!({
        "id":"native-fault", "tenant_id":"test", "image":image,
        "runtime_class":runtime, "priority":0,
        "resources":{"cpu_millis":100,"memory_bytes":67108864,"disk_bytes":0}
    }))
    .unwrap();
    if let Ok(path) = std::env::var("ADX_SANDBOXD_TEST_ROOTFS") {
        spec.execution.rootfs = Some(adx_core::execution::Rootfs {
            readonly: false,
            source: adx_core::execution::StorageSource::Local(path),
        });
    }
    let mut sentinel =
        adxlet::sandboxd::start_request(&spec, "native-fault-1", 1, &[], &config).unwrap();
    sentinel.labels = HashMap::from([("fixture".into(), "unmanaged-sentinel".into())]);
    let sentinel = client.start(sentinel).await.unwrap().into_inner();
    assert!(!sentinel.id.is_empty());
    let mut owned = vec![sentinel.id.clone()];
    // Capture panics inside a task so the outer scope can reclaim native runtimes.
    let test_client = client.clone();
    let test_owned = Arc::new(Mutex::new(vec![]));
    let cleanup_ids = test_owned.clone();
    let result = tokio::spawn(async move {
        for (delayed, restart) in [(false, false), (true, false), (true, true)] {
            let directory = tempfile::tempdir().unwrap();
            let socket = directory.path().join("proxy.sock");
            let proxy = Proxy { client: test_client.clone(), delayed, request: Arc::default() };
            let listener = UnixListener::bind(&socket).unwrap();
            let service = sandbox_service_server::SandboxServiceServer::new(proxy.clone());
            let server = tokio::spawn(async move {
                tonic::transport::Server::builder().add_service(service)
                    .serve_with_incoming(UnixListenerStream::new(listener)).await.unwrap();
            });
            let adapter = Sandboxd::connect(socket.clone(), config.clone()).await.unwrap();
            let error = adapter.start(&spec, "native-fault-1", 1, &[]).await.unwrap_err();
            assert!(proxy.request.lock().unwrap().is_some(), "fault must reach Start: {error:?}");
            let mut node = authority::manager(Arc::new(adapter.clone()));
            let mut authority = authority::Fixture::new().await;
            authority.cycle(&node).await.unwrap();
            assert!(authority.authority.reports.lock().unwrap().last().unwrap().accepting_allocations);
            if restart {
                // Reconstruct both driver and node, losing all prior Start state.
                node = authority::manager(Arc::new(Sandboxd::connect(socket, config.clone()).await.unwrap()));
                authority = authority::Fixture::new().await;
                cycle(&mut authority, &node).await.unwrap();
            }
            if delayed {
                let request = proxy.request.lock().unwrap().clone().unwrap();
                let late = test_client.clone().start(request).await.unwrap().into_inner();
                cleanup_ids.lock().unwrap().push(late.id);
            }
            tokio::time::timeout(Duration::from_secs(30), async {
                loop {
                    let result = cycle(&mut authority, &node).await;
                    assert!(node.accepting_allocations());
                    // A concurrent native Delete may hold inventory past the
                    // short sampling budget. Retry that transient read failure
                    // within the same bounded convergence deadline.
                    if let Err(error) = result {
                        assert_eq!(error.code(), tonic::Code::Unavailable);
                        assert_eq!(error.message(), "inventory timed out");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                    let inventory = test_client.clone().list(ListSandboxesRequest::default()).await.unwrap().into_inner().sandboxes;
                    assert!(inventory.iter().any(|s| s.labels.get("fixture").is_some_and(|v| v == "unmanaged-sentinel")));
                    if inventory.iter().all(|s| !s.labels.contains_key("adx.environment_id")) { break; }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }).await.unwrap();
            assert_eq!(node.used(), adx_core::Resources::default());
            server.abort();
            eprintln!("PASS native sandboxd: delayed={delayed}, restart={restart}; no managed residuals, sentinel retained");
        }
    }).await;
    owned.extend(test_owned.lock().unwrap().clone());
    // This fixture was verified empty before starting: all remaining resources
    // belong to this test, including a successful Start with a lost reply.
    owned.extend(
        client
            .list(ListSandboxesRequest::default())
            .await
            .unwrap()
            .into_inner()
            .sandboxes
            .into_iter()
            .map(|s| s.id),
    );
    owned.sort();
    owned.dedup();
    for id in owned {
        client
            .delete(DeleteRequest {
                id,
                ..Default::default()
            })
            .await
            .unwrap();
    }
    assert!(client
        .list(ListSandboxesRequest::default())
        .await
        .unwrap()
        .into_inner()
        .sandboxes
        .is_empty());
    result.unwrap();
}
