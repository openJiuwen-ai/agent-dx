//! A-1/A-2/A-3 (API Server): `/readyz` folds embedded readiness and follows
//! draining; draining rejects new management operations BEFORE acceptance
//! with the structured not-started contract; in-process embedded Activator
//! operations are counted so shutdown can wait for them ("management HTTP at
//! zero ≠ activations finished").
use adx_activator::local_environment::EnvironmentApplication;
use adx_apiserver::{clients::Clients, config::Config, environment_service::EnvironmentService};
use adx_protocol::control as pb;
use futures_util::StreamExt;
use serde_json::json;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::Notify;
use tonic::{Request, Response, Status};

#[derive(Default)]
struct State {
    hold_release_owner: AtomicBool,
    release_owner_entered: Notify,
    release_owner_release: Notify,
}
#[derive(Clone)]
struct Service(Arc<State>);

macro_rules! service_impl {
    ($trait:path, { $($implemented:item)* }, [$($name:ident($request:ident) -> $response:ident),* $(,)?]) => {
        #[tonic::async_trait]
        impl $trait for Service {
            $($implemented)*
            $(async fn $name(&self, _: Request<pb::$request>) -> Result<Response<pb::$response>, Status> {
                Err(Status::unimplemented("unused test RPC"))
            })*
        }
    };
}

service_impl!(pb::coordinator_service_server::CoordinatorService, {
    type WatchNodesStream = std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<pb::NodeDirectory, Status>> + Send>>;
    async fn watch_nodes(&self, _: Request<pb::WatchNodesRequest>) -> Result<Response<Self::WatchNodesStream>, Status> {
        Ok(Response::new(Box::pin(futures_util::stream::once(async {
            Ok(pb::NodeDirectory { epoch: 1, valid_for_millis: 5000, nodes: vec![] })
        })
        .chain(futures_util::stream::pending::<Result<
            pb::NodeDirectory,
            Status,
        >>()))))
    }
}, [
    get_scheduling_queue(GetSchedulingQueueRequest) -> GetSchedulingQueueResponse,
    set_node_scheduling(SetNodeSchedulingRequest) -> NodeSchedulingState,
    prepare_create(LocalEnvironmentCreateRequest) -> PreparedEnvironment,
    reserve_local_environment(ClaimEnvironmentRequest) -> ClaimEnvironmentResponse,
    claim_environment(ClaimEnvironmentRequest) -> ClaimEnvironmentResponse,
    forward_create(LocalEnvironmentCreateRequest) -> EnvironmentResult,
    unregister_node(UnregisterNodeRequest) -> UnregisterNodeResponse,
    register_node(RegisterNodeRequest) -> RegisterNodeResponse,
    inspect_node(InspectNodeRequest) -> InspectNodeResponse,
    create_environment(CreateEnvironmentRequest) -> EnvironmentResult,
    get_environment(GetEnvironmentRequest) -> GetEnvironmentResponse,
    ensure_running(EnsureRunningRequest) -> GetEnvironmentResponse,
    retire_dormant(GetEnvironmentRequest) -> EnvironmentResult,
    commit_environment(CommitEnvironmentRequest) -> CommitEnvironmentResponse,
    get_environment_control(GetEnvironmentControlRequest) -> EnvironmentControlRecord,
    batch_get_environment_control(BatchGetEnvironmentControlRequest)
        -> BatchGetEnvironmentControlResponse,
    update_environment_control(UpdateEnvironmentControlRequest) -> EnvironmentControlRecord,
]);

#[tonic::async_trait]
impl pb::environment_directory_service_server::EnvironmentDirectoryService for Service {
    type WatchEnvironmentsStream = std::pin::Pin<
        Box<dyn futures_util::Stream<Item = Result<pb::EnvironmentDirectoryFrame, Status>> + Send>,
    >;
    async fn watch_environments(
        &self,
        _: Request<pb::WatchEnvironmentsRequest>,
    ) -> Result<Response<Self::WatchEnvironmentsStream>, Status> {
        let full = pb::EnvironmentDirectoryFrame {
            epoch: 1,
            revision: 1,
            reset: true,
            ..Default::default()
        };
        Ok(Response::new(Box::pin(
            futures_util::stream::once(async { Ok(full) }).chain(futures_util::stream::pending::<
                Result<pb::EnvironmentDirectoryFrame, Status>,
            >()),
        )))
    }
}

service_impl!(pb::volume_service_server::VolumeService, {
    async fn release_owner(
        &self,
        _: Request<pb::ReleaseVolumeOwnerRequest>,
    ) -> Result<Response<pb::DeleteVolumeResponse>, Status> {
        if self.0.hold_release_owner.load(Ordering::SeqCst) {
            self.0.release_owner_entered.notify_one();
            self.0.release_owner_release.notified().await;
        }
        Ok(Response::new(pb::DeleteVolumeResponse {}))
    }
}, [
    ensure_volume(EnsureVolumeRequest) -> Volume,
    get_volume(GetVolumeRequest) -> VolumeInfo,
    list_volumes(ListVolumesRequest) -> ListVolumesResponse,
    delete_volume(GetVolumeRequest) -> DeleteVolumeResponse,
]);

struct Fixture {
    clients: Arc<Clients>,
    service: Arc<EnvironmentService>,
    state: Arc<State>,
}

impl Fixture {
    async fn new() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let state = Arc::new(State::default());
        let endpoint = Service(state.clone());
        let server = tokio::spawn(async move {
            let incoming = futures_util::stream::unfold(listener, |listener| async {
                let socket = listener.accept().await.map(|(socket, _)| socket);
                Some((socket, listener))
            });
            tonic::transport::Server::builder()
                .add_service(pb::coordinator_service_server::CoordinatorServiceServer::new(endpoint.clone()))
                .add_service(pb::environment_directory_service_server::EnvironmentDirectoryServiceServer::new(endpoint.clone()))
                .add_service(pb::volume_service_server::VolumeServiceServer::new(endpoint))
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });
        let config: Config = serde_json::from_value(json!({
            "listen":"127.0.0.1:0", "coordinator_address":address, "internal_security":"network",
            "ingress_mode":"standalone", "rpc_timeout_seconds":2, "cache_entries":8,
            "auth_cache_ttl_seconds":1
        }))
        .unwrap();
        let clients = Clients::new(config).unwrap();
        let service = EnvironmentService::new(clients.clone());
        // Wait for both directory watches to deliver their first frame.
        tokio::time::timeout(Duration::from_secs(3), async {
            while !clients.ready() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("directory watches must synchronize");
        std::mem::forget(server);
        Self {
            clients,
            service,
            state,
        }
    }
}

/// Serve the Api on a private listener and return its address.
async fn serve_api(api: Arc<adx_apiserver::http::Api>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let http = tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            let api = api.clone();
            tokio::spawn(async move {
                let service = hyper::service::service_fn(move |request| api.clone().serve(request));
                hyper::server::conn::http1::Builder::new()
                    .serve_connection(hyper_util::rt::TokioIo::new(socket), service)
                    .await
                    .unwrap();
            });
        }
    });
    std::mem::forget(http);
    format!("http://{address}")
}

/// A-1: `/readyz` folds the embedded component's admission readiness into
/// the directory-sync criteria — an embedded component that has not
/// finished initializing keeps the replica not ready.
#[tokio::test]
async fn readyz_folds_embedded_readiness() {
    adx_transport::install_crypto_provider();
    let fixture = Fixture::new().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();

    let unready = adx_apiserver::http::Api::new(fixture.clients.clone()).unwrap();
    unready.set_embedded_readiness(Box::new(|| false));
    let address = serve_api(unready).await;
    assert_eq!(
        client
            .get(format!("{address}/readyz"))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "directory synced but the embedded component is not ready"
    );

    let ready = adx_apiserver::http::Api::new(fixture.clients.clone()).unwrap();
    ready.set_embedded_readiness(Box::new(|| true));
    let address = serve_api(ready).await;
    assert_eq!(
        client
            .get(format!("{address}/readyz"))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::OK
    );
}

/// A-2: once draining, new management operations are rejected BEFORE
/// acceptance (here: before authentication) with the structured
/// not-started contract; probes stay answerable.
#[tokio::test]
async fn draining_rejects_new_operations_before_acceptance() {
    adx_transport::install_crypto_provider();
    let fixture = Fixture::new().await;
    let api = adx_apiserver::http::Api::new(fixture.clients.clone()).unwrap();
    let address = serve_api(api.clone()).await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();

    // Before draining the same unauthenticated request reaches
    // authentication and is rejected there.
    assert_eq!(
        client
            .post(format!("{address}/api/environment/v1/environments"))
            .body("{}")
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::UNAUTHORIZED
    );

    api.begin_draining();

    let response = client
        .post(format!("{address}/api/environment/v1/environments"))
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["error"]["code"], "UNAVAILABLE");
    assert_eq!(body["error"]["retry"], "after_backoff");
    assert_eq!(body["error"]["outcome"], "not_started");
    assert!(
        body["message"]
            .as_str()
            .is_some_and(|message| message.contains("draining")),
        "{body}"
    );
    // Probes stay answerable; readiness follows draining, liveness does not.
    assert_eq!(
        client
            .get(format!("{address}/readyz"))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        client
            .get(format!("{address}/healthz"))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::OK
    );
}

/// A-3: in-process embedded Activator operations are counted while they run,
/// so the shutdown completion criterion can wait for work that the
/// management HTTP connection count cannot see.
#[tokio::test]
async fn embedded_operations_are_counted_while_in_flight() {
    let fixture = Fixture::new().await;
    assert_eq!(fixture.service.embedded_operations(), 0);

    // Hold the volume-owner release inside the Coordinator mock so the
    // embedded operation stays in flight observably.
    fixture
        .state
        .hold_release_owner
        .store(true, Ordering::SeqCst);
    let service = fixture.service.clone();
    let state = fixture.state.clone();
    let waiter = tokio::spawn(async move {
        let inner = service.clone();
        let operation = tokio::spawn(async move {
            // Through the embedded seam (EnvironmentApplication), the way
            // the embedded Activator would call it.
            EnvironmentApplication::release_volume_owner(&*inner, "tenant", "owner").await
        });
        state.release_owner_entered.notified().await;
        // The guard is held while the backend call is in flight.
        assert_eq!(service.embedded_operations(), 1);
        state.release_owner_release.notify_one();
        let _ = operation.await;
    });
    tokio::time::timeout(Duration::from_secs(3), waiter)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        fixture.service.embedded_operations(),
        0,
        "counter returns to zero when the operation completes"
    );
}

/// The optional plain-HTTP readiness listener (`readiness_listen`) serves the
/// same admission semantics as the main listener's `/readyz` — for kubelet
/// httpGet probes in mtls deployments where the main port demands client
/// certificates. Ready replicas answer 200, draining replicas 503, and any
/// other path is a probe-only 404.
#[tokio::test]
async fn readiness_listener_serves_probe_semantics() {
    let fixture = Fixture::new().await;
    let api = adx_apiserver::http::Api::new(fixture.clients.clone()).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(adx_apiserver::http::serve_readiness(listener, api.clone()));
    std::mem::forget(server);

    // The fixture waits for the directory watches: a synchronized replica
    // answers 200.
    let response = reqwest::get(format!("http://{address}/readyz"))
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);

    // Draining flips the probe to 503 before the process exits.
    api.begin_draining();
    let response = reqwest::get(format!("http://{address}/readyz"))
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);

    // Anything else is a plain 404 — the listener is probe-only.
    let response = reqwest::get(format!("http://{address}/metrics"))
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
}
