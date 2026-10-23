//! In-process authority server; all node cycles use the production gRPC client.
use adx_core::{EnvironmentRecord, Resources};
use adx_protocol::control as pb;
use adxlet::{Adxlet, Durability, Readiness, Routes, RuntimeDriver, RuntimeObservation, StateSink};
use async_trait::async_trait;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{net::TcpListener, task::JoinHandle, time::Instant};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};

#[derive(Clone, Default)]
pub struct Authority {
    pub reports: Arc<Mutex<Vec<pb::RegisterNodeRequest>>>,
    pub records: Arc<Mutex<Vec<pb::EnvironmentRecord>>>,
    pub unavailable: Arc<AtomicBool>,
    pub reject_ready: Arc<AtomicBool>,
    pub stall_report: Arc<AtomicBool>,
    pub requires_handshake: Arc<AtomicBool>,
    pub block_inspect: Arc<AtomicBool>,
    pub inspect_entered: Arc<tokio::sync::Notify>,
    pub inspect_release: Arc<tokio::sync::Notify>,
}
#[async_trait]
impl pb::coordinator_service_server::CoordinatorService for Authority {
    async fn register_node(
        &self,
        request: Request<pb::RegisterNodeRequest>,
    ) -> Result<Response<pb::RegisterNodeResponse>, Status> {
        if self.stall_report.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        let report = request.into_inner();
        assert!(!report.reconciling || !report.accepting_allocations);
        if self.requires_handshake.load(Ordering::SeqCst) {
            if !report.reconciling {
                return Err(Status::failed_precondition("node requires reconciliation"));
            }
            self.requires_handshake.store(false, Ordering::SeqCst);
        }
        let rejected = !report.reconciling && self.reject_ready.load(Ordering::SeqCst);
        self.reports.lock().unwrap().push(report);
        if rejected {
            return Err(Status::unavailable("ready report rejected"));
        }
        Ok(Response::new(pb::RegisterNodeResponse::default()))
    }
    async fn inspect_node(
        &self,
        _: Request<pb::InspectNodeRequest>,
    ) -> Result<Response<pb::InspectNodeResponse>, Status> {
        assert!(
            !self
                .reports
                .lock()
                .unwrap()
                .last()
                .unwrap()
                .accepting_allocations
        );
        if self.unavailable.load(Ordering::SeqCst) {
            return Err(Status::unavailable("authority unavailable"));
        }
        if self.block_inspect.load(Ordering::SeqCst) {
            self.inspect_entered.notify_one();
            self.inspect_release.notified().await;
        }
        Ok(Response::new(pb::InspectNodeResponse {
            records: self.records.lock().unwrap().clone(),
            ..Default::default()
        }))
    }
    type WatchNodesStream = tokio_stream::Empty<Result<pb::NodeDirectory, Status>>;
    async fn watch_nodes(
        &self,
        _: Request<pb::WatchNodesRequest>,
    ) -> Result<Response<Self::WatchNodesStream>, Status> {
        Err(Status::unimplemented("unused test RPC"))
    }
    async fn get_scheduling_queue(
        &self,
        _: Request<pb::GetSchedulingQueueRequest>,
    ) -> Result<Response<pb::GetSchedulingQueueResponse>, Status> {
        Err(Status::unimplemented("unused test RPC"))
    }
    async fn set_node_scheduling(
        &self,
        _: Request<pb::SetNodeSchedulingRequest>,
    ) -> Result<Response<pb::NodeSchedulingState>, Status> {
        Err(Status::unimplemented("unused test RPC"))
    }
    async fn prepare_create(
        &self,
        _: Request<pb::LocalEnvironmentCreateRequest>,
    ) -> Result<Response<pb::PreparedEnvironment>, Status> {
        Err(Status::unimplemented("unused test RPC"))
    }
    async fn claim_environment(
        &self,
        _: Request<pb::ClaimEnvironmentRequest>,
    ) -> Result<Response<pb::ClaimEnvironmentResponse>, Status> {
        Err(Status::unimplemented("unused test RPC"))
    }
    async fn forward_create(
        &self,
        _: Request<pb::LocalEnvironmentCreateRequest>,
    ) -> Result<Response<pb::EnvironmentResult>, Status> {
        Err(Status::unimplemented("unused test RPC"))
    }
    async fn create_environment(
        &self,
        _: Request<pb::CreateEnvironmentRequest>,
    ) -> Result<Response<pb::EnvironmentResult>, Status> {
        Err(Status::unimplemented("unused test RPC"))
    }
    async fn get_environment(
        &self,
        _: Request<pb::GetEnvironmentRequest>,
    ) -> Result<Response<pb::GetEnvironmentResponse>, Status> {
        Err(Status::unimplemented("unused test RPC"))
    }
    async fn get_environment_metadata(
        &self,
        _: Request<pb::GetEnvironmentMetadataRequest>,
    ) -> Result<Response<pb::EnvironmentMetadataRecord>, Status> {
        Err(Status::unimplemented("unused test RPC"))
    }
    async fn list_environment_metadata(
        &self,
        _: Request<pb::ListEnvironmentMetadataRequest>,
    ) -> Result<Response<pb::ListEnvironmentMetadataResponse>, Status> {
        Err(Status::unimplemented("unused test RPC"))
    }
    async fn list_e2b_environment_metadata(
        &self,
        _: Request<pb::ListEnvironmentMetadataRequest>,
    ) -> Result<Response<pb::ListEnvironmentMetadataResponse>, Status> {
        Err(Status::unimplemented("unused test RPC"))
    }
    async fn get_environment_control(
        &self,
        _: Request<pb::GetEnvironmentControlRequest>,
    ) -> Result<Response<pb::EnvironmentControlRecord>, Status> {
        Err(Status::unimplemented("unused test RPC"))
    }
    async fn update_environment_control(
        &self,
        _: Request<pb::UpdateEnvironmentControlRequest>,
    ) -> Result<Response<pb::EnvironmentControlRecord>, Status> {
        Err(Status::unimplemented("unused test RPC"))
    }
    async fn list_environment_controls(
        &self,
        _: Request<pb::ListEnvironmentControlsRequest>,
    ) -> Result<Response<pb::ListEnvironmentControlsResponse>, Status> {
        Err(Status::unimplemented("unused test RPC"))
    }
    async fn ensure_running(
        &self,
        _: Request<pb::EnsureRunningRequest>,
    ) -> Result<Response<pb::GetEnvironmentResponse>, Status> {
        Err(Status::unimplemented("unused test RPC"))
    }
    async fn retire_dormant(
        &self,
        _: Request<pb::GetEnvironmentRequest>,
    ) -> Result<Response<pb::EnvironmentResult>, Status> {
        Err(Status::unimplemented("unused test RPC"))
    }
    async fn commit_environment(
        &self,
        _: Request<pb::CommitEnvironmentRequest>,
    ) -> Result<Response<pb::CommitEnvironmentResponse>, Status> {
        Err(Status::unimplemented("unused test RPC"))
    }
}

pub struct Fixture {
    pub authority: Authority,
    pub client:
        pb::coordinator_service_client::CoordinatorServiceClient<adx_transport::rpc::RpcChannel>,
    pub registration: pb::RegisterNodeRequest,
    task: JoinHandle<()>,
}
impl Fixture {
    pub async fn new() -> Self {
        let authority = Authority::default();
        let service =
            pb::coordinator_service_server::CoordinatorServiceServer::new(authority.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        let transport =
            adx_transport::rpc::RpcClient::network(adx_protocol::auth::Principal::Node("n".into()));
        let channel = transport
            .endpoint(&address.to_string())
            .unwrap()
            .connect()
            .await
            .unwrap();
        Self {
            authority,
            client: pb::coordinator_service_client::CoordinatorServiceClient::new(
                transport.wrap(channel),
            ),
            registration: pb::RegisterNodeRequest {
                node_id: "n".into(),
                session_id: "test-session".into(),
                runtime_classes: vec!["runc".into(), "supervisor".into(), "conch".into()],
                ..Default::default()
            },
            task,
        }
    }
    pub async fn cycle(&mut self, manager: &Adxlet) -> Result<(), Status> {
        self.cycle_with_journal(manager, None).await
    }
    pub async fn cycle_with_journal(
        &mut self,
        manager: &Adxlet,
        journal: Option<&adxlet::journal::JournalSink>,
    ) -> Result<(), Status> {
        adxlet::node_reconciliation::reconcile_node(
            &mut self.client,
            &mut self.registration,
            manager,
            journal,
            Duration::from_secs(2),
            Duration::from_millis(10),
            Instant::now() + Duration::from_secs(60),
        )
        .await
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct Hooks;
#[async_trait]
impl Readiness for Hooks {
    async fn wait_ready(&self, _: &EnvironmentRecord) -> adx_core::Result<()> {
        Ok(())
    }
}
#[async_trait]
impl Routes for Hooks {
    async fn activate(&self, _: &EnvironmentRecord) -> adx_core::Result<()> {
        Ok(())
    }
    async fn retire(&self, _: &EnvironmentRecord) -> adx_core::Result<()> {
        Ok(())
    }
    async fn retire_orphan(&self, _: &RuntimeObservation) -> adx_core::Result<()> {
        Ok(())
    }
}
#[async_trait]
impl StateSink for Hooks {
    async fn commit(&self, _: &EnvironmentRecord) -> adx_core::Result<Durability> {
        Ok(Durability::Published)
    }
}
pub fn manager(runtime: Arc<dyn RuntimeDriver>) -> Adxlet {
    let hooks = Arc::new(Hooks);
    let node = Adxlet::new("n".into(), runtime, hooks.clone(), hooks.clone(), hooks);
    node.update_capacity(
        Resources {
            cpu_millis: 10000,
            memory_bytes: 1 << 30,
            disk_bytes: 0,
        },
        Duration::from_secs(60),
    )
    .unwrap();
    node
}
