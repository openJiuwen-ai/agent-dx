use crate::{config::Config, environment_directory::EnvironmentDirectory, ownership::Cache};
use adx_observability::trace;
use adx_protocol::control as pb;
use adx_transport::rpc::{RpcChannel, RpcClient, SecurityMode};
use adx_transport::tls::grpc_client_config;
use sha2::{Digest, Sha256};
use std::{
    future::Future,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::Mutex;
use tonic::{Response, Status};

pub struct Clients {
    pub config: Config,
    directory: Mutex<crate::directory::Directory>,
    tls: RpcClient,
    discovery: Option<adx_discovery::RedisDiscovery>,
    endpoint: Mutex<Cache<(), String>>,
    channels: Mutex<Cache<String, RpcChannel>>,
    auth: Mutex<Cache<[u8; 32], pb::CallerContext>>,
    environments: Mutex<EnvironmentDirectory>,
}
impl Clients {
    pub fn new(config: Config) -> Result<Arc<Self>, Box<dyn std::error::Error>> {
        config.validate()?;
        let tls = if config.internal_security == SecurityMode::Network {
            RpcClient::network(adx_protocol::auth::Principal::ApiServer)
        } else {
            grpc_client_config(
                &config.ca,
                &config.certificate,
                &config.private_key,
                &config.server_name,
            )?
            .into()
        };
        let discovery = config
            .discovery
            .as_ref()
            .map(|d| {
                adx_discovery::RedisDiscovery::new(&d.redis_url, &d.namespace, config.timeout())
            })
            .transpose()?;
        let limit = config.cache_entries;
        let clients = Arc::new(Self {
            config,
            directory: Mutex::default(),
            tls,
            discovery,
            endpoint: Mutex::new(Cache::new(1)),
            channels: Mutex::new(Cache::new(limit)),
            auth: Mutex::new(Cache::new(limit)),
            environments: Mutex::default(),
        });
        let weak = Arc::downgrade(&clients);
        tokio::spawn(async move {
            while let Some(clients) = weak.upgrade() {
                let result = clients.watch_environments().await;
                if result.as_ref().is_err_and(|error| {
                    matches!(
                        error.code(),
                        tonic::Code::OutOfRange
                            | tonic::Code::FailedPrecondition
                            | tonic::Code::DataLoss
                    )
                }) {
                    clients.environments.lock().await.clear();
                }
                drop(clients);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
        let weak = Arc::downgrade(&clients);
        tokio::spawn(async move {
            while let Some(clients) = weak.upgrade() {
                let _ = clients.watch_directory().await;
                clients.directory.lock().await.clear();
                drop(clients);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
        Ok(clients)
    }
    async fn watch_environments(&self) -> Result<(), Status> {
        let mut client =
            pb::environment_directory_service_client::EnvironmentDirectoryServiceClient::new(
                self.coordinator().await?,
            )
            .max_decoding_message_size(64 * 1024 * 1024);
        let mut stream = self
            .rpc(
                "apiserver.watch_environments",
                client.watch_environments(pb::WatchEnvironmentsRequest {}),
            )
            .await?;
        while let Some(frame) = stream.message().await? {
            self.environments.lock().await.update(frame)?;
        }
        Err(Status::unavailable("environment directory closed"))
    }
    async fn watch_directory(&self) -> Result<(), Status> {
        let mut client = pb::coordinator_service_client::CoordinatorServiceClient::new(
            self.coordinator().await?,
        );
        let mut stream = self
            .rpc(
                "apiserver.watch_nodes",
                client.watch_nodes(pb::WatchNodesRequest {}),
            )
            .await?;
        loop {
            let frame = tokio::time::timeout(Duration::from_secs(5), stream.message())
                .await
                .map_err(|_| Status::unavailable("node directory expired"))??
                .ok_or_else(|| Status::unavailable("node directory closed"))?;
            self.directory.lock().await.update(frame)?;
        }
    }
    pub async fn create_environment(
        &self,
        request: pb::CreateEnvironmentRequest,
        budget: Duration,
    ) -> Result<pb::EnvironmentResult, Status> {
        if self.config.create_mode == crate::config::CreateMode::LocalFirst {
            let node = self.directory.lock().await.select();
            if let Some(node) = node {
                let mut client = pb::node_service_client::NodeServiceClient::new(
                    self.channel(&node.address).await?,
                );
                let mut request = trace::inject(pb::LocalEnvironmentCreateRequest {
                    create: Some(request),
                    node_session_id: node.session_id,
                });
                request.set_timeout(budget);
                let result = self
                    .rpc_with_timeout(
                        "apiserver.create_local",
                        budget,
                        client.create_local_environment(request),
                    )
                    .await;
                // The entry node owns the only fallback decision. A definitive
                // local miss is forwarded to the central scheduler with the same
                // identity; transport failure remains an unknown outcome.
                return result;
            }
        }
        let mut client = pb::coordinator_service_client::CoordinatorServiceClient::new(
            self.coordinator().await?,
        );
        self.rpc_with_timeout(
            "apiserver.create",
            budget,
            client.create_environment(trace::inject(request)),
        )
        .await
    }
    pub async fn nodes(&self) -> Result<Vec<pb::NodeEndpoint>, Status> {
        self.directory
            .lock()
            .await
            .snapshot()
            .ok_or_else(|| Status::unavailable("node directory unavailable"))
    }
    pub async fn scheduling_queue(&self) -> Result<pb::GetSchedulingQueueResponse, Status> {
        let mut client = pb::coordinator_service_client::CoordinatorServiceClient::new(
            self.coordinator().await?,
        );
        self.rpc(
            "apiserver.get_scheduling_queue",
            client.get_scheduling_queue(trace::inject(pb::GetSchedulingQueueRequest {})),
        )
        .await
    }
    pub async fn set_node_scheduling(
        &self,
        node_id: String,
        accepting_allocations: bool,
    ) -> Result<pb::NodeSchedulingState, Status> {
        let mut client = pb::coordinator_service_client::CoordinatorServiceClient::new(
            self.coordinator().await?,
        );
        self.rpc(
            "apiserver.set_node_scheduling",
            client.set_node_scheduling(trace::inject(pb::SetNodeSchedulingRequest {
                node_id,
                accepting_allocations,
            })),
        )
        .await
    }
    pub async fn coordinator(&self) -> Result<RpcChannel, Status> {
        let address = if let Some(discovery) = &self.discovery {
            let cached = self.endpoint.lock().await.get(&());
            if let Some(v) = cached {
                v
            } else {
                let value = discovery
                    .lookup()
                    .await
                    .map_err(|_| Status::unavailable("Coordinator discovery unavailable"))?;
                self.endpoint.lock().await.insert(
                    (),
                    value.address.clone(),
                    Duration::from_secs(
                        self.config
                            .discovery
                            .as_ref()
                            .expect("discovery client requires discovery configuration")
                            .poll_seconds,
                    ),
                );
                value.address
            }
        } else {
            self.config.coordinator_address.clone()
        };
        self.channel(&address).await
    }
    pub async fn channel(&self, address: &str) -> Result<RpcChannel, Status> {
        let mut channels = self.channels.lock().await;
        if let Some(v) = channels.get(&address.to_owned()) {
            return Ok(v);
        }
        let endpoint = self
            .tls
            .endpoint(address)
            .map_err(|_| Status::unavailable("invalid RPC endpoint or security mode"))?
            .connect_timeout(self.config.timeout());
        let channel = self.tls.wrap(endpoint.connect_lazy());
        channels.insert(
            address.to_owned(),
            channel.clone(),
            Duration::from_secs(3600),
        );
        Ok(channel)
    }
    pub async fn rpc<T>(
        &self,
        name: &'static str,
        future: impl Future<Output = Result<Response<T>, Status>>,
    ) -> Result<T, Status> {
        self.rpc_with_timeout(name, self.config.timeout(), future)
            .await
    }
    pub async fn rpc_with_timeout<T>(
        &self,
        name: &'static str,
        timeout: Duration,
        future: impl Future<Output = Result<Response<T>, Status>>,
    ) -> Result<T, Status> {
        trace::Trace::child(name)
            .run_result(async {
                tokio::time::timeout(timeout, future)
                    .await
                    .map_err(|_| Status::deadline_exceeded("control RPC deadline exceeded"))?
                    .map(Response::into_inner)
            })
            .await
    }
    pub async fn authenticate(&self, key: &str) -> Result<pb::CallerContext, Status> {
        if !(32..=512).contains(&key.len()) {
            return Err(Status::unauthenticated("invalid API key"));
        }
        let digest: [u8; 32] = Sha256::digest(key.as_bytes()).into();
        if let Some(c) = self.auth.lock().await.get(&digest) {
            return Ok(c);
        }
        let mut client = pb::auth_service_client::AuthServiceClient::new(self.coordinator().await?);
        let result = self
            .rpc(
                "apiserver.verify_key",
                client.verify_api_key(trace::inject(pb::VerifyApiKeyRequest {
                    api_key: key.into(),
                })),
            )
            .await?;
        let caller = result
            .caller
            .filter(|c| !c.tenant_id.is_empty())
            .ok_or_else(|| Status::unauthenticated("invalid identity"))?;
        let mut ttl = self.config.auth_cache_ttl_seconds;
        if result.expires_at_unix_seconds != 0 {
            ttl = ttl.min(
                result
                    .expires_at_unix_seconds
                    .saturating_sub(unix_seconds()),
            );
            if ttl == 0 {
                return Err(Status::unauthenticated("expired API key"));
            }
        }
        self.auth
            .lock()
            .await
            .insert(digest, caller.clone(), Duration::from_secs(ttl));
        Ok(caller)
    }
    pub async fn owner(
        &self,
        id: &str,
        caller: &pb::CallerContext,
        refresh: bool,
    ) -> Result<pb::GetEnvironmentResponse, Status> {
        if !refresh {
            let value = self.environments.lock().await.get(id)?;
            authorize(caller, value.record.as_ref())?;
            return Ok(value);
        }
        let mut client = pb::coordinator_service_client::CoordinatorServiceClient::new(
            self.coordinator().await?,
        );
        let v = self
            .rpc(
                "apiserver.get_environment",
                client.get_environment(trace::inject(pb::GetEnvironmentRequest {
                    environment_id: id.into(),
                    caller: Some(caller.clone()),
                })),
            )
            .await?;
        authorize(caller, v.record.as_ref())?;
        if v.record
            .as_ref()
            .and_then(|r| r.spec.as_ref())
            .is_none_or(|s| s.id != id)
            || v.node_address.is_empty()
        {
            return Err(Status::data_loss("incomplete owner response"));
        }
        self.put_owner(v.clone()).await?;
        Ok(v)
    }
    pub async fn put_owner(&self, value: pb::GetEnvironmentResponse) -> Result<(), Status> {
        self.environments.lock().await.put(value)
    }
}

pub fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
pub fn authorize(
    caller: &pb::CallerContext,
    record: Option<&pb::EnvironmentRecord>,
) -> Result<(), Status> {
    let r = record.ok_or_else(|| Status::data_loss("missing environment record"))?;
    let spec = r
        .spec
        .as_ref()
        .ok_or_else(|| Status::data_loss("missing instance spec"))?;
    let assignment = r
        .assignment
        .as_ref()
        .filter(|a| a.environment_id == spec.id && a.generation > 0)
        .ok_or_else(|| Status::data_loss("invalid assignment"))?;
    if assignment.node_id.is_empty() {
        return Err(Status::data_loss("missing node identity"));
    }
    if !caller.administrator && caller.tenant_id != spec.tenant_id {
        return Err(Status::permission_denied(
            "environment belongs to another tenant",
        ));
    }
    Ok(())
}
