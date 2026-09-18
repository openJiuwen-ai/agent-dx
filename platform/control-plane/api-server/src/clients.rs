use crate::{config::Config, instance_directory::InstanceDirectory, ownership::Cache};
use adx_observability::trace;
use adx_protocol::control as pb;
use sha2::{Digest, Sha256};
use std::{
    future::Future,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::Mutex;
use tonic::{
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity},
    Response, Status,
};

pub struct Clients {
    pub config: Config,
    directory: Mutex<crate::directory::Directory>,
    tls: ClientTlsConfig,
    discovery: Option<adx_discovery::RedisDiscovery>,
    endpoint: Mutex<Cache<(), String>>,
    channels: Mutex<Cache<String, Channel>>,
    auth: Mutex<Cache<[u8; 32], pb::CallerContext>>,
    instances: Mutex<InstanceDirectory>,
}
impl Clients {
    pub fn new(config: Config) -> Result<Arc<Self>, Box<dyn std::error::Error>> {
        config.validate()?;
        let tls = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(std::fs::read(&config.ca)?))
            .identity(Identity::from_pem(
                std::fs::read(&config.certificate)?,
                std::fs::read(&config.private_key)?,
            ))
            .domain_name(&config.server_name);
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
            instances: Mutex::default(),
        });
        let weak = Arc::downgrade(&clients);
        tokio::spawn(async move {
            while let Some(clients) = weak.upgrade() {
                let result = clients.watch_instances().await;
                if result.as_ref().is_err_and(|error| {
                    matches!(
                        error.code(),
                        tonic::Code::OutOfRange
                            | tonic::Code::FailedPrecondition
                            | tonic::Code::DataLoss
                    )
                }) {
                    clients.instances.lock().await.clear();
                }
                drop(clients);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
        if clients.config.create_mode == crate::config::CreateMode::LocalFirst {
            let weak = Arc::downgrade(&clients);
            tokio::spawn(async move {
                while let Some(clients) = weak.upgrade() {
                    let _ = clients.watch_directory().await;
                    clients.directory.lock().await.clear();
                    drop(clients);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            });
        }
        Ok(clients)
    }
    async fn watch_instances(&self) -> Result<(), Status> {
        let mut client =
            pb::instance_directory_service_client::InstanceDirectoryServiceClient::new(
                self.master().await?,
            )
            .max_decoding_message_size(64 * 1024 * 1024);
        let mut stream = self
            .rpc(
                "api_server.watch_instances",
                client.watch_instances(pb::WatchInstancesRequest {}),
            )
            .await?;
        while let Some(frame) = stream.message().await? {
            self.instances.lock().await.update(frame)?;
        }
        Err(Status::unavailable("instance directory closed"))
    }
    async fn watch_directory(&self) -> Result<(), Status> {
        let mut client = pb::master_service_client::MasterServiceClient::new(self.master().await?);
        let mut stream = self
            .rpc(
                "api_server.watch_nodes",
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
    pub async fn create_instance(
        &self,
        request: pb::CreateInstanceRequest,
        budget: Duration,
    ) -> Result<pb::InstanceResult, Status> {
        if self.config.create_mode == crate::config::CreateMode::LocalFirst {
            let node = self.directory.lock().await.select();
            if let Some(node) = node {
                let mut client = pb::node_service_client::NodeServiceClient::new(
                    self.channel(&node.address).await?,
                );
                let result = self
                    .rpc_with_timeout(
                        "api_server.create_local",
                        budget / 2,
                        client.create_local_instance(trace::inject(pb::LocalCreateRequest {
                            create: Some(request.clone()),
                            node_session_id: node.session_id,
                        })),
                    )
                    .await;
                match result {
                    Ok(result) => return Ok(result),
                    Err(e)
                        if matches!(
                            e.code(),
                            tonic::Code::Unavailable
                                | tonic::Code::DeadlineExceeded
                                | tonic::Code::FailedPrecondition
                        ) => {}
                    Err(e) => return Err(e),
                }
                // Same identity/specification. Master serializes this against
                // any in-flight claim; NotFound never licenses a replacement ID.
                let mut client =
                    pb::master_service_client::MasterServiceClient::new(self.master().await?);
                return self
                    .rpc_with_timeout(
                        "api_server.create",
                        budget / 2,
                        client.create_instance(trace::inject(request)),
                    )
                    .await;
            }
        }
        let mut client = pb::master_service_client::MasterServiceClient::new(self.master().await?);
        self.rpc_with_timeout(
            "api_server.create",
            budget,
            client.create_instance(trace::inject(request)),
        )
        .await
    }
    pub async fn master(&self) -> Result<Channel, Status> {
        let address = if let Some(discovery) = &self.discovery {
            let cached = self.endpoint.lock().await.get(&());
            if let Some(v) = cached {
                v
            } else {
                let value = discovery
                    .lookup()
                    .await
                    .map_err(|_| Status::unavailable("Master discovery unavailable"))?;
                self.endpoint.lock().await.insert(
                    (),
                    value.address.clone(),
                    Duration::from_secs(self.config.discovery.as_ref().unwrap().poll_seconds),
                );
                value.address
            }
        } else {
            self.config.master_address.clone()
        };
        self.channel(&address).await
    }
    pub async fn channel(&self, address: &str) -> Result<Channel, Status> {
        let address = if address.contains("://") {
            address.to_string()
        } else {
            format!("https://{address}")
        };
        if !address.starts_with("https://") {
            return Err(Status::unavailable("internal RPC requires TLS"));
        }
        let mut channels = self.channels.lock().await;
        if let Some(v) = channels.get(&address) {
            return Ok(v);
        }
        let endpoint = Endpoint::from_shared(address.clone())
            .map_err(|_| Status::unavailable("invalid RPC endpoint"))?
            .tls_config(self.tls.clone())
            .map_err(|_| Status::unavailable("invalid RPC TLS configuration"))?
            .connect_timeout(self.config.timeout());
        let channel = endpoint.connect_lazy();
        channels.insert(address, channel.clone(), Duration::from_secs(3600));
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
        let mut client = pb::auth_service_client::AuthServiceClient::new(self.master().await?);
        let result = self
            .rpc(
                "api_server.verify_key",
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
    ) -> Result<pb::GetInstanceResponse, Status> {
        if !refresh {
            let value = self.instances.lock().await.get(id)?;
            authorize(caller, value.record.as_ref())?;
            return Ok(value);
        }
        let mut client = pb::master_service_client::MasterServiceClient::new(self.master().await?);
        let v = self
            .rpc(
                "api_server.get_instance",
                client.get_instance(trace::inject(pb::GetInstanceRequest {
                    instance_id: id.into(),
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
    pub async fn put_owner(&self, value: pb::GetInstanceResponse) -> Result<(), Status> {
        self.instances.lock().await.put(value)
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
    record: Option<&pb::InstanceRecord>,
) -> Result<(), Status> {
    let r = record.ok_or_else(|| Status::data_loss("missing instance record"))?;
    let spec = r
        .spec
        .as_ref()
        .ok_or_else(|| Status::data_loss("missing instance spec"))?;
    let assignment = r
        .assignment
        .as_ref()
        .filter(|a| a.instance_id == spec.id && a.generation > 0)
        .ok_or_else(|| Status::data_loss("invalid assignment"))?;
    if assignment.node_id.is_empty() {
        return Err(Status::data_loss("missing node identity"));
    }
    if !caller.administrator && caller.tenant_id != spec.tenant_id {
        return Err(Status::permission_denied(
            "instance belongs to another tenant",
        ));
    }
    Ok(())
}
