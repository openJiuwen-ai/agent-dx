use crate::{
    config::Config,
    environment_directory::{EnvironmentDirectory, EnvironmentPage},
    ownership::Cache,
    workspace_affinity::WorkspaceAffinityResolver,
};
use adx_observability::trace;
use adx_protocol::control as pb;
use adx_transport::rpc::{RpcChannel, RpcClient, SecurityMode};
use adx_transport::tls::grpc_client_config;
use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::{
    future::Future,
    num::NonZeroUsize,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::Mutex;
use tonic::{Code, Response, Status};

#[cfg(test)]
mod discovery_tests;

pub struct Clients {
    pub config: Config,
    pub workspace: Option<WorkspaceAffinityResolver>,
    directory: Mutex<crate::directory::Directory>,
    tls: RpcClient,
    discovery: Option<adx_discovery::RedisDiscovery>,
    endpoint: Mutex<Cache<(), String>>,
    channels: Mutex<Cache<String, RpcChannel>>,
    auth: Mutex<Cache<[u8; 32], pb::CallerContext>>,
    environments: Mutex<EnvironmentDirectory>,
    /// Readiness: set once the environment/node directory watches have each
    /// delivered their first frame. `/readyz` reflects this so a probe can
    /// distinguish "initial sync not complete" from a running request. A
    /// transient watch reconnect does NOT flip this false, avoiding flap.
    env_synced: Arc<AtomicBool>,
    nodes_synced: Arc<AtomicBool>,
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
        // Affinity is optional: a node without the Workspace service must still
        // start. An unreadable token keeps the resolver so every affected create
        // still follows the configured error policy instead of silently
        // scheduling without affinity.
        let workspace = config.workspace_affinity.clone().map(|affinity| {
            let resolver = WorkspaceAffinityResolver::new(affinity);
            if let Some(error) = resolver.init_error() {
                adx_observability::warn!(
                    error = %error,
                    "Workspace affinity is unavailable; creates follow the configured error policy"
                );
            }
            resolver
        });
        let clients = Arc::new(Self {
            config,
            workspace,
            directory: Mutex::default(),
            tls,
            discovery,
            endpoint: Mutex::new(Cache::new(1)),
            channels: Mutex::new(Cache::new(limit)),
            auth: Mutex::new(Cache::new(limit)),
            environments: Mutex::default(),
            env_synced: Arc::new(AtomicBool::new(false)),
            nodes_synced: Arc::new(AtomicBool::new(false)),
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
                // The stream ended: the primary it was pinned to is gone or
                // draining for handover, so re-resolve before reconnecting
                // instead of waiting out the endpoint cache TTL.
                clients.invalidate_coordinator().await;
                drop(clients);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
        let weak = Arc::downgrade(&clients);
        tokio::spawn(async move {
            while let Some(clients) = weak.upgrade() {
                let _ = clients.watch_directory().await;
                clients.directory.lock().await.clear();
                clients.invalidate_coordinator().await;
                drop(clients);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
        Ok(clients)
    }
    /// Whether the API Server has completed its initial directory sync and is
    /// ready to serve management requests. Probes read this via `/readyz`.
    /// A transient watch reconnect does not flip this false, avoiding flap.
    pub fn ready(&self) -> bool {
        self.env_synced.load(Ordering::Acquire) && self.nodes_synced.load(Ordering::Acquire)
    }

    fn mark_env_synced(&self) {
        self.env_synced.store(true, Ordering::Release);
    }

    fn mark_nodes_synced(&self) {
        self.nodes_synced.store(true, Ordering::Release);
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
            self.mark_env_synced();
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
            self.mark_nodes_synced();
        }
    }
    pub async fn create_environment(
        &self,
        request: pb::CreateEnvironmentRequest,
        budget: Duration,
    ) -> Result<(pb::EnvironmentResult, &'static str), Status> {
        let budget = adx_transport::deadline::create_response_timeout(budget);
        if request.expected_assignment.is_none()
            && self.config.create_mode == crate::config::CreateMode::LocalFirst
            // Only the Coordinator can resolve a Snapshot's source-node hint
            // and compare eligible nodes across shards before admission.
            && request.spec.as_ref().is_none_or(|spec| spec.snapshot_id.is_none())
        {
            let select = trace::Trace::child("apiserver.select_local");
            select.attribute(
                "environment.id",
                request
                    .spec
                    .as_ref()
                    .map_or_else(String::new, |spec| spec.id.clone()),
            );
            let node = select
                .scope_future(async {
                    match request.spec.as_ref() {
                        Some(spec) => self.directory.lock().await.select_for(spec),
                        None => self.directory.lock().await.select(),
                    }
                })
                .await;
            select.attribute(
                "result",
                if node.is_some() { "success" } else { "fail" }.into(),
            );
            if let Some(node) = node {
                select.attribute("node.id", node.node_id.clone());
                drop(select);
                let mut client = pb::node_service_client::NodeServiceClient::new(
                    self.channel(&node.address).await?,
                );
                let mut request = trace::inject(pb::LocalEnvironmentCreateRequest {
                    persist_claim: self.config.persist_local_claim,
                    create: Some(request),
                    node_session_id: node.session_id.clone(),
                });
                request.set_timeout(budget);
                let result = self
                    .rpc_with_timeout(
                        "apiserver.create_local",
                        budget,
                        Box::pin(client.create_local_environment(request)),
                    )
                    .await?;
                if let Some(owner) = local_owner(&node, &result)? {
                    self.put_owner(owner).await?;
                }
                // The entry node owns the only fallback decision. A definitive
                // local miss is forwarded to the central scheduler with the same
                // identity. A result assigned to another node followed that
                // central path and is resolved through the authoritative directory.
                return Ok((result, "local-first"));
            }
            drop(select);
        }
        self.create_via_coordinator(request, budget).await
    }
    /// Central create, retried across a primary change: a structured "not
    /// accepted" rejection from a draining primary guarantees nothing was
    /// enqueued, so the same request identity is retried against the next
    /// primary within the original budget. Any other failure (including a
    /// plain Unavailable, which may follow an applied write) surfaces
    /// untouched for the unknown-outcome query path.
    async fn create_via_coordinator(
        &self,
        request: pb::CreateEnvironmentRequest,
        budget: Duration,
    ) -> Result<(pb::EnvironmentResult, &'static str), Status> {
        self.with_primary_retry(budget, false, |timeout| {
            let request = request.clone();
            async move {
                let mut client = pb::coordinator_service_client::CoordinatorServiceClient::new(
                    self.coordinator().await?,
                );
                let mut request = trace::inject(request);
                request.set_timeout(timeout);
                self.rpc_with_timeout(
                    "apiserver.create",
                    timeout,
                    client.create_environment(request),
                )
                .await
            }
        })
        .await
        .map(|result| (result, "central"))
    }
    pub async fn nodes(&self) -> Result<Vec<pb::NodeEndpoint>, Status> {
        self.directory
            .lock()
            .await
            .snapshot()
            .ok_or_else(|| Status::unavailable("node directory unavailable"))
    }
    pub async fn scheduling_queue(&self) -> Result<pb::GetSchedulingQueueResponse, Status> {
        self.with_primary_retry(self.config.timeout(), true, |timeout| async move {
            let mut client = pb::coordinator_service_client::CoordinatorServiceClient::new(
                self.coordinator().await?,
            );
            self.rpc_with_timeout(
                "apiserver.get_scheduling_queue",
                timeout,
                client.get_scheduling_queue(trace::inject(pb::GetSchedulingQueueRequest {})),
            )
            .await
        })
        .await
    }
    pub async fn set_node_scheduling(
        &self,
        node_id: String,
        accepting_allocations: bool,
    ) -> Result<pb::NodeSchedulingState, Status> {
        self.with_primary_retry(self.config.timeout(), false, |timeout| {
            let node_id = node_id.clone();
            async move {
                let mut client = pb::coordinator_service_client::CoordinatorServiceClient::new(
                    self.coordinator().await?,
                );
                self.rpc_with_timeout(
                    "apiserver.set_node_scheduling",
                    timeout,
                    client.set_node_scheduling(trace::inject(pb::SetNodeSchedulingRequest {
                        node_id,
                        accepting_allocations,
                    })),
                )
                .await
            }
        })
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
    /// Drop the cached Coordinator primary endpoint and its channel so the
    /// next call re-resolves the primary through discovery. Called
    /// whenever a stream or call suggests the cached primary is gone or
    /// draining for handover.
    async fn invalidate_coordinator(&self) {
        let address = self.endpoint.lock().await.get(&());
        self.endpoint.lock().await.remove(&());
        if let Some(address) = address {
            self.channels.lock().await.remove(&address);
        }
    }
    /// Retry a Coordinator call through a primary change, within `budget`.
    /// A structured "not accepted" rejection guarantees nothing was
    /// enqueued, so the same request may be retried against the next
    /// primary with the same identity; `retry_plain_unavailable`
    /// additionally retries a plain Unavailable, which is safe only for
    /// side-effect-free reads. Every retry re-resolves the primary through
    /// discovery — the cached one is stale, gone, or draining — after a
    /// capped, jittered backoff. Failures that may follow an applied write
    /// (deadline exceeded, other statuses) are never retried here: they
    /// surface as outcome-unknown for the query path.
    async fn with_primary_retry<T, F>(
        &self,
        budget: Duration,
        retry_plain_unavailable: bool,
        mut call: impl FnMut(Duration) -> F,
    ) -> Result<T, Status>
    where
        F: std::future::Future<Output = Result<T, Status>>,
    {
        let deadline = tokio::time::Instant::now().checked_add(budget).ok_or_else(|| {
            Status::unavailable("coordinator call timeout is too large")
        })?;
        let mut backoff = Duration::from_millis(200);
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let result = call(remaining).await;
            let retry = match &result {
                Ok(_) => false,
                Err(status) => {
                    adx_protocol::not_accepted_status(status)
                        || (retry_plain_unavailable && status.code() == tonic::Code::Unavailable)
                }
            };
            if !retry {
                return result;
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let delay = jittered(backoff.min(remaining));
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() || delay > remaining {
                return result;
            }
            adx_observability::warn!(
                "coordinator primary change suspected; re-resolving and retrying"
            );
            self.invalidate_coordinator().await;
            tokio::time::sleep(delay).await;
            backoff = (backoff * 2).min(Duration::from_secs(2));
        }
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
        let deadline = tokio::time::Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| Status::unavailable(format!("{name}: RPC timeout is too large")))?;
        // Heap-own generated RPC futures before constructing another async
        // helper frame; carrying them by value can overflow normal workers.
        self.rpc_until(name, deadline, Box::pin(future)).await
    }

    async fn rpc_until<T>(
        &self,
        name: &'static str,
        deadline: tokio::time::Instant,
        future: Pin<Box<impl Future<Output = Result<Response<T>, Status>>>>,
    ) -> Result<T, Status> {
        trace::Trace::child(name)
            .run_result(async {
                let result = match tokio::time::timeout_at(deadline, future).await {
                    Ok(result) => result
                        .map_err(normalize_rpc_error)
                        .map(Response::into_inner),
                    Err(_) => Err(Status::deadline_exceeded(format!(
                        "{name}: control RPC response deadline exceeded"
                    ))),
                };
                match &result {
                    Ok(_) => trace::attribute("result", "success".into()),
                    Err(error) => {
                        trace::attribute("result", "fail".into());
                        trace::attribute("error.code", i32::from(error.code()).to_string());
                        trace::attribute("error.message", error.message().to_owned());
                        trace::error_with(error.message().to_owned());
                    }
                }
                result
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
        let result = self.verify_key(key).await?;
        let caller = result
            .caller
            .clone()
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

    /// Verifies the current credential state without using the local cache.
    /// Revocation and expiry are checked by the authoritative Coordinator.
    pub async fn authenticate_fresh(&self, key: &str) -> Result<pb::CallerContext, Status> {
        let result = self.verify_key(key).await?;
        result
            .caller
            .filter(|caller| !caller.tenant_id.is_empty())
            .ok_or_else(|| Status::unauthenticated("invalid identity"))
    }

    async fn verify_key(&self, key: &str) -> Result<pb::VerifyApiKeyResponse, Status> {
        if !(32..=512).contains(&key.len()) {
            return Err(Status::unauthenticated("invalid API key"));
        }
        // Retry against a re-resolved coordinator primary: during a warm
        // standby takeover the cached channel may point at the departing
        // primary, and the verify must not fail the request outright.
        // Discovery, channel acquisition and the auth RPC share the budget
        // of each attempt; a discovery reconnect must not grant the RPC a
        // fresh budget.
        let result = self
            .with_primary_retry(self.config.timeout(), true, |timeout| async move {
                let deadline = tokio::time::Instant::now().checked_add(timeout).ok_or_else(
                    || {
                        Status::unavailable(
                            "apiserver.verify_key: authentication timeout is too large",
                        )
                    },
                )?;
                let channel = tokio::time::timeout_at(deadline, self.coordinator())
                    .await
                    .map_err(|_| {
                        adx_observability::warn!(
                            phase = "coordinator",
                            category = "deadline",
                            "API authentication Coordinator acquisition deadline exceeded"
                        );
                        Status::deadline_exceeded(
                            "apiserver.verify_key: Coordinator acquisition deadline exceeded",
                        )
                    })??;
                let mut client = pb::auth_service_client::AuthServiceClient::new(channel);
                let remaining =
                    deadline.saturating_duration_since(tokio::time::Instant::now());
                self.rpc_with_timeout(
                    "apiserver.verify_key",
                    remaining,
                    client.verify_api_key(trace::inject(pb::VerifyApiKeyRequest {
                        api_key: key.into(),
                    })),
                )
                .await
            })
            .await?;
        if result.expires_at_unix_seconds != 0 && result.expires_at_unix_seconds <= unix_seconds() {
            return Err(Status::unauthenticated("expired API key"));
        }
        Ok(result)
    }
    pub async fn environment_control(
        &self,
        id: &str,
        caller: &pb::CallerContext,
    ) -> Result<pb::EnvironmentControlRecord, Status> {
        let mut client = pb::coordinator_service_client::CoordinatorServiceClient::new(
            self.coordinator().await?,
        );
        self.rpc(
            "apiserver.get_environment_control",
            client.get_environment_control(trace::inject(pb::GetEnvironmentControlRequest {
                id: id.into(),
                caller: Some(caller.clone()),
            })),
        )
        .await
    }
    pub async fn batch_environment_control(
        &self,
        caller: &pb::CallerContext,
        ids: Vec<String>,
    ) -> Result<Vec<pb::EnvironmentControlRecord>, Status> {
        let mut client = pb::coordinator_service_client::CoordinatorServiceClient::new(
            self.coordinator().await?,
        );
        self.rpc(
            "apiserver.batch_environment_control",
            client.batch_get_environment_control(trace::inject(
                pb::BatchGetEnvironmentControlRequest {
                    ids,
                    caller: Some(caller.clone()),
                },
            )),
        )
        .await
        .map(|response| response.records)
    }
    pub async fn update_environment_control(
        &self,
        id: &str,
        caller: &pb::CallerContext,
        timeout_seconds: u64,
        extend_only: bool,
    ) -> Result<pb::EnvironmentControlRecord, Status> {
        let mut client = pb::coordinator_service_client::CoordinatorServiceClient::new(
            self.coordinator().await?,
        );
        self.rpc(
            "apiserver.update_environment_control",
            client.update_environment_control(trace::inject(pb::UpdateEnvironmentControlRequest {
                id: id.into(),
                caller: Some(caller.clone()),
                timeout_seconds,
                extend_only,
            })),
        )
        .await
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
        let v = self
            .with_primary_retry(self.config.timeout(), false, |timeout| async move {
                let mut client = pb::coordinator_service_client::CoordinatorServiceClient::new(
                    self.coordinator().await?,
                );
                self.rpc_with_timeout(
                    "apiserver.get_environment",
                    timeout,
                    client.get_environment(trace::inject(pb::GetEnvironmentRequest {
                        environment_id: id.into(),
                        caller: Some(caller.clone()),
                    })),
                )
                .await
            })
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
    pub(crate) async fn visible_environments(
        &self,
        caller: &pb::CallerContext,
        after: Option<&str>,
        limit: NonZeroUsize,
    ) -> Result<EnvironmentPage, Status> {
        self.filtered_environments(caller, after, limit, |_| Ok(true))
            .await
    }
    /// Apply entrypoint filters before the common directory counts a page.
    pub(crate) async fn filtered_environments<F>(
        &self,
        caller: &pb::CallerContext,
        after: Option<&str>,
        limit: NonZeroUsize,
        mut visible: F,
    ) -> Result<EnvironmentPage, Status>
    where
        F: FnMut(&pb::EnvironmentRecord) -> Result<bool, Status> + Send,
    {
        self.environments.lock().await.page(after, limit, |entry| {
            if !is_visible(entry, caller)? {
                return Ok(false);
            }
            visible(
                entry
                    .record
                    .as_ref()
                    .ok_or_else(|| Status::data_loss("missing environment record"))?,
            )
        })
    }

    pub async fn put_owner(&self, value: pb::GetEnvironmentResponse) -> Result<(), Status> {
        self.environments.lock().await.put(value)
    }
}

// Half-to-full jitter of a retry backoff: bounded randomization avoids
// synchronized retry storms across API Server replicas after a Coordinator
// primary change.
fn jittered(backoff: Duration) -> Duration {
    let millis = u64::try_from(backoff.as_millis()).unwrap_or(u64::MAX);
    if millis == 0 {
        return backoff;
    }
    let half = millis / 2;
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as u64;
    Duration::from_millis(half + nanos % (half + 1))
}

// Tonic reports a connection reset during handshake/reconnect as Unknown or Cancelled,
// even though its source identifies a transport failure. Keep application
// statuses intact: an RPC timeout or transport failure never proves that a
// lifecycle operation did not execute.
fn normalize_rpc_error(status: Status) -> Status {
    if !matches!(status.code(), Code::Unknown | Code::Cancelled) {
        return status;
    }
    let mut source = std::error::Error::source(&status);
    let mut transport = false;
    while let Some(error) = source {
        if error.is::<tonic::TimeoutExpired>() {
            return Status::with_details_and_metadata(
                Code::DeadlineExceeded,
                "control RPC response deadline exceeded",
                Bytes::copy_from_slice(status.details()),
                status.metadata().clone(),
            );
        }
        if error.is::<tonic::transport::Error>() {
            transport = true;
        }
        source = error.source();
    }
    if transport {
        return Status::with_details_and_metadata(
            Code::Unavailable,
            status.message(),
            Bytes::copy_from_slice(status.details()),
            status.metadata().clone(),
        );
    }
    status
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

fn is_visible(
    entry: &pb::GetEnvironmentResponse,
    caller: &pb::CallerContext,
) -> Result<bool, Status> {
    match authorize(caller, entry.record.as_ref()) {
        Ok(()) => Ok(true),
        Err(error) if error.code() == Code::PermissionDenied => Ok(false),
        Err(error) => Err(error),
    }
}

fn local_owner(
    node: &pb::NodeEndpoint,
    result: &pb::EnvironmentResult,
) -> Result<Option<pb::GetEnvironmentResponse>, Status> {
    if result.durability != pb::Durability::Published as i32 {
        return Ok(None);
    }
    let record = result
        .record
        .as_ref()
        .ok_or_else(|| Status::data_loss("local create returned no environment record"))?;
    let assignment = record
        .assignment
        .as_ref()
        .ok_or_else(|| Status::data_loss("local create returned no assignment"))?;
    if assignment.node_id != node.node_id {
        return Ok(None);
    }
    if node.address.is_empty() || node.relay_address.is_empty() {
        return Err(Status::data_loss("selected node endpoint is incomplete"));
    }
    Ok(Some(pb::GetEnvironmentResponse {
        record: Some(record.clone()),
        node_address: node.address.clone(),
        relay_address: node.relay_address.clone(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rpc_test_clients(address: &str) -> Arc<Clients> {
        Clients::new(
            serde_json::from_value(serde_json::json!({
                "listen": "127.0.0.1:0",
                "coordinator_address": address,
                "internal_security": "network",
                "ingress_mode": "standalone",
                "rpc_timeout_seconds": 1,
                "cache_entries": 8,
                "auth_cache_ttl_seconds": 1
            }))
            .unwrap(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn refused_node_connection_is_retryable_unavailable() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);
        let clients = rpc_test_clients(&address);
        let mut node = pb::node_service_client::NodeServiceClient::new(
            clients.channel(&address).await.unwrap(),
        );
        let error = clients
            .rpc(
                "test.delete",
                node.delete_environment(pb::DeleteEnvironmentRequest::default()),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::Unavailable, "{error:?}");
        let detail = crate::errors::ErrorDetail::from_status(
            &error,
            "request",
            Some("operation"),
            Some("environment"),
            true,
        );
        assert_eq!(detail.retry, adx_error::RetryDirective::SameOperation);
        assert_eq!(detail.outcome, adx_error::OperationOutcome::Unknown);
    }

    #[tokio::test]
    async fn application_internal_error_keeps_its_code() {
        let clients = rpc_test_clients("http://127.0.0.1:1");
        let error = clients
            .rpc::<()>("test.application", async {
                Err(Status::internal("application invariant failed"))
            })
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::Internal);
        assert_eq!(error.message(), "application invariant failed");
        for original in [
            Status::unknown("application error"),
            Status::cancelled("request cancelled"),
        ] {
            let normalized = normalize_rpc_error(original.clone());
            assert_eq!(normalized.code(), original.code());
            assert_eq!(normalized.message(), original.message());
        }
    }

    #[test]
    fn tonic_timeout_is_a_deadline_not_a_disconnect() {
        let original = Status::from_error(Box::new(tonic::TimeoutExpired(())));
        assert_eq!(original.code(), Code::Cancelled);
        let normalized = normalize_rpc_error(original);
        assert_eq!(normalized.code(), Code::DeadlineExceeded);
        assert!(normalized.message().contains("response deadline exceeded"));
        assert!(adx_protocol::deadline_failure(&normalized).is_none());
    }

    #[test]
    fn jitter_stays_within_half_to_full_of_the_backoff() {
        for _ in 0..64 {
            let jitter = jittered(Duration::from_millis(200));
            assert!(jitter >= Duration::from_millis(100));
            assert!(jitter <= Duration::from_millis(200));
        }
        assert_eq!(jittered(Duration::ZERO), Duration::ZERO);
    }

    #[tokio::test]
    async fn a_not_accepted_rejection_is_retried_through_a_fresh_primary() {
        let clients = rpc_test_clients("http://127.0.0.1:1");
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        // A draining primary rejects the create as "not accepted" —
        // nothing was enqueued — so the same identity is retried against
        // the next primary within the original budget.
        let counter = attempts.clone();
        let result: Result<usize, Status> = clients
            .with_primary_retry(Duration::from_millis(600), false, move |_timeout| {
                let counter = counter.clone();
                async move {
                    if counter.fetch_add(1, Ordering::SeqCst) == 0 {
                        Err(adx_protocol::not_accepted(
                            "Coordinator handover in progress; operation not accepted",
                        ))
                    } else {
                        Ok(7)
                    }
                }
            })
            .await;
        assert_eq!(result.unwrap(), 7);
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_plain_unavailable_is_never_retried_for_writes() {
        let clients = rpc_test_clients("http://127.0.0.1:1");
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        // A plain Unavailable may follow an applied write, so a mutating
        // call surfaces it immediately for the unknown-outcome query path.
        let counter = attempts.clone();
        let result: Result<usize, Status> = clients
            .with_primary_retry(Duration::from_millis(300), false, move |_timeout| {
                let counter = counter.clone();
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    Err(Status::unavailable("connection reset"))
                }
            })
            .await;
        assert_eq!(result.unwrap_err().code(), Code::Unavailable);
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_plain_unavailable_is_retried_for_reads() {
        let clients = rpc_test_clients("http://127.0.0.1:1");
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        // A read has no side effects, so a plain Unavailable (including a
        // draining primary that has not answered yet) is retried.
        let counter = attempts.clone();
        let result: Result<usize, Status> = clients
            .with_primary_retry(Duration::from_millis(600), true, move |_timeout| {
                let counter = counter.clone();
                async move {
                    if counter.fetch_add(1, Ordering::SeqCst) == 0 {
                        Err(Status::unavailable("connection reset"))
                    } else {
                        Ok(9)
                    }
                }
            })
            .await;
        assert_eq!(result.unwrap(), 9);
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn retries_stop_when_the_budget_is_exhausted() {
        let clients = rpc_test_clients("http://127.0.0.1:1");
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = attempts.clone();
        let result: Result<usize, Status> = clients
            .with_primary_retry(Duration::from_millis(400), false, move |_timeout| {
                let counter = counter.clone();
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    Err(adx_protocol::not_accepted("handover in progress"))
                }
            })
            .await;
        // The original failure surfaces once the budget ends, after at
        // least one bounded-backoff retry.
        assert_eq!(result.unwrap_err().code(), Code::Unavailable);
        assert!(
            attempts.load(Ordering::SeqCst) >= 2,
            "at least one retry must happen"
        );
        // Half-to-full jitter over the capped backoff consumes at least half
        // of the remaining budget per retry, so a 400ms budget allows ~11
        // attempts in the adversarial low-draw tail — still two orders of
        // magnitude below a no-backoff spin (thousands of attempts). The
        // bound separates exponential backoff from a spin with margin.
        assert!(
            attempts.load(Ordering::SeqCst) <= 16,
            "bounded backoff must not spin the budget away: {}",
            attempts.load(Ordering::SeqCst)
        );
    }

    #[tokio::test]
    async fn disconnected_node_connection_is_retryable_unavailable() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                drop(socket);
            }
        });
        let clients = rpc_test_clients(&address);
        let mut node = pb::node_service_client::NodeServiceClient::new(
            clients.channel(&address).await.unwrap(),
        );
        let error = clients
            .rpc(
                "test.delete",
                node.delete_environment(pb::DeleteEnvironmentRequest::default()),
            )
            .await
            .unwrap_err();
        task.abort();
        assert_eq!(error.code(), Code::Unavailable, "{error:?}");
    }

    fn owned(id: &str, tenant: &str) -> pb::GetEnvironmentResponse {
        pb::GetEnvironmentResponse {
            record: Some(pb::EnvironmentRecord {
                observation: None,
                execution_artifacts: vec![],
                snapshot_source: None,
                failure: None,
                spec: Some(pb::EnvironmentSpec {
                    compatibility: None,
                    id: id.into(),
                    tenant_id: tenant.into(),
                    ..Default::default()
                }),
                assignment: Some(pb::Assignment {
                    environment_id: id.into(),
                    node_id: "node".into(),
                    generation: 1,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn visibility_excludes_other_tenants_and_administrator_sees_all() {
        let entries = [owned("one", "tenant-one"), owned("two", "tenant-two")];
        let tenant = pb::CallerContext {
            tenant_id: "tenant-one".into(),
            ..Default::default()
        };
        assert!(is_visible(&entries[0], &tenant).unwrap());
        assert!(!is_visible(&entries[1], &tenant).unwrap());
        let admin = pb::CallerContext {
            administrator: true,
            ..tenant
        };
        assert!(entries
            .iter()
            .all(|entry| is_visible(entry, &admin).unwrap()));
    }

    #[test]
    fn local_create_owner_uses_the_selected_node_addresses() {
        let node = pb::NodeEndpoint {
            node_id: "node".into(),
            address: "http://node:9000".into(),
            relay_address: "https://node:9443".into(),
            ..Default::default()
        };
        let result = pb::EnvironmentResult {
            newly_created: false,
            record: owned("capsule", "tenant").record,
            durability: pb::Durability::Published as i32,
            ..Default::default()
        };

        let owner = local_owner(&node, &result).unwrap().unwrap();

        assert_eq!(owner.node_address, node.address);
        assert_eq!(owner.relay_address, node.relay_address);
        assert_eq!(owner.record.unwrap().assignment.unwrap().generation, 1);
    }

    #[test]
    fn centrally_forwarded_create_is_not_cached_as_local() {
        let node = pb::NodeEndpoint {
            node_id: "selected".into(),
            address: "http://node:9000".into(),
            relay_address: "https://node:9443".into(),
            ..Default::default()
        };
        let result = pb::EnvironmentResult {
            newly_created: false,
            record: owned("capsule", "tenant").record,
            durability: pb::Durability::Published as i32,
            ..Default::default()
        };

        assert!(local_owner(&node, &result).unwrap().is_none());
    }

    #[test]
    fn journaled_local_result_is_not_published_to_the_api_cache() {
        let node = pb::NodeEndpoint {
            node_id: "node".into(),
            address: "http://node:9000".into(),
            relay_address: "https://node:9443".into(),
            ..Default::default()
        };
        let result = pb::EnvironmentResult {
            newly_created: false,
            record: owned("capsule", "tenant").record,
            durability: pb::Durability::Journaled as i32,
            ..Default::default()
        };

        assert!(local_owner(&node, &result).unwrap().is_none());
    }
}
