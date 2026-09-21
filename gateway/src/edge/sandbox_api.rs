//! General Sandbox capability adapter. Platform remains the lifecycle authority.
//! Agent callers and external Sandbox requests share this exact implementation.
use super::DataPlaneL4Connector;
use crate::common::protocol::ConnectTarget;
use adx_agent_core::sandbox::*;
use adx_agent_core::{
    limits,
    transport::{service_origin, validate_service_token, RequestProgress, ServiceAuth},
};
use adx_discovery::RedisDiscovery;
use adx_protocol::control as pb;
use adx_transport::tls::TlsFiles;
use async_trait::async_trait;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, sync::Arc, time::Duration};
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

type Result<T> = std::result::Result<T, SandboxError>;
const EXECUTION_HASH: &str = "ADX_AGENT_EXECUTION_HASH";
const SERVICE_PORTS: &str = "ADX_AGENT_SERVICE_PORTS";
const HAS_ENTRYPOINT: &str = "ADX_AGENT_HAS_ENTRYPOINT";
pub const PATH: &str = "/api/sandbox/v2/instances";

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreinstalledProfile {
    pub image: String,
    pub isolation_runtime: String,
    pub entrypoint: Vec<String>,
    pub working_dir: String,
    pub user: Option<String>,
    /// Absolute path baked into the image; None only for an idle RRT sandbox.
    pub config_path: Option<String>,
    pub rrt_port: u16,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxConfig {
    pub redis_url: String,
    pub platform_namespace: String,
    /// Must carry the existing Platform Frontend role, not the Edge role certificate.
    pub frontend_tls: TlsFiles,
    pub rpc_timeout_seconds: u64,
    pub preinstalled_profiles: Vec<PreinstalledProfile>,
}

pub struct PlatformSandbox {
    discovery: RedisDiscovery,
    tls: ClientTlsConfig,
    timeout: Duration,
    connector: DataPlaneL4Connector,
    profiles: Vec<PreinstalledProfile>,
    rrt_token: String,
    master: tokio::sync::Mutex<Option<Channel>>,
}
impl PlatformSandbox {
    pub fn new(
        config: SandboxConfig,
        connector: DataPlaneL4Connector,
        rrt_token: String,
    ) -> std::result::Result<Self, Box<dyn std::error::Error>> {
        if config.rpc_timeout_seconds == 0 || config.rpc_timeout_seconds > 60 {
            return Err("positive Sandbox RPC timeout <=60 seconds and RRT token of at least 32 bytes required".into());
        }
        validate_service_token(&rrt_token)?;
        validate_profiles(&config.preinstalled_profiles)?;
        let (_, tls, _) = config.frontend_tls.load()?;
        let timeout = Duration::from_secs(config.rpc_timeout_seconds);
        Ok(Self {
            discovery: RedisDiscovery::new(&config.redis_url, &config.platform_namespace, timeout)?,
            tls,
            timeout,
            connector,
            profiles: config.preinstalled_profiles,
            rrt_token,
            master: tokio::sync::Mutex::new(None),
        })
    }
    async fn connect(&self, address: String) -> Result<Channel> {
        service_origin(&address, false).map_err(SandboxError::Unavailable)?;
        Endpoint::from_shared(address)
            .map_err(|_| SandboxError::Unavailable("invalid Platform endpoint".into()))?
            .tls_config(self.tls.clone())
            .map_err(|_| SandboxError::Unavailable("Platform TLS configuration failed".into()))?
            .connect_timeout(self.timeout)
            .timeout(self.timeout)
            .connect()
            .await
            .map_err(|_| SandboxError::Unavailable("Platform connection unavailable".into()))
    }
    async fn master(&self) -> Result<Channel> {
        let mut cached = self.master.lock().await;
        if let Some(channel) = cached.as_ref() {
            return Ok(channel.clone());
        }
        let discovered = self
            .discovery
            .lookup()
            .await
            .map_err(|_| SandboxError::Unavailable("Master discovery unavailable".into()))?;
        let channel = self.connect(discovered.address).await?;
        *cached = Some(channel.clone());
        Ok(channel)
    }
    async fn invalidate(&self) {
        *self.master.lock().await = None;
    }
    async fn inspect(&self, tenant: &str, id: &str) -> Result<Option<pb::GetCapsuleResponse>> {
        validate_identity(tenant, id)?;
        let mut client = pb::master_service_client::MasterServiceClient::new(self.master().await?);
        match client
            .get_capsule(pb::GetCapsuleRequest {
                capsule_id: id.into(),
                caller: Some(caller(tenant)),
            })
            .await
        {
            Ok(response) => {
                let response = response.into_inner();
                validate_record(response.record.as_ref(), tenant, id)?;
                Ok(Some(response))
            }
            Err(status) if status.code() == tonic::Code::NotFound => Ok(None),
            Err(status) => {
                if transient(&status) {
                    self.invalidate().await;
                }
                Err(rpc_error(status, false))
            }
        }
    }
    async fn observation(
        &self,
        record: pb::CapsuleRecord,
        node_proxy: &str,
    ) -> Result<SandboxObservation> {
        let spec = record.spec.as_ref().ok_or_else(|| {
            SandboxError::Unavailable("Platform returned no specification".into())
        })?;
        let mut result = SandboxObservation {
            id: spec.id.clone(),
            tenant: spec.tenant_id.clone(),
            phase: match pb::CapsuleState::try_from(record.state).ok() {
                Some(pb::CapsuleState::Running) => SandboxPhase::Running,
                Some(pb::CapsuleState::Deleted) => SandboxPhase::Deleted,
                Some(pb::CapsuleState::Failed) => SandboxPhase::Failed,
                _ => SandboxPhase::Creating,
            },
            ready: false,
            runtime_id: (!record.runtime_id.is_empty()).then(|| record.runtime_id.clone()),
            message: None,
        };
        if result.phase != SandboxPhase::Running {
            return Ok(result);
        }
        match tokio::time::timeout(self.timeout, self.runtime_ready(&record, node_proxy)).await {
            Ok(Ok(true)) => result.ready = true,
            Ok(Ok(false)) => {
                result.message = Some("waiting for RRT or declared service ports".into())
            }
            Ok(Err(error)) => {
                result.phase = SandboxPhase::Failed;
                result.message = Some(error);
            }
            Err(_) => result.message = Some("runtime readiness observation timed out".into()),
        }
        Ok(result)
    }
    async fn runtime_ready(
        &self,
        record: &pb::CapsuleRecord,
        node_proxy: &str,
    ) -> std::result::Result<bool, String> {
        let spec = record.spec.as_ref().ok_or("missing specification")?;
        let port = spec
            .env
            .get("RRT_HTTP_PORT")
            .and_then(|p| p.parse::<u16>().ok())
            .filter(|p| *p != 0)
            .ok_or("RRT readiness metadata missing")?;
        let entrypoint = spec
            .env
            .get(HAS_ENTRYPOINT)
            .ok_or("entrypoint metadata missing")?
            == "true";
        let response = match self
            .runtime_request(record, node_proxy, port, entrypoint)
            .await
        {
            Ok(value) => value,
            Err(_) => return Ok(false), // Transport absence during startup is not a confirmed process failure.
        };
        if entrypoint {
            match response.get("status").and_then(|v| v.as_str()) {
                Some("running") => (),
                Some("error") => {
                    return Err(response
                        .get("message")
                        .and_then(|v| v.as_str())
                        .unwrap_or("RRT entrypoint error")
                        .into())
                }
                _ => return Err(format!("RRT entrypoint is not running: {}", response)),
            }
        } else if response.get("status").and_then(|v| v.as_str()) != Some("ok") {
            return Ok(false);
        }
        let ports: Vec<u16> = serde_json::from_str(
            spec.env
                .get(SERVICE_PORTS)
                .ok_or("service metadata missing")?,
        )
        .map_err(|_| "invalid service metadata")?;
        for port in ports {
            let (_stop, cancelled) = tokio::sync::watch::channel(false);
            let target = connect_target(record, port).map_err(|e| e.to_string())?;
            if self
                .connector
                .connect_stream_with_activity(node_proxy, &target, cancelled, true)
                .await
                .is_err()
            {
                return Ok(false);
            }
        }
        Ok(true)
    }
    async fn runtime_request(
        &self,
        record: &pb::CapsuleRecord,
        node_proxy: &str,
        port: u16,
        entrypoint: bool,
    ) -> Result<serde_json::Value> {
        let (_stop, cancelled) = tokio::sync::watch::channel(false);
        let target = connect_target(record, port)?;
        let stream = self
            .connector
            .connect_stream_with_activity(node_proxy, &target, cancelled, true)
            .await
            .map_err(|_| SandboxError::Unavailable("RRT stream unavailable".into()))?;
        let (mut client, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .map_err(|_| SandboxError::Unavailable("RRT HTTP handshake failed".into()))?;
        let task = tokio::spawn(async move {
            let _ = connection.await;
        });
        struct Abort(tokio::task::JoinHandle<()>);
        impl Drop for Abort {
            fn drop(&mut self) {
                self.0.abort();
            }
        }
        let _guard = Abort(task);
        let mut request = http::Request::builder()
            .method(if entrypoint { "POST" } else { "GET" })
            .uri(if entrypoint { "/invoke" } else { "/healthz" })
            .header("host", "rrt")
            .header("content-type", "application/json");
        if entrypoint {
            let token = record
                .spec
                .as_ref()
                .and_then(|s| s.env.get("RRT_HTTP_TOKEN"))
                .ok_or_else(|| SandboxError::Unavailable("RRT token missing".into()))?;
            request = request.header("x-auth", token);
        }
        let body = if entrypoint {
            Bytes::from_static(b"{\"action\":\"entrypoint.poll\",\"args\":{\"wait_timeout\":0}}")
        } else {
            Bytes::new()
        };
        let request = request
            .body(Full::new(body))
            .map_err(|_| SandboxError::Unavailable("RRT request encoding failed".into()))?;
        let response = client
            .send_request(request)
            .await
            .map_err(|_| SandboxError::Unavailable("RRT request failed".into()))?;
        if response.status() != http::StatusCode::OK {
            return Err(SandboxError::Unavailable("RRT status unavailable".into()));
        }
        let body = http_body_util::Limited::new(response.into_body(), limits::RRT_RESPONSE_BYTES)
            .collect()
            .await
            .map_err(|_| SandboxError::Unavailable("RRT response invalid or too large".into()))?
            .to_bytes();
        serde_json::from_slice(&body)
            .map_err(|_| SandboxError::Unavailable("invalid RRT status JSON".into()))
    }
}
fn caller(tenant: &str) -> pb::CallerContext {
    pb::CallerContext {
        tenant_id: tenant.into(),
        administrator: false,
    }
}
fn validate_identity(tenant: &str, id: &str) -> Result<()> {
    if [tenant, id].iter().any(|v| {
        v.trim().is_empty() || v.len() > limits::IDENTIFIER_BYTES || v.chars().any(char::is_control)
    }) {
        return Err(SandboxError::Invalid("invalid tenant or Sandbox ID".into()));
    }
    Ok(())
}
fn validate_record(record: Option<&pb::CapsuleRecord>, tenant: &str, id: &str) -> Result<()> {
    let record =
        record.ok_or_else(|| SandboxError::Unavailable("Platform returned no instance".into()))?;
    let spec = record
        .spec
        .as_ref()
        .ok_or_else(|| SandboxError::Unavailable("Platform returned no specification".into()))?;
    if spec.id != id
        || spec.tenant_id != tenant
        || record
            .assignment
            .as_ref()
            .is_some_and(|a| a.capsule_id != id)
    {
        return Err(SandboxError::Unavailable(
            "Platform response identity mismatch".into(),
        ));
    }
    Ok(())
}
fn connect_target(record: &pb::CapsuleRecord, port: u16) -> Result<ConnectTarget> {
    let spec = record
        .spec
        .as_ref()
        .ok_or_else(|| SandboxError::Unavailable("missing specification".into()))?;
    let assignment = record
        .assignment
        .as_ref()
        .ok_or_else(|| SandboxError::Unavailable("missing assignment".into()))?;
    let runtime = record
        .runtime
        .as_ref()
        .ok_or_else(|| SandboxError::Unavailable("missing runtime".into()))?;
    if !adx_protocol::valid_runtime_id(&spec.id, assignment.generation, &runtime.id) {
        return Err(SandboxError::Unavailable("invalid runtime identity".into()));
    }
    Ok(ConnectTarget {
        instance_id: spec.id.clone(),
        workload_id: runtime.id.clone(),
        target_ip: record
            .ip
            .parse()
            .map_err(|_| SandboxError::Unavailable("runtime address unavailable".into()))?,
        target_port: port,
        request_id: uuid::Uuid::new_v4().to_string(),
    })
}
fn transient(status: &tonic::Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::Unavailable
            | tonic::Code::DeadlineExceeded
            | tonic::Code::Unknown
            | tonic::Code::Internal
    )
}
fn rpc_error(status: tonic::Status, write: bool) -> SandboxError {
    match status.code() {
        // Resource authorization must not reveal another tenant's instance.
        // This is an error, not an observation of a deleted sandbox.
        tonic::Code::PermissionDenied => SandboxError::NotFound,
        tonic::Code::Unauthenticated => {
            SandboxError::Unavailable("Platform service authentication failed".into())
        }
        tonic::Code::InvalidArgument => SandboxError::Invalid(status.message().into()),
        tonic::Code::AlreadyExists | tonic::Code::FailedPrecondition => {
            SandboxError::Conflict(status.message().into())
        }
        tonic::Code::Unimplemented => SandboxError::Unsupported(status.message().into()),
        _ if write => SandboxError::OutcomeUnknown(
            "Platform write response unavailable; inspect original ID".into(),
        ),
        _ => SandboxError::Unavailable("Platform read unavailable".into()),
    }
}
fn validate_profiles(profiles: &[PreinstalledProfile]) -> Result<()> {
    if profiles.is_empty() {
        return Err(SandboxError::Invalid(
            "preinstalled verification profiles required".into(),
        ));
    }
    let mut identities = BTreeSet::new();
    for profile in profiles {
        let execution = ExecutionSpec {
            image: profile.image.clone(),
            isolation_runtime: profile.isolation_runtime.clone(),
            entrypoint: profile.entrypoint.clone(),
            working_dir: profile.working_dir.clone(),
            user: profile.user.clone(),
            env: Default::default(),
            resources: adx_agent_core::Resources {
                cpu_millis: 1,
                memory_mib: 1,
            },
            service: vec![],
        };
        execution.validate().map_err(SandboxError::Invalid)?;
        if profile.rrt_port == 0
            || profile.entrypoint.is_empty() != profile.config_path.is_none()
            || profile.config_path.as_ref().is_some_and(|p| {
                !p.starts_with('/') || p.contains('\0') || p.split('/').any(|c| c == "..")
            })
        {
            return Err(SandboxError::Invalid(
                "invalid preinstalled RRT profile".into(),
            ));
        }
        let identity = serde_json::to_string(&(
            profile.image.clone(),
            profile.isolation_runtime.clone(),
            profile.entrypoint.clone(),
            profile.working_dir.clone(),
            profile.user.clone(),
        ))
        .expect("profile serialization");
        if !identities.insert(identity) {
            return Err(SandboxError::Invalid(
                "duplicate preinstalled profile".into(),
            ));
        }
    }
    Ok(())
}
fn matching_profile<'a>(
    execution: &ExecutionSpec,
    profiles: &'a [PreinstalledProfile],
) -> Result<&'a PreinstalledProfile> {
    execution.validate().map_err(SandboxError::Invalid)?;
    let profile=profiles.iter().find(|p|p.image==execution.image&&p.isolation_runtime==execution.isolation_runtime&&p.entrypoint==execution.entrypoint&&p.working_dir==execution.working_dir&&p.user==execution.user)
        .ok_or_else(||SandboxError::Unsupported("no matching preinstalled RRT/argv/cwd/user profile; dynamic Platform injection is unavailable".into()))?;
    Ok(profile)
}
fn to_platform(
    request: &CreateSandbox,
    profiles: &[PreinstalledProfile],
    rrt_token: &str,
) -> Result<pb::CapsuleSpec> {
    validate_identity(&request.tenant, &request.id)?;
    let execution = &request.execution;
    let profile = matching_profile(execution, profiles)?;
    let mut env: std::collections::HashMap<_, _> = execution.env.clone().into_iter().collect();
    if let Some(path) = &profile.config_path {
        env.insert("ADX_IMAGE_PROCESS_CONFIG".into(), path.clone());
    }
    env.insert("RRT_HTTP_TOKEN".into(), rrt_token.into());
    env.insert("RRT_HTTP_PORT".into(), profile.rrt_port.to_string());
    env.insert(
        HAS_ENTRYPOINT.into(),
        (!execution.entrypoint.is_empty()).to_string(),
    );
    let ports: Vec<_> = execution
        .service
        .iter()
        .map(|s| s.port)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    env.insert(
        SERVICE_PORTS.into(),
        serde_json::to_string(&ports).expect("ports serialization"),
    );
    let hash = Sha256::digest(serde_json::to_vec(execution).expect("execution serialization"));
    env.insert(EXECUTION_HASH.into(), format!("{hash:x}"));
    Ok(pb::CapsuleSpec {
        id: request.id.clone(),
        tenant_id: request.tenant.clone(),
        image: execution.image.clone(),
        runtime_class: execution.isolation_runtime.clone(),
        resources: Some(pb::Resources {
            cpu_millis: execution.resources.cpu_millis,
            memory_bytes: execution.resources.memory_mib * 1024 * 1024,
            disk_bytes: 0,
        }),
        priority: 0,
        scheduling: None,
        env,
        lifecycle: None,
        snapshot_id: None,
        // Preinstalled profiles use the node deployment without a runtime environment.
        environment: None,
        sandbox: None,
    })
}
#[async_trait]
impl Sandbox for PlatformSandbox {
    fn validate_execution(&self, execution: &ExecutionSpec) -> Result<()> {
        matching_profile(execution, &self.profiles).map(|_| ())
    }
    async fn create(&self, request: &CreateSandbox) -> Result<SandboxObservation> {
        let spec = to_platform(request, &self.profiles, &self.rrt_token)?;
        let mut client = pb::master_service_client::MasterServiceClient::new(self.master().await?);
        match client
            .create_capsule(pb::CreateCapsuleRequest {
                spec: Some(spec),
                caller: Some(caller(&request.tenant)),
                schedule_timeout_seconds: 30,
                create_timeout_seconds: 90,
            })
            .await
        {
            Ok(response) => {
                let response = response.into_inner();
                validate_record(response.record.as_ref(), &request.tenant, &request.id)?;
                // Read the current assignment and proxy address; a failed observation never repeats create.
                self.get(&request.tenant, &request.id)
                    .await
                    .map_err(|_| {
                        SandboxError::OutcomeUnknown(
                            "create accepted but current Sandbox state is unavailable".into(),
                        )
                    })?
                    .ok_or_else(|| {
                        SandboxError::OutcomeUnknown("created Sandbox not yet queryable".into())
                    })
            }
            Err(status) => {
                if transient(&status) {
                    self.invalidate().await;
                }
                Err(rpc_error(status, true))
            }
        }
    }
    async fn get(&self, tenant: &str, id: &str) -> Result<Option<SandboxObservation>> {
        let Some(response) = self.inspect(tenant, id).await? else {
            return Ok(None);
        };
        self.observation(
            response.record.expect("validated record"),
            &response.node_proxy_address,
        )
        .await
        .map(Some)
    }
    async fn delete(&self, tenant: &str, id: &str) -> Result<SandboxObservation> {
        let response=self.inspect(tenant,id).await?.ok_or_else(||SandboxError::OutcomeUnknown("Sandbox is absent; Platform provides no tombstone for a never-observed create, so deletion is not confirmed".into()))?;
        let record = response.record.expect("validated record");
        if record.state == pb::CapsuleState::Deleted as i32 {
            return self.observation(record, &response.node_proxy_address).await;
        }
        let assignment = record
            .assignment
            .ok_or_else(|| SandboxError::Unavailable("Sandbox assignment missing".into()))?;
        let mut client = pb::node_service_client::NodeServiceClient::new(
            // Node registration exposes an authority; only Master discovery
            // returns a URL. Node RPCs still use the same mandatory mTLS.
            self.connect(format!("https://{}", response.node_address))
                .await?,
        );
        let result = client
            .delete_capsule(pb::DeleteCapsuleRequest {
                assignment: Some(assignment),
                caller: Some(caller(tenant)),
            })
            .await
            .map_err(|s| rpc_error(s, true))?
            .into_inner();
        validate_record(result.record.as_ref(), tenant, id)?;
        let record = result.record.expect("validated record");
        // Require a published terminal state: journaled local deletion alone is not globally confirmed.
        if record.state == pb::CapsuleState::Deleted as i32
            && result.durability != pb::Durability::Published as i32
        {
            return Err(SandboxError::OutcomeUnknown(
                "deletion not yet published by Platform".into(),
            ));
        }
        self.observation(record, &response.node_proxy_address).await
    }
}

/// Transport/authentication only; implementation is also callable in-process by Agent APIs.
pub struct SandboxApi {
    pub backend: Arc<dyn Sandbox>,
    auth: ServiceAuth,
}
impl SandboxApi {
    pub fn new(backend: Arc<dyn Sandbox>, service_token: &str) -> Result<Self> {
        Ok(Self {
            backend,
            auth: ServiceAuth::new(service_token).map_err(SandboxError::Invalid)?,
        })
    }
    pub fn matches(path: &str) -> bool {
        path == PATH
            || path
                .strip_prefix(PATH)
                .is_some_and(|tail| tail.starts_with('/'))
    }
    pub fn service_authorized(&self, headers: &http::HeaderMap) -> bool {
        self.auth.accepts(
            headers
                .get(http::header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok()),
        )
    }
    pub async fn handle(
        &self,
        request: http::Request<hyper::body::Incoming>,
        authenticated_tenant: Option<&str>,
    ) -> http::Response<super::server::ProxyBody> {
        let progress = RequestProgress::default();
        let response = match tokio::time::timeout(
            limits::SANDBOX_REQUEST_TIMEOUT,
            self.dispatch(request, authenticated_tenant, &progress),
        )
        .await
        {
            Ok(response) => response,
            Err(_) => Err(if progress.may_have_written() {
                SandboxError::OutcomeUnknown(
                    "Sandbox request deadline exceeded; inspect the original ID".into(),
                )
            } else {
                SandboxError::Unavailable("Sandbox read or request admission timed out".into())
            }),
        };
        match response {
            Ok(Some(observed)) => json_response(http::StatusCode::OK, &observed),
            Ok(None) => json_response(http::StatusCode::NOT_FOUND, &SandboxError::NotFound),
            Err(error) => {
                let status = match &error {
                    SandboxError::NotFound => http::StatusCode::NOT_FOUND,
                    SandboxError::Invalid(_) => http::StatusCode::BAD_REQUEST,
                    SandboxError::Conflict(_) => http::StatusCode::CONFLICT,
                    SandboxError::Unsupported(_) => http::StatusCode::NOT_IMPLEMENTED,
                    SandboxError::Unavailable(_) | SandboxError::OutcomeUnknown(_) => {
                        http::StatusCode::SERVICE_UNAVAILABLE
                    }
                };
                json_response(status, &error)
            }
        }
    }
    async fn dispatch(
        &self,
        request: http::Request<hyper::body::Incoming>,
        authenticated_tenant: Option<&str>,
        progress: &RequestProgress,
    ) -> Result<Option<SandboxObservation>> {
        let (parts, body) = request.into_parts();
        let pairs: Vec<_> = url::form_urlencoded::parse(parts.uri.query().unwrap_or("").as_bytes())
            .into_owned()
            .collect();
        if pairs.len() != 1 || pairs[0].0 != "tenant" {
            return Err(SandboxError::Invalid(
                "exactly one tenant query parameter required".into(),
            ));
        }
        let tenant = &pairs[0].1;
        if authenticated_tenant.is_some_and(|t| t != tenant) {
            return Err(SandboxError::Conflict(
                "tenant does not match authenticated identity".into(),
            ));
        }
        let suffix = parts
            .uri
            .path()
            .strip_prefix(PATH)
            .ok_or_else(|| SandboxError::Invalid("invalid Sandbox path".into()))?;
        let id = if suffix.is_empty() {
            None
        } else {
            let segment = suffix
                .strip_prefix('/')
                .filter(|s| !s.is_empty() && !s.contains('/'))
                .ok_or_else(|| SandboxError::Invalid("invalid Sandbox ID path".into()))?;
            Some(
                percent_encoding::percent_decode_str(segment)
                    .decode_utf8()
                    .map_err(|_| SandboxError::Invalid("Sandbox ID is not UTF-8".into()))?
                    .into_owned(),
            )
        };
        match (parts.method, id) {
            (http::Method::POST, None) => {
                let bytes = http_body_util::Limited::new(body, limits::HTTP_JSON_BYTES)
                    .collect()
                    .await
                    .map_err(|_| {
                        SandboxError::Invalid("invalid or oversized Sandbox request".into())
                    })?
                    .to_bytes();
                let request: CreateSandbox = serde_json::from_slice(&bytes)
                    .map_err(|_| SandboxError::Invalid("invalid Sandbox create JSON".into()))?;
                if &request.tenant != tenant {
                    return Err(SandboxError::Invalid(
                        "body and query tenant disagree".into(),
                    ));
                }
                progress.start_write();
                self.backend.create(&request).await.map(Some)
            }
            (http::Method::GET, Some(id)) => self.backend.get(tenant, &id).await,
            (http::Method::DELETE, Some(id)) => {
                progress.start_write();
                self.backend.delete(tenant, &id).await.map(Some)
            }
            _ => Err(SandboxError::Invalid(
                "unsupported Sandbox method/path".into(),
            )),
        }
    }
}
fn json_response<T: serde::Serialize>(
    status: http::StatusCode,
    value: &T,
) -> http::Response<super::server::ProxyBody> {
    let body = Full::new(Bytes::from(
        serde_json::to_vec(value).expect("serializable Sandbox response"),
    ))
    .map_err(
        |never: std::convert::Infallible| -> Box<dyn std::error::Error + Send + Sync> {
            match never {}
        },
    )
    .boxed_unsync();
    http::Response::builder()
        .status(status)
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(body)
        .expect("static Sandbox response")
}

#[cfg(test)]
mod tests {
    use super::*;
    use adx_agent_core::{Protocol, Resources, Service};
    #[test]
    fn platform_authorization_errors_preserve_tenant_privacy_and_write_uncertainty() {
        for write in [false, true] {
            assert_eq!(
                rpc_error(
                    tonic::Status::permission_denied("instance belongs to another tenant"),
                    write
                ),
                SandboxError::NotFound
            );
            assert!(matches!(
                rpc_error(
                    tonic::Status::unauthenticated("invalid component credential"),
                    write
                ),
                SandboxError::Unavailable(_)
            ));
            assert!(matches!(
                rpc_error(tonic::Status::failed_precondition("spec mismatch"), write),
                SandboxError::Conflict(_)
            ));
        }
        assert!(matches!(
            rpc_error(tonic::Status::unavailable("lost write response"), true),
            SandboxError::OutcomeUnknown(_)
        ));
    }
    fn profile() -> PreinstalledProfile {
        PreinstalledProfile {
            image: "app@sha256:fixed".into(),
            isolation_runtime: "runc".into(),
            entrypoint: vec!["/app/start".into(), "--port".into(), "8080".into()],
            working_dir: "/app".into(),
            user: None,
            config_path: Some("/etc/adx/start.json".into()),
            rrt_port: 50090,
        }
    }
    fn request() -> CreateSandbox {
        let p = profile();
        CreateSandbox {
            id: "sandbox-id".into(),
            tenant: "tenant".into(),
            execution: ExecutionSpec {
                image: p.image,
                isolation_runtime: p.isolation_runtime,
                entrypoint: p.entrypoint,
                working_dir: p.working_dir,
                user: p.user,
                env: Default::default(),
                resources: Resources {
                    cpu_millis: 500,
                    memory_mib: 256,
                },
                service: vec![
                    Service {
                        protocol: Protocol::Http,
                        port: 8080,
                    },
                    Service {
                        protocol: Protocol::Ws,
                        port: 8080,
                    },
                ],
            },
        }
    }
    #[test]
    fn preinstalled_mapping_keeps_units_argv_identity_and_protocol_ports() {
        let request = request();
        let mapped = to_platform(&request, &[profile()], "test-token").unwrap();
        assert_eq!(mapped.id, request.id);
        assert_eq!(mapped.tenant_id, request.tenant);
        assert_eq!(mapped.runtime_class, "runc");
        assert_eq!(mapped.resources.unwrap().memory_bytes, 256 * 1024 * 1024);
        assert_eq!(
            mapped.env["ADX_IMAGE_PROCESS_CONFIG"],
            "/etc/adx/start.json"
        );
        assert_eq!(mapped.env[SERVICE_PORTS], "[8080]");
        assert_eq!(mapped.env[HAS_ENTRYPOINT], "true");
        let mut other = request.clone();
        other.execution.service.push(Service {
            protocol: Protocol::Ssh,
            port: 22,
        });
        assert_ne!(
            mapped.env[EXECUTION_HASH],
            to_platform(&other, &[profile()], "test-token").unwrap().env[EXECUTION_HASH]
        );
        assert!(mapped.scheduling.is_none());
        assert!(mapped.lifecycle.is_none());
    }
    #[test]
    fn startup_intent_cannot_be_silently_replaced_by_a_preset() {
        for mutate in 0..4 {
            let mut request = request();
            match mutate {
                0 => request.execution.entrypoint.push("extra".into()),
                1 => request.execution.working_dir = "/elsewhere".into(),
                2 => request.execution.user = Some("1000".into()),
                _ => request.execution.isolation_runtime = "runsc".into(),
            }
            assert!(matches!(
                to_platform(&request, &[profile()], "test-token"),
                Err(SandboxError::Unsupported(_))
            ));
        }
        let mut forged = request();
        forged
            .execution
            .env
            .insert(EXECUTION_HASH.into(), "forged".into());
        assert!(matches!(
            to_platform(&forged, &[profile()], "test-token"),
            Err(SandboxError::Invalid(_))
        ));
        let mut p = profile();
        p.entrypoint.clear();
        p.config_path = None;
        validate_profiles(&[p.clone()]).unwrap();
        p.config_path = Some("/etc/implicit-entrypoint.json".into());
        assert!(validate_profiles(&[p]).is_err());
    }
    struct ProfilesOnly(Vec<PreinstalledProfile>);
    #[async_trait]
    impl Sandbox for ProfilesOnly {
        fn validate_execution(&self, execution: &ExecutionSpec) -> Result<()> {
            matching_profile(execution, &self.0).map(|_| ())
        }
        async fn create(&self, _: &CreateSandbox) -> Result<SandboxObservation> {
            panic!("validation must not create")
        }
        async fn get(&self, _: &str, _: &str) -> Result<Option<SandboxObservation>> {
            panic!("validation must not query")
        }
        async fn delete(&self, _: &str, _: &str) -> Result<SandboxObservation> {
            panic!("validation must not delete")
        }
    }
    fn inline_options() -> adx_agent_api::management::Options {
        let p = profile();
        adx_agent_api::management::Options {
            profiles: vec![adx_agent_api::management::InlineProfile {
                sandbox_type: adx_agent_core::inline::SandboxType::Docker,
                request_image: Some(p.image.clone()),
                image: p.image,
                isolation_runtime: p.isolation_runtime,
                request_user: p.user,
                working_dir: p.working_dir,
                default_entrypoint: p.entrypoint,
                service: vec![],
                preinstalled_workspace: None,
                preinstalled_mounts: vec![],
            }],
            backend_timeout: Duration::from_secs(2),
            max_inflight: 4,
        }
    }
    #[tokio::test]
    async fn inline_profiles_fail_at_startup_and_custom_argv_fail_before_admission() {
        use adx_agent_api::management::InlineService;
        for field in 0..5 {
            let mut options = inline_options();
            let p = &mut options.profiles[0];
            match field {
                0 => {
                    p.image = "other:1".into();
                    p.request_image = Some(p.image.clone());
                }
                1 => p.isolation_runtime = "runsc".into(),
                2 => p.default_entrypoint = vec!["/different".into()],
                3 => p.working_dir = "/different".into(),
                _ => p.request_user = Some("1000".into()),
            }
            assert!(InlineService::new(Arc::new(ProfilesOnly(vec![profile()])), options).is_err());
        }
        let service =
            InlineService::new(Arc::new(ProfilesOnly(vec![profile()])), inline_options()).unwrap();
        let request: adx_agent_core::inline::CreateRequest=serde_json::from_value(serde_json::json!({
            "name":"profile","namespace":"default","runtime_spec":{"runtime":"Python3.11","sandbox_type":"docker",
            "rootfs":{"imageurl":profile().image},"cmds":[["/unsupported"]]}
        })).unwrap();
        assert!(matches!(
            service.create("tenant", request.clone()).await,
            Err(adx_agent_api::Error::Unsupported(_))
        ));
    }
    #[test]
    fn response_identity_and_generation_are_checked() {
        let mut record = pb::CapsuleRecord {
            spec: Some(to_platform(&request(), &[profile()], "token").unwrap()),
            assignment: Some(pb::Assignment {
                capsule_id: "sandbox-id".into(),
                node_id: "node".into(),
                shard_id: 0,
                generation: 1,
                devices: vec![],
            }),
            runtime_id: "sandbox-id-1".into(),
            runtime_ip: "10.1.1.2".into(),
            ..Default::default()
        };
        validate_record(Some(&record), "tenant", "sandbox-id").unwrap();
        assert!(validate_record(Some(&record), "another-tenant", "sandbox-id").is_err());
        assert_eq!(
            connect_target(&record, 8080).unwrap().workload_id,
            "sandbox-id-1"
        );
        record.runtime_id = "sandbox-id-2".into();
        assert!(connect_target(&record, 8080).is_err());
    }
    struct Fake;
    #[async_trait]
    impl Sandbox for Fake {
        async fn create(&self, r: &CreateSandbox) -> Result<SandboxObservation> {
            Ok(SandboxObservation {
                id: r.id.clone(),
                tenant: r.tenant.clone(),
                phase: SandboxPhase::Creating,
                ready: false,
                runtime_id: None,
                message: None,
            })
        }
        async fn get(&self, _: &str, _: &str) -> Result<Option<SandboxObservation>> {
            Ok(None)
        }
        async fn delete(&self, _: &str, _: &str) -> Result<SandboxObservation> {
            Err(SandboxError::OutcomeUnknown("not confirmed".into()))
        }
    }
    #[tokio::test]
    async fn sandbox_http_keeps_tenant_checks_and_unknown_delete_semantics() {
        let token = "sandbox-service-test-token-32-bytes";
        let api = Arc::new(SandboxApi::new(Arc::new(Fake), token).unwrap());
        let mut headers = http::HeaderMap::new();
        headers.insert("authorization", format!("Bearer {token}").parse().unwrap());
        assert!(api.service_authorized(&headers));
        headers.insert("authorization", "Bearer forged".parse().unwrap());
        assert!(!api.service_authorized(&headers));
        let (client, server) = tokio::io::duplex(64 * 1024);
        let serving = tokio::spawn(async move {
            hyper::server::conn::http1::Builder::new()
                .serve_connection(
                    TokioIo::new(server),
                    hyper::service::service_fn(move |r| {
                        let api = api.clone();
                        async move {
                            Ok::<_, std::convert::Infallible>(api.handle(r, Some("tenant")).await)
                        }
                    }),
                )
                .await
                .unwrap()
        });
        let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(client))
            .await
            .unwrap();
        let conn = tokio::spawn(connection);
        let response = sender
            .send_request(
                http::Request::builder()
                    .method("POST")
                    .uri(format!("{PATH}?tenant=another"))
                    .body(Full::new(Bytes::from(
                        serde_json::to_vec(&request()).unwrap(),
                    )))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), http::StatusCode::CONFLICT);
        let _ = response.collect().await;
        let response = sender
            .send_request(
                http::Request::builder()
                    .method("POST")
                    .uri(format!("{PATH}?tenant=tenant"))
                    .body(Full::new(Bytes::from(
                        serde_json::to_vec(&request()).unwrap(),
                    )))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), http::StatusCode::OK);
        let body = response.collect().await.unwrap().to_bytes();
        assert_eq!(
            serde_json::from_slice::<SandboxObservation>(&body)
                .unwrap()
                .phase,
            SandboxPhase::Creating
        );
        let response = sender
            .send_request(
                http::Request::builder()
                    .method("DELETE")
                    .uri(format!("{PATH}/sandbox-id?tenant=tenant"))
                    .body(Full::new(Bytes::new()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), http::StatusCode::SERVICE_UNAVAILABLE);
        let body = response.collect().await.unwrap().to_bytes();
        assert!(matches!(
            serde_json::from_slice::<SandboxError>(&body).unwrap(),
            SandboxError::OutcomeUnknown(_)
        ));
        conn.abort();
        serving.abort();
    }
    struct Stalled;
    #[async_trait]
    impl Sandbox for Stalled {
        async fn create(&self, _: &CreateSandbox) -> Result<SandboxObservation> {
            std::future::pending().await
        }
        async fn get(&self, _: &str, _: &str) -> Result<Option<SandboxObservation>> {
            std::future::pending().await
        }
        async fn delete(&self, _: &str, _: &str) -> Result<SandboxObservation> {
            std::future::pending().await
        }
    }
    #[tokio::test(start_paused = true)]
    async fn sandbox_deadline_distinguishes_reads_admission_and_submitted_writes() {
        for (method, pending_body, expected_unknown) in [
            ("GET", false, false),
            ("POST", true, false),
            ("POST", false, true),
            ("DELETE", false, true),
        ] {
            let api = Arc::new(
                SandboxApi::new(Arc::new(Stalled), "service-test-token-at-least-32-bytes").unwrap(),
            );
            let (client, server) = tokio::io::duplex(64 * 1024);
            let serving = tokio::spawn(async move {
                hyper::server::conn::http1::Builder::new()
                    .serve_connection(
                        TokioIo::new(server),
                        hyper::service::service_fn(move |r| {
                            let api = api.clone();
                            async move {
                                Ok::<_, std::convert::Infallible>(
                                    api.handle(r, Some("tenant")).await,
                                )
                            }
                        }),
                    )
                    .await
            });
            let (mut sender, connection) =
                hyper::client::conn::http1::handshake(TokioIo::new(client))
                    .await
                    .unwrap();
            let conn = tokio::spawn(connection);
            let body = if pending_body {
                http_body_util::StreamBody::new(futures_util::stream::pending::<
                    std::result::Result<hyper::body::Frame<Bytes>, std::convert::Infallible>,
                >())
                .boxed()
            } else {
                Full::new(Bytes::from(serde_json::to_vec(&request()).unwrap())).boxed()
            };
            let path = if method == "POST" {
                PATH.to_owned()
            } else {
                format!("{PATH}/id")
            };
            let response = sender
                .send_request(
                    http::Request::builder()
                        .method(method)
                        .uri(format!("{path}?tenant=tenant"))
                        .body(body)
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), http::StatusCode::SERVICE_UNAVAILABLE);
            let error: SandboxError =
                serde_json::from_slice(&response.collect().await.unwrap().to_bytes()).unwrap();
            if expected_unknown {
                assert!(matches!(error, SandboxError::OutcomeUnknown(_)));
            } else {
                assert!(matches!(error, SandboxError::Unavailable(_)));
            }
            conn.abort();
            serving.abort();
        }
    }
}
