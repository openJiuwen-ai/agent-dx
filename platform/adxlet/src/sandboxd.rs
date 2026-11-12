//! sandboxd PR #56 adapter. The RPC task outlives a canceled caller so cleanup
//! cannot race its client task. Failed starts are terminal; ambiguous outcomes
//! retain targeted cleanup retries until their backend can be removed.
use crate::RuntimeDriver;
use adx_core::{EnvironmentSpec, Error, Result, UnsupportedCapability};
use async_trait::async_trait;
use hyper_util::rt::TokioIo;
use std::collections::{BTreeMap, HashMap};
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::UnixStream;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Request};
use tower::service_fn;

mod recovery;

pub mod proto {
    tonic::include_proto!("runtime.v1");
}
use proto::sandbox_service_client::SandboxServiceClient;

#[derive(Clone)]
pub struct Config {
    pub runtime_profile: Option<adx_core::runtime_profile::RuntimeProfile>,
    /// Exact backend executable paths configured in sandboxd; never inferred from PATH.
    pub checkpoint_runtime_binaries: BTreeMap<String, PathBuf>,
    pub local_rootfs_templates: BTreeMap<String, adx_core::local_rootfs::LocalRootfsTemplate>,
    pub command: Vec<String>,
    pub env: HashMap<String, String>,
    pub cwd: String,
    pub rpc_timeout: Duration,
    pub runtime_logs: Option<crate::runtime_logs::RuntimeLogPolicy>,
    pub network_baseline: Option<crate::network_baseline::NetworkBaseline>,
    /// Resolve traffic peers on the destination node, independently of policy ceilings.
    pub network_peer_aliases: BTreeMap<String, ipnet::Ipv4Net>,
    /// None keeps legacy exceptions; configured CIDRs constrain host-originated control traffic.
    pub platform_ingress_sources: Option<Vec<ipnet::Ipv4Net>>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            runtime_profile: None,
            checkpoint_runtime_binaries: BTreeMap::new(),
            local_rootfs_templates: BTreeMap::new(),
            command: vec!["/usr/local/bin/adx-execd".into()],
            env: HashMap::from([("EXECD_HTTP_PORT".into(), "50090".into())]),
            cwd: "/".into(),
            rpc_timeout: Duration::from_secs(30),
            runtime_logs: None,
            network_baseline: None,
            network_peer_aliases: BTreeMap::new(),
            platform_ingress_sources: None,
        }
    }
}

enum StartState {
    Idle,
    Started(IpAddr),
    Settled,
    Failed,
    UncertainStart,
    // Checkpoint may still be executing after its transport failed.
    Uncertain,
}
type Cell = Arc<tokio::sync::Mutex<StartState>>;

#[derive(Clone)]
pub struct Sandboxd {
    client: SandboxServiceClient<Channel>,
    config: Config,
    rootfs_identity: Option<crate::RuntimeRootfsIdentity>,
    recovery_profiles: BTreeMap<String, BTreeMap<String, String>>,
    starts: Arc<Mutex<BTreeMap<String, Cell>>>,
    backend_ids: Arc<Mutex<BTreeMap<String, String>>>,
}

fn rootfs_identity(
    profile: Option<&adx_core::runtime_profile::RuntimeProfile>,
) -> Result<Option<crate::RuntimeRootfsIdentity>> {
    use sha2::{Digest, Sha256};
    use std::io::Read;
    let Some(profile) = profile.filter(|profile| profile.rootfs.r#type == "local") else {
        return Ok(None);
    };
    let mut file = std::fs::File::open(&profile.rootfs.path).map_err(unavailable)?;
    if !file.metadata().map_err(unavailable)?.is_file() {
        return Err(Error::Invalid(
            "runtime rootfs must be a regular file".into(),
        ));
    }
    let mut digest = Sha256::new();
    let mut buffer = [0u8; 65536];
    loop {
        let count = file.read(&mut buffer).map_err(unavailable)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    Ok(Some(crate::RuntimeRootfsIdentity {
        path: profile.rootfs.path.clone(),
        sha256: format!("{:x}", digest.finalize()),
    }))
}

fn unavailable(error: impl std::fmt::Display) -> Error {
    Error::Unavailable(format!("sandboxd: {error}"))
}

/// Wait for an externally managed sandboxd without publishing this node for admission.
/// Invalid local configuration still fails immediately; transport and readiness failures
/// are retried until the caller cancels this future during process shutdown.
pub async fn connect_when_ready(
    path: PathBuf,
    config: Config,
    readiness_timeout: Duration,
    retry_interval: Duration,
) -> Result<Sandboxd> {
    if readiness_timeout.is_zero() || retry_interval.is_zero() {
        return Err(Error::Invalid(
            "sandboxd readiness and retry intervals must be positive".into(),
        ));
    }
    loop {
        let unavailable = match Sandboxd::connect(path.clone(), config.clone()).await {
            Ok(runtime) => match runtime.wait_ready_with_timeout(readiness_timeout).await {
                Ok(()) => return Ok(runtime),
                Err(error @ Error::Unavailable(_)) => error,
                Err(error) => return Err(error),
            },
            Err(error @ Error::Unavailable(_)) => error,
            Err(error) => return Err(error),
        };
        adx_observability::warn!(error=%unavailable, "sandboxd unavailable; node admission remains closed");
        tokio::time::sleep(retry_interval).await;
    }
}

impl Sandboxd {
    /// Measured, immutable node-session evidence for declared recovery backends.
    pub fn recovery_labels(&self) -> BTreeMap<String, String> {
        self.recovery_profiles
            .values()
            .flat_map(|profile| profile.clone())
            .collect()
    }

    /// Discover live execution backends through sandboxd's Unix socket.
    pub async fn available_runtime_classes(&self) -> Result<Vec<String>> {
        let response = self
            .client
            .clone()
            .list_available_runtimes(self.request(proto::ListAvailableRuntimesRequest {}))
            .await
            .map_err(unavailable)?
            .into_inner();
        let mut classes = response.runtime_classes;
        classes.extend(
            response
                .runtimes
                .into_iter()
                .map(|runtime| runtime.runtime_class),
        );
        classes.retain(|class| !class.trim().is_empty());
        classes.sort();
        classes.dedup();
        Ok(classes)
    }

    pub async fn connect(path: PathBuf, config: Config) -> Result<Self> {
        if (config.command.is_empty() && config.runtime_profile.is_none())
            || config.rpc_timeout.is_zero()
        {
            return Err(Error::Invalid(
                "sandboxd command and positive RPC timeout are required".into(),
            ));
        }
        for (name, template) in &config.local_rootfs_templates {
            adx_core::local_rootfs::capability_label(name)?;
            template.validate()?;
            std::fs::metadata(&template.path).map_err(|error| {
                Error::Invalid(format!("local rootfs template {name}: {error}"))
            })?;
        }
        if let Some(e) = &config.runtime_profile {
            e.validate()?;
            let paths = if e.rootfs.r#type == "local" {
                vec![&e.rootfs.path, &e.bootstrap.root]
            } else {
                vec![]
            };
            for path in paths {
                let metadata = std::fs::metadata(path)
                    .map_err(|error| Error::Invalid(format!("runtime artifact {path}: {error}")))?;
                if !metadata.is_file() {
                    return Err(Error::Invalid(
                        "runtime artifact must be a regular EROFS file".into(),
                    ));
                }
                use std::io::{Read, Seek, SeekFrom};
                let mut f = std::fs::File::open(path).map_err(unavailable)?;
                let mut magic = [0; 4];
                f.seek(SeekFrom::Start(1024)).map_err(unavailable)?;
                f.read_exact(&mut magic).map_err(unavailable)?;
                if magic != [0xe2, 0xe1, 0xf5, 0xe0] {
                    return Err(Error::Invalid("runtime artifact is not EROFS".into()));
                }
            }
        }
        if let Some(policy) = &config.runtime_logs {
            policy
                .validate()
                .map_err(|error| Error::Invalid(error.to_string()))?;
            crate::runtime_logs::RuntimeLogs::new(policy.clone()).map_err(unavailable)?;
        }
        // Deployment rootfs files must remain immutable for the process lifetime.
        // Hash bytes once off the async executor; a changed image requires a new session.
        let profile = config.runtime_profile.clone();
        let binaries = config.checkpoint_runtime_binaries.clone();
        let (rootfs_identity, recovery_profiles) = tokio::task::spawn_blocking(move || {
            let identity = rootfs_identity(profile.as_ref())?;
            let default_root = profile.as_ref().is_some_and(|profile| {
                profile.bootstrap.r#type == "erofs" && profile.bootstrap.root == profile.rootfs.path
            });
            let profiles =
                recovery::profiles(identity.as_ref().filter(|_| default_root), &binaries)?;
            Ok::<_, Error>((identity, profiles))
        })
        .await
        .map_err(unavailable)??;
        let channel = Endpoint::from_static("http://localhost")
            .connect_timeout(config.rpc_timeout)
            .connect_with_connector(service_fn(move |_| {
                let path = path.clone();
                async move { UnixStream::connect(path).await.map(TokioIo::new) }
            }))
            .await
            .map_err(unavailable)?;
        Ok(Self {
            client: SandboxServiceClient::new(channel),
            config,
            rootfs_identity,
            recovery_profiles,
            starts: Arc::default(),
            backend_ids: Arc::default(),
        })
    }

    /// Do not publish node admission before sandboxd's own housekeeping and
    /// resource/image recovery report readiness. A bound UDS alone is insufficient.
    pub async fn wait_ready(&self) -> Result<()> {
        self.wait_ready_with_timeout(self.config.rpc_timeout).await
    }

    pub async fn wait_ready_with_timeout(&self, timeout: Duration) -> Result<()> {
        tokio::time::timeout(timeout, async {
            loop {
                match self
                    .client
                    .clone()
                    .list_available_runtimes(self.request(proto::ListAvailableRuntimesRequest {}))
                    .await
                {
                    Ok(_) => return Ok(()),
                    Err(e) if e.code() == Code::Unimplemented => return Ok(()),
                    Err(e) if e.code() == Code::Unavailable => {
                        tokio::time::sleep(Duration::from_millis(200)).await
                    }
                    Err(e) => return Err(unavailable(e)),
                }
            }
        })
        .await
        .map_err(|_| unavailable("backend readiness timed out"))?
    }
    async fn capabilities(&self, runtime: &str) -> Result<Option<proto::RuntimeInfo>> {
        match self
            .client
            .clone()
            .list_available_runtimes(self.request(proto::ListAvailableRuntimesRequest {}))
            .await
        {
            Ok(r) => Ok(r
                .into_inner()
                .runtimes
                .into_iter()
                .find(|r| r.runtime_class == runtime)),
            Err(e) if e.code() == Code::Unimplemented => Ok(None),
            Err(e) => Err(unavailable(e)),
        }
    }
    async fn execution_request(
        &self,
        spec: &EnvironmentSpec,
        runtime_id: &str,
        generation: u64,
        devices: &[adx_core::scheduling::DeviceAllocation],
    ) -> Result<Request<proto::StartRequest>> {
        if spec.execution.user.is_some() || spec.execution.group.is_some() {
            adx_observability::warn!(
                environment_id = %spec.id,
                runtime_class = %spec.runtime_class,
                "sandboxd ignores tool user/group overrides and uses its configured identity"
            );
        }
        let mut config = self.config.clone();
        // Runtime capability metadata owns the guest paths. Never inherit caller
        // values that could redirect the privileged checkpoint handoff.
        config
            .env
            .insert("ADX_CHECKPOINT_HANDOFF_FILE".into(), String::new());
        config.env.insert("ADX_ENV_FILE".into(), String::new());
        if let Some(info) = self.capabilities(&spec.runtime_class).await? {
            if info.supports_checkpoint_restore {
                config.env.insert(
                    "ADX_CHECKPOINT_HANDOFF_FILE".into(),
                    info.checkpoint_handoff_path,
                );
                config
                    .env
                    .insert("ADX_ENV_FILE".into(), info.restore_env_path);
            }
        }
        let request = start_request(spec, runtime_id, generation, devices, &config)?;
        crate::managed_mounts::prepare(spec).await?;
        Ok(self.request(request))
    }

    async fn restore_request(
        &self,
        spec: &EnvironmentSpec,
        runtime_id: &str,
        generation: u64,
        devices: &[adx_core::scheduling::DeviceAllocation],
        source: crate::RestoreSource<'_>,
    ) -> Result<Request<proto::StartRequest>> {
        let path = source.path;
        let origin = source.origin;
        self.checkpoint_supported(&spec.runtime_class).await?;
        if !path.is_absolute() {
            return Err(Error::Invalid("absolute checkpoint path required".into()));
        }
        let mut request = self
            .execution_request(spec, runtime_id, generation, devices)
            .await?;
        if let Some(origin) = origin {
            let context = adx_core::runtime::RuntimeRestore {
                target: adx_core::runtime::RuntimeIdentity {
                    environment_id: spec.id.clone(),
                    runtime_id: runtime_id.into(),
                    ownership_generation: generation,
                },
                origin: Some(origin.clone()),
            };
            context.validate(origin)?;
            request.get_mut().envs.insert(
                "ADX_RESTORE_ORIGIN".into(),
                serde_json::to_string(origin).map_err(unavailable)?,
            );
        }
        request.get_mut().checkpoint_info = Some(proto::CheckpointInfo {
            checkpoint_dir: path
                .to_str()
                .ok_or_else(|| Error::Invalid("checkpoint path is not UTF-8".into()))?
                .into(),
        });
        Ok(request)
    }
    async fn execute_start(
        &self,
        runtime_id: &str,
        mut request: Request<proto::StartRequest>,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<IpAddr> {
        let mut state = self.cell(runtime_id).lock_owned().await;
        if let StartState::Started(ip) = *state {
            return Ok(ip);
        }
        if matches!(*state, StartState::Failed) {
            return Err(Error::Conflict);
        }
        if matches!(*state, StartState::Uncertain | StartState::UncertainStart) {
            return Err(unavailable("start outcome requires reconciliation"));
        }
        if let Some(deadline) = deadline {
            request.set_timeout(
                adx_transport::deadline::remaining(deadline, Duration::MAX).ok_or_else(|| {
                    Error::DeadlineExceeded(
                        "backend preparation/runtime start deadline exceeded".into(),
                    )
                })?,
            );
        }
        *state = StartState::Failed;
        let mut client = self.client.clone();
        let backend_ids = self.backend_ids.clone();
        let logical_id = runtime_id.to_string();
        // The spawned task starts with an empty task-local context; keep the
        // sandbox.create trace on the identity-mapping log emitted below.
        tokio::spawn(adx_observability::trace::with_current_context(async move {
            let response = match client.start(request).await {
                Ok(response) => response.into_inner(),
                Err(error) => {
                    if error.code() == Code::DeadlineExceeded
                        && error
                            .metadata()
                            .get("sandboxd-start-failure")
                            .is_some_and(|value| value == "true")
                    {
                        return Err(Error::DeadlineExceeded(format!(
                            "sandboxd: {}",
                            error.message()
                        )));
                    }
                    if !matches!(
                        error.code(),
                        Code::InvalidArgument
                            | Code::Unimplemented
                            | Code::PermissionDenied
                            | Code::Unauthenticated
                    ) && error
                        .metadata()
                        .get("sandboxd-start-settled")
                        .is_none_or(|value| value != "true")
                    {
                        *state = StartState::UncertainStart;
                    }
                    if error.code() == Code::DeadlineExceeded
                        && deadline.is_some_and(|deadline| deadline <= tokio::time::Instant::now())
                    {
                        return Err(Error::DeadlineExceeded(format!(
                            "sandboxd Start deadline exceeded: {error}"
                        )));
                    }
                    return Err(adx_protocol::backend_error(error));
                }
            };
            if !response.id.is_empty() {
                adx_observability::info!(
                    event = "environment_identity_mapped",
                    runtime_id = %logical_id,
                    backend_id = %response.id,
                    "sandboxd physical identity mapped to ADX runtime"
                );
                backend_ids
                    .lock()
                    .expect("shared state lock poisoned")
                    .insert(logical_id, response.id.clone());
            }
            if response.code != 0 {
                return Err(unavailable(response.message));
            }
            if response.id.is_empty() {
                return Err(unavailable("Start returned no backend identity"));
            }
            let ip: IpAddr = response
                .sandbox_ip
                .parse()
                .map_err(|_| unavailable("Start did not return a valid environment IP"))?;
            if ip.is_unspecified() || ip.is_loopback() || ip.is_multicast() {
                return Err(unavailable("Start returned a non-routable environment IP"));
            }
            *state = StartState::Started(ip);
            Ok(ip)
        }))
        .await
        .map_err(unavailable)?
    }

    fn cell(&self, id: &str) -> Cell {
        self.starts
            .lock()
            .expect("shared state lock poisoned")
            .entry(id.into())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(StartState::Idle)))
            .clone()
    }

    fn request<T>(&self, message: T) -> Request<T> {
        let mut request = Request::new(message);
        request.set_timeout(self.config.rpc_timeout);
        request
    }

    // Platform execution IDs and backend IDs are independent. Labels rebuild
    // the in-memory mapping after restart; no backend naming convention is assumed.
    async fn list_id(&self, logical_id: &str) -> Result<Vec<proto::SandboxStatus>> {
        let physical = self
            .backend_ids
            .lock()
            .expect("shared state lock poisoned")
            .get(logical_id)
            .cloned();
        let selector = if !logical_id.is_empty() && physical.is_none() {
            HashMap::from([("adx.runtime_id".into(), logical_id.into())])
        } else {
            HashMap::new()
        };
        let response = self
            .client
            .clone()
            .list(self.request(proto::ListSandboxesRequest {
                id: physical.clone().unwrap_or_default(),
                selector,
            }))
            .await;
        let mut environments = match response {
            Ok(response) => response.into_inner().sandboxes,
            Err(error) if physical.is_some() && error.code() == Code::NotFound => {
                return Ok(Vec::new());
            }
            Err(error) => return Err(unavailable(error)),
        };
        let mut discovered = BTreeMap::new();
        for backend in &mut environments {
            if !backend.labels.contains_key("adx.environment_id") {
                continue;
            }
            let environment = &backend.labels["adx.environment_id"];
            let generation = backend
                .labels
                .get("adx.generation")
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
                .ok_or_else(|| unavailable("managed runtime has no valid generation"))?;
            let logical = backend
                .labels
                .get("adx.runtime_id")
                .filter(|id| adx_core::valid_runtime_id(environment, generation, id))
                .ok_or_else(|| unavailable("managed runtime has invalid execution labels"))?;
            if backend.id.is_empty()
                || environment.is_empty()
                || (!logical_id.is_empty() && logical != logical_id)
                || discovered
                    .insert(logical.clone(), backend.id.clone())
                    .is_some()
            {
                return Err(unavailable("ambiguous managed runtime identity"));
            }
            backend.id = logical.clone();
        }
        let mut ids = self.backend_ids.lock().expect("shared state lock poisoned");
        for (logical, physical) in &discovered {
            if ids
                .get(logical)
                .is_some_and(|previous| previous != physical)
            {
                return Err(unavailable("backend identity changed during execution"));
            }
        }
        ids.extend(discovered);
        Ok(environments)
    }
    async fn physical_id(&self, logical_id: &str) -> Result<Option<String>> {
        if let Some(id) = self
            .backend_ids
            .lock()
            .expect("shared state lock poisoned")
            .get(logical_id)
            .cloned()
        {
            return Ok(Some(id));
        }
        self.list_id(logical_id).await?;
        Ok(self
            .backend_ids
            .lock()
            .expect("shared state lock poisoned")
            .get(logical_id)
            .cloned())
    }

    pub async fn has_managed_environments(&self) -> Result<bool> {
        Ok(self
            .list_id("")
            .await?
            .iter()
            .any(|s| s.labels.contains_key("adx.environment_id")))
    }

    pub async fn is_running(&self, id: &str) -> Result<bool> {
        Ok(self
            .list_id(id)
            .await?
            .iter()
            .any(|x| x.id == id && x.state == proto::SandboxState::Running as i32))
    }

    pub async fn observe(&self, id: &str) -> Result<crate::RuntimeStatus> {
        use adx_core::{RuntimeFailure, RuntimeFailureReason};
        let environments = self.list_id(id).await?;
        let Some(status) = environments.iter().find(|status| status.id == id) else {
            return Ok(crate::RuntimeStatus {
                running: false,
                failure: Some(RuntimeFailure {
                    probe_failure: None,
                    reason: RuntimeFailureReason::Missing,
                    exit_code: None,
                    message: "backend execution is absent".into(),
                }),
            });
        };
        match proto::SandboxState::try_from(status.state) {
            Ok(proto::SandboxState::Running) => Ok(crate::RuntimeStatus {
                running: true,
                failure: None,
            }),
            Ok(proto::SandboxState::Exited) => {
                let physical = self.physical_id(id).await?.ok_or(Error::NotFound)?;
                // Wait is called only for an already-exited execution. Preserve
                // its terminal reason before controller cleanup deletes it.
                let result = self
                    .client
                    .clone()
                    .wait(self.request(proto::WaitRequest { id: physical }))
                    .await
                    .map_err(unavailable)?
                    .into_inner();
                Ok(crate::RuntimeStatus {
                    running: false,
                    failure: Some(runtime_failure(result)),
                })
            }
            _ => Err(unavailable("backend execution state is unknown")),
        }
    }

    pub async fn check_health(&self) -> Result<()> {
        self.client
            .clone()
            .list_available_runtimes(self.request(proto::ListAvailableRuntimesRequest {}))
            .await
            .map_err(unavailable)?;
        Ok(())
    }

    pub async fn stats(&self, id: &str) -> Result<proto::StatsResponse> {
        Ok(self
            .client
            .clone()
            .stats(self.request(proto::StatsRequest {
                id: self.physical_id(id).await?.ok_or(Error::NotFound)?,
            }))
            .await
            .map_err(unavailable)?
            .into_inner())
    }
}

pub fn start_request(
    spec: &EnvironmentSpec,
    runtime_id: &str,
    ownership_generation: u64,
    devices: &[adx_core::scheduling::DeviceAllocation],
    config: &Config,
) -> Result<proto::StartRequest> {
    build_start_request(
        spec,
        runtime_id,
        ownership_generation,
        devices,
        config,
        true,
    )
}

pub(crate) fn build_start_request(
    spec: &EnvironmentSpec,
    runtime_id: &str,
    ownership_generation: u64,
    devices: &[adx_core::scheduling::DeviceAllocation],
    config: &Config,
    omit_profile_path_for_custom_rootfs: bool,
) -> Result<proto::StartRequest> {
    spec.validate()?;
    adx_core::scheduling::validate_device_assignment(&spec.scheduling.devices, devices)?;
    if runtime_id.is_empty()
        || ownership_generation == 0
        || (config.command.is_empty() && config.runtime_profile.is_none())
    {
        return Err(Error::Invalid(
            "runtime identity and command are required".into(),
        ));
    }
    // sandboxd's resource map is f64: reject lossy integer conversions.
    if spec.resources.cpu_millis > 1 << 53 || spec.resources.memory_bytes > 1 << 53 {
        return Err(Error::Invalid(
            "resource value exceeds sandboxd numeric precision".into(),
        ));
    }
    if !deployment_environment_accepts(
        spec.runtime_profile.as_ref(),
        config.runtime_profile.as_ref(),
    ) {
        return Err(Error::Invalid(
            "environment runtime environment differs from this node deployment".into(),
        ));
    }
    validate_protected_ingress(spec)?;
    let environment = spec.runtime_profile.as_ref();
    let options = &spec.execution;
    let custom_rootfs = options.rootfs.is_some() || !spec.image.is_empty();
    let mut envs: HashMap<String, String> = spec.env.clone().into_iter().collect();
    if let Some(e) = environment {
        for (key, value) in &e.env {
            if key == "PATH" && custom_rootfs && omit_profile_path_for_custom_rootfs {
                continue;
            }
            envs.insert(key.clone(), value.clone());
        }
    }
    if let Some(adx_core::execution::Rootfs {
        source: adx_core::execution::StorageSource::Local(path),
        ..
    }) = &spec.execution.rootfs
    {
        let template = config
            .local_rootfs_templates
            .values()
            .find(|value| &value.path == path);
        if let Some(template) = template {
            template.validate()?;
            for (key, value) in &template.env {
                envs.insert(key.clone(), spec.env.get(key).unwrap_or(value).clone());
            }
        }
    }
    // Deployment configuration owns control ports, tokens, and execution identity.
    envs.extend(config.env.clone());
    // User specs cannot enable/redirect the control stream or supply credentials.
    envs.remove("ADX_RUNTIME_CONTROL_ADDRESS");
    envs.remove("ADX_RUNTIME_CONTROL_TOKEN");
    envs.remove("ADX_RUNTIME_CONTROL_SECRET");
    // Privileged bootstrap settings come only from the trusted node profile;
    // the optional common service config owns enabling and instance credentials.
    const ENVD_CONFIGURATION_KEYS: [&str; 6] = [
        "ADX_ENVD_PATH",
        "ADX_ENVD_SHA256",
        "ADX_ENVD_ARGS",
        "ADX_ENVD_DEFAULT_USER",
        "ADX_ENVD_DEFAULT_WORKDIR",
        "ADX_ENVD_FREEZE_ON_CHECKPOINT",
    ];
    for key in ENVD_CONFIGURATION_KEYS.into_iter().chain([
        "ADX_ENVD_ACCESS_TOKEN",
        "ADX_ENVD_GATEWAY_AUTH",
        "ADX_ENVD_VERSION",
    ]) {
        envs.remove(key);
    }
    if let Some(service) = &spec.envd {
        service.validate()?;
        let profile = config.runtime_profile.as_ref().ok_or_else(|| {
            Error::Invalid("envd service requires a trusted runtime profile".into())
        })?;
        for key in ENVD_CONFIGURATION_KEYS {
            if let Some(value) = profile.env.get(key) {
                envs.insert(key.into(), value.clone());
            }
        }
        if envs.get("ADX_ENVD_PATH").is_none_or(|path| path.is_empty()) {
            return Err(Error::Invalid(
                "envd service requires a trusted artifact path".into(),
            ));
        }
        envs.insert("ADX_ENVD_VERSION".into(), service.version.clone());
        if service.gateway_auth {
            envs.insert("ADX_ENVD_GATEWAY_AUTH".into(), "true".into());
        } else if let Some(token) = &service.access_token {
            envs.insert("ADX_ENVD_ACCESS_TOKEN".into(), token.clone());
        }
    }
    if let (Some(address), Some(secret)) = (
        config.env.get("ADX_RUNTIME_CONTROL_ADDRESS"),
        config.env.get("ADX_RUNTIME_CONTROL_SECRET"),
    ) {
        let identity = adx_core::runtime::RuntimeIdentity {
            environment_id: spec.id.clone(),
            runtime_id: runtime_id.into(),
            ownership_generation,
        };
        envs.insert("ADX_RUNTIME_CONTROL_ADDRESS".into(), address.clone());
        envs.insert(
            "ADX_RUNTIME_CONTROL_TOKEN".into(),
            adx_protocol::runtime_stream::execution_token(secret.as_bytes(), &identity)?,
        );
    }
    envs.remove("ADX_RESTORE_ORIGIN");
    envs.insert("ADX_ENVIRONMENT_ID".into(), spec.id.clone());
    envs.insert("ADX_RUNTIME_ID".into(), runtime_id.into());
    envs.insert(
        "ADX_OWNERSHIP_GENERATION".into(),
        ownership_generation.to_string(),
    );
    let mut xpu = std::collections::BTreeMap::<String, Vec<u32>>::new();
    for d in devices {
        let kind = match d.kind {
            adx_core::scheduling::DeviceKind::Gpu => "gpu",
            adx_core::scheduling::DeviceKind::Npu => "npu",
        };
        xpu.entry(kind.into()).or_default().push(d.id);
    }
    options.validate_tunnel_network(options.network.as_ref())?;
    envs.retain(|key, _| !key.starts_with("EXECD_TUNNEL_"));
    if let Some(port) = options.tunnel_http_port {
        envs.insert("EXECD_TUNNEL_HTTP_PORT".into(), port.to_string());
        envs.insert("EXECD_TUNNEL_WS_PORT".into(), (port - 1).to_string());
    }
    // Only the typed constructor option may configure runtime initialization.
    envs.remove("ADX_ENVIRONMENT_CWD");
    if let Some(cwd) = &options.cwd {
        adx_core::execution::validate_cwd(cwd)?;
        envs.insert("ADX_ENVIRONMENT_CWD".into(), cwd.clone());
    }
    let mut mounts = options
        .mounts
        .iter()
        .map(environment_mount)
        .collect::<Result<Vec<_>>>()?;
    if custom_rootfs {
        mounts.extend(environment.map_or_else(Vec::new, |e| {
            vec![proto::Mount {
                r#type: if e.bootstrap.r#type == "image" {
                    "bind".into()
                } else {
                    e.bootstrap.r#type.clone()
                },
                target: e.bootstrap.target.clone(),
                options: if e.bootstrap.r#type == "image" {
                    vec!["ro".into(), "rbind".into()]
                } else {
                    vec!["ro".into()]
                },
                source: Some(if e.bootstrap.r#type == "image" {
                    proto::mount::Source::ImageUrl(e.bootstrap.image.clone())
                } else {
                    proto::mount::Source::HostPath(e.bootstrap.root.clone())
                }),
            }]
        }));
    }
    // Kata's default OCI spec mounts /dev as tmpfs but omits devpts. Execd
    // allocates terminals inside the guest, without CAP_SYS_ADMIN to mount it.
    // Let the runtime prepare a private PTY filesystem at container creation.
    if spec.runtime_class == "kata" && !mounts.iter().any(|mount| mount.target == "/dev/pts") {
        mounts.push(proto::Mount {
            r#type: "devpts".into(),
            target: "/dev/pts".into(),
            options: [
                "nosuid",
                "noexec",
                "newinstance",
                "ptmxmode=0666",
                "mode=0620",
                "gid=5",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect(),
            source: Some(proto::mount::Source::HostPath("devpts".into())),
        });
    }
    let image_process_config = environment.map_or(
        adx_core::runtime_profile::DEFAULT_IMAGE_PROCESS_CONFIG,
        |value| value.bootstrap.image_process_config.as_str(),
    );
    envs.remove(adx_core::probes::CONFIG_ENV);
    if options.readiness_probe.is_some() || options.healthcheck_probe.is_some() {
        envs.insert(
            adx_core::probes::CONFIG_ENV.into(),
            serde_json::to_string(&options.probe_config())
                .map_err(|e| Error::Invalid(e.to_string()))?,
        );
    }
    envs.remove(adx_core::execution::WORKLOAD_PROCESS_ENV);
    if let Some(process) = &options.process {
        process.validate()?;
        envs.insert(
            adx_core::execution::WORKLOAD_PROCESS_ENV.into(),
            serde_json::to_string(process).map_err(|error| Error::Invalid(error.to_string()))?,
        );
    }
    if options.inherit_entrypoint {
        envs.insert(
            "ADX_IMAGE_PROCESS_CONFIG".into(),
            image_process_config.into(),
        );
    }
    let cpu_limit = options.limits.cpu_millis.max(spec.resources.cpu_millis);
    let memory_limit = options.limits.memory_bytes.max(spec.resources.memory_bytes);
    let disk_limit = options.limits.disk_bytes.max(spec.resources.disk_bytes);
    let (stdout, stderr) = config
        .runtime_logs
        .as_ref()
        .map(|policy| {
            policy
                .paths(runtime_id)
                .map_err(|error| Error::Invalid(error.to_string()))
        })
        .transpose()?
        .unwrap_or_default();
    Ok(proto::StartRequest {
        xpu_allocations: xpu
            .into_iter()
            .map(|(kind, mut device_ids)| {
                device_ids.sort();
                proto::XpuAllocation {
                    r#type: kind,
                    device_ids,
                }
            })
            .collect(),
        sandbox_id: String::new(),
        runtime: spec.runtime_class.clone(),
        rootfs: Some(if let Some(rootfs) = &options.rootfs {
            environment_rootfs(rootfs)?
        } else if let Some(e) = environment.filter(|_| spec.image.is_empty()) {
            if e.rootfs.r#type == "image" {
                proto::RootfsConfig {
                    readonly: e.rootfs.readonly,
                    r#type: proto::RootfsSrcType::Image as i32,
                    source: Some(proto::rootfs_config::Source::ImageUrl(
                        e.rootfs.image.clone(),
                    )),
                    writable_layer_size_bytes: 0,
                }
            } else {
                proto::RootfsConfig {
                    readonly: e.rootfs.readonly,
                    r#type: proto::RootfsSrcType::Local as i32,
                    source: Some(proto::rootfs_config::Source::Path(e.rootfs.path.clone())),
                    writable_layer_size_bytes: 0,
                }
            }
        } else {
            proto::RootfsConfig {
                readonly: false,
                r#type: proto::RootfsSrcType::Image as i32,
                source: Some(proto::rootfs_config::Source::ImageUrl(spec.image.clone())),
                writable_layer_size_bytes: 0,
            }
        }),
        mounts,
        command: environment
            .map(|e| e.bootstrap.entrypoint.clone())
            .unwrap_or_else(|| config.command.clone()),
        cwd: config.cwd.clone(),
        envs,
        stdout,
        stderr,
        resources: HashMap::from([
            ("CPU".into(), cpu_limit as f64),
            ("Memory".into(), memory_limit as f64 / 1_048_576.0),
        ]),
        labels: HashMap::from([
            ("adx.environment_id".into(), spec.id.clone()),
            ("adx.tenant_id".into(), spec.tenant_id.clone()),
            ("adx.runtime_id".into(), runtime_id.into()),
            ("adx.generation".into(), ownership_generation.to_string()),
        ]),
        metric_labels: HashMap::from([
            ("environment_id".into(), spec.id.clone()),
            ("runtime_id".into(), runtime_id.into()),
            ("tenantid".into(), spec.tenant_id.clone()),
        ]),
        writable_layer_limit_bytes: disk_limit,
        extra_config: options.extra_config.clone(),
        network_policy: optional_network_policy(
            options.network.as_ref(),
            &options.ports,
            options.tunnel_http_port,
            config,
        )?,
        inject_entrypoint: if options.inherit_entrypoint {
            image_process_config.into()
        } else {
            String::new()
        },
        ..Default::default()
    })
}

pub(crate) fn validate_protected_ingress(spec: &EnvironmentSpec) -> Result<()> {
    spec.execution
        .validate_protected_ingress(spec.execution.network.as_ref())?;
    spec.execution.validate_private_service_ports()?;
    if !spec.execution.protected_ingress.is_empty()
        && !matches!(
            spec.runtime_class.as_str(),
            "runsc" | "firecracker" | "firecracker-pvm"
        )
    {
        return Err(Error::Invalid(
            "protected ingress requires an ACL-capable runtime".into(),
        ));
    }
    Ok(())
}

fn deployment_environment_accepts(
    requested: Option<&adx_core::runtime_profile::RuntimeProfile>,
    configured: Option<&adx_core::runtime_profile::RuntimeProfile>,
) -> bool {
    match (requested, configured) {
        (None, None) => true,
        (Some(requested), Some(configured)) => {
            // Environment rootfs settings are an overlay on the trusted node
            // deployment. The caller may select the runtime and read-only
            // behavior, but cannot redirect the deployment-owned source,
            // bootstrap executable, or environment.
            requested.bootstrap == configured.bootstrap
                && requested.env == configured.env
                && requested.rootfs.r#type == configured.rootfs.r#type
                && requested.rootfs.path == configured.rootfs.path
                && requested.rootfs.image == configured.rootfs.image
        }
        _ => false,
    }
}

fn environment_s3(value: &adx_core::execution::S3Source) -> proto::S3Config {
    proto::S3Config {
        endpoint: value.endpoint.clone(),
        bucket: value.bucket.clone(),
        object: value.object.clone(),
        access_key_id: value.access_key_id.clone(),
        access_key_secret: value.access_key_secret.clone(),
    }
}

fn environment_rootfs(value: &adx_core::execution::Rootfs) -> Result<proto::RootfsConfig> {
    use adx_core::execution::StorageSource;
    let (kind, source) = match &value.source {
        StorageSource::Image(value) => (
            proto::RootfsSrcType::Image,
            proto::rootfs_config::Source::ImageUrl(value.clone()),
        ),
        StorageSource::S3(value) => (
            proto::RootfsSrcType::S3,
            proto::rootfs_config::Source::S3Config(environment_s3(value)),
        ),
        StorageSource::Local(value) => (
            proto::RootfsSrcType::Local,
            proto::rootfs_config::Source::Path(value.clone()),
        ),
    };
    Ok(proto::RootfsConfig {
        readonly: value.readonly,
        r#type: kind as i32,
        source: Some(source),
        writable_layer_size_bytes: 0,
    })
}

fn environment_mount(value: &adx_core::execution::Mount) -> Result<proto::Mount> {
    use adx_core::execution::StorageSource;
    let inline = value.source.inline()?;
    let source = match inline {
        StorageSource::Image(value) => proto::mount::Source::ImageUrl(value.clone()),
        StorageSource::S3(value) => proto::mount::Source::S3Config(environment_s3(value)),
        StorageSource::Local(value) => proto::mount::Source::HostPath(value.clone()),
    };
    let mut options = value.options.clone();
    // OCI bind mounts require an explicit mount flag as well as type="bind".
    // Public Local mount options describe access flags such as ro/rw.
    if value.kind == "bind"
        && matches!(inline, StorageSource::Local(_))
        && !options.iter().any(|flag| flag == "bind" || flag == "rbind")
    {
        options.push("rbind".into());
    }
    Ok(proto::Mount {
        r#type: value.kind.clone(),
        target: value.target.clone(),
        options,
        source: Some(source),
    })
}

fn optional_network_policy(
    value: Option<&adx_core::execution::NetworkPolicy>,
    ports: &[u16],
    tunnel_http_port: Option<u16>,
    config: &Config,
) -> Result<Option<proto::NetworkPolicy>> {
    match (value, config.network_baseline.as_ref()) {
        (Some(value), _) => {
            environment_network_policy(value, ports, tunnel_http_port, config).map(Some)
        }
        (None, Some(baseline)) => {
            environment_network_policy(&baseline.enforce(None)?, ports, tunnel_http_port, config)
                .map(Some)
        }
        (None, None) => Ok(None),
    }
}

pub(crate) fn environment_network_policy(
    value: &adx_core::execution::NetworkPolicy,
    ports: &[u16],
    tunnel_http_port: Option<u16>,
    config: &Config,
) -> Result<proto::NetworkPolicy> {
    use adx_core::execution as model;
    crate::network_baseline::validate_peer_aliases(&config.network_peer_aliases)?;
    let mut resolved = value.clone();
    if let Some(traffic) = &mut resolved.traffic {
        for rule in &mut traffic.rules {
            if rule.peer.domain.is_empty() {
                continue;
            }
            if let Some(cidr) = crate::network_baseline::resolve_peer_alias(
                &config.network_peer_aliases,
                &rule.peer.domain,
            ) {
                if !rule.peer.address.is_empty() || !rule.peer.cidr.is_empty() {
                    return Err(Error::Invalid(
                        "network peer alias cannot be combined with an address or CIDR".into(),
                    ));
                }
                rule.peer.domain.clear();
                rule.peer.cidr = cidr.to_string();
            }
        }
    }
    let enforced = config
        .network_baseline
        .as_ref()
        .map(|baseline| baseline.enforce(Some(&resolved)))
        .transpose()?;
    let value = enforced.as_ref().unwrap_or(&resolved);
    if config
        .platform_ingress_sources
        .as_ref()
        .is_some_and(|sources| sources.is_empty())
    {
        return Err(Error::Invalid(
            "platform_ingress_sources must not be empty".into(),
        ));
    }
    if config.platform_ingress_sources.is_some()
        && value
            .traffic
            .as_ref()
            .is_none_or(|traffic| traffic.mode != model::TrafficMode::Stateful)
    {
        return Err(Error::Invalid(
            "source-bounded platform exceptions require stateful traffic policy".into(),
        ));
    }
    model::ExecutionOptions {
        tunnel_http_port,
        ports: ports.to_vec(),
        ..Default::default()
    }
    .validate_tunnel_network(Some(value))?;
    let action = |value| match value {
        model::NetworkAction::Allow => proto::NetworkPolicyAction::Allow as i32,
        model::NetworkAction::Deny => proto::NetworkPolicyAction::Deny as i32,
    };
    let execd_port = config
        .env
        .get("EXECD_HTTP_PORT")
        .map(|v| v.parse::<u16>())
        .transpose()
        .map_err(|_| Error::Invalid("invalid Execd control port".into()))?
        .unwrap_or(50090);
    if execd_port == 0 {
        return Err(Error::Invalid(
            "positive Execd control port required".into(),
        ));
    }
    let control_peer = runtime_control_peer(config)?;
    let traffic = value.traffic.as_ref().map(|traffic| {
        let mut rules: Vec<_> = traffic
            .rules
            .iter()
            .map(|rule| proto::TrafficRule {
                action: action(rule.action),
                direction: match rule.direction {
                    model::NetworkDirection::Ingress => proto::NetworkDirection::Ingress as i32,
                    model::NetworkDirection::Egress => proto::NetworkDirection::Egress as i32,
                    model::NetworkDirection::Both => proto::NetworkDirection::Both as i32,
                },
                protocol: match rule.protocol {
                    model::NetworkProtocol::Any => proto::NetworkProtocol::Any as i32,
                    model::NetworkProtocol::Tcp => proto::NetworkProtocol::Tcp as i32,
                    model::NetworkProtocol::Udp => proto::NetworkProtocol::Udp as i32,
                    model::NetworkProtocol::Icmp => proto::NetworkProtocol::Icmp as i32,
                },
                peer: Some(proto::NetworkEndpoint {
                    address: rule.peer.address.clone(),
                    port: rule.peer.port,
                    cidr: rule.peer.cidr.clone(),
                    domain: rule.peer.domain.clone(),
                    port_range: rule.peer.port_range.as_ref().map(|range| proto::PortRange {
                        first: range.first,
                        last: range.last,
                    }),
                }),
                sandbox_port: rule.environment_port,
                sandbox_port_range: rule.environment_port_range.as_ref().map(|range| {
                    proto::PortRange {
                        first: range.first,
                        last: range.last,
                    }
                }),
                priority: rule.priority,
            })
            .collect();
        // Control traffic and declared public ports remain reachable under a
        // deny-by-default user policy. UINT32_MAX is reserved by the public
        // contract for these platform-owned rules.
        let sources = config.platform_ingress_sources.as_ref().map_or_else(
            || vec![proto::NetworkEndpoint::default()],
            |sources| {
                sources
                    .iter()
                    .map(|source| proto::NetworkEndpoint {
                        cidr: source.to_string(),
                        ..Default::default()
                    })
                    .collect()
            },
        );
        for port in std::iter::once(execd_port).chain(ports.iter().copied()) {
            for source in &sources {
                rules.push(proto::TrafficRule {
                    action: proto::NetworkPolicyAction::Allow as i32,
                    // Execd control replies must not depend on a conntrack entry
                    // from the previous policy generation. Configured platform
                    // CIDRs also bound this bidirectional control-port exception.
                    // Stateless published ports need an explicit return path.
                    direction: if port == execd_port
                        || (config.platform_ingress_sources.is_none()
                            && traffic.mode == model::TrafficMode::Stateless)
                    {
                        proto::NetworkDirection::Both as i32
                    } else {
                        proto::NetworkDirection::Ingress as i32
                    },
                    protocol: proto::NetworkProtocol::Tcp as i32,
                    peer: Some(source.clone()),
                    // sandboxd schema v2 forbids the legacy scalar
                    // `sandbox_port`. An exact port is represented as a one-value
                    // range, including platform-owned EXECD and published-port
                    // exceptions.
                    sandbox_port_range: Some(proto::PortRange {
                        first: u32::from(port),
                        last: u32::from(port),
                    }),
                    priority: u32::MAX,
                    ..Default::default()
                });
            }
        }
        // Only a new inbound handshake may establish the tunnel return path.
        // A Both rule would allow arbitrary outbound TCP with this source port.
        if let Some(port) = tunnel_http_port {
            for source in &sources {
                rules.push(proto::TrafficRule {
                    action: proto::NetworkPolicyAction::Allow as i32,
                    direction: proto::NetworkDirection::Ingress as i32,
                    protocol: proto::NetworkProtocol::Tcp as i32,
                    peer: Some(source.clone()),
                    sandbox_port_range: Some(proto::PortRange {
                        first: u32::from(port - 1),
                        last: u32::from(port - 1),
                    }),
                    priority: u32::MAX,
                    ..Default::default()
                });
            }
        }
        // Execd initiates this connection. Preserve only the node-owned TCP
        // endpoint, including its replies under stateless policies. User env
        // cannot widen the destination or port, and schema v2 uses CIDR/ranges.
        if let Some(peer) = &control_peer {
            rules.push(proto::TrafficRule {
                action: proto::NetworkPolicyAction::Allow as i32,
                direction: proto::NetworkDirection::Both as i32,
                protocol: proto::NetworkProtocol::Tcp as i32,
                peer: Some(peer.clone()),
                priority: u32::MAX,
                ..Default::default()
            });
        }
        proto::TrafficPolicy {
            ingress_default_action: action(traffic.ingress_default_action),
            egress_default_action: action(traffic.egress_default_action),
            rules,
            mode: match traffic.mode {
                model::TrafficMode::Stateless => proto::TrafficPolicyMode::Stateless as i32,
                model::TrafficMode::Stateful => proto::TrafficPolicyMode::Stateful as i32,
            },
            ..Default::default()
        }
    });
    Ok(proto::NetworkPolicy {
        traffic,
        dns: value.dns.as_ref().map(|dns| proto::DnsPolicy {
            default_action: action(dns.default_action),
            rules: dns
                .rules
                .iter()
                .map(|rule| proto::DnsRule {
                    action: action(rule.action),
                    pattern: rule.pattern.clone(),
                })
                .collect(),
        }),
        schema_version: 2,
    })
}

fn runtime_control_peer(config: &Config) -> Result<Option<proto::NetworkEndpoint>> {
    if !config.env.contains_key("ADX_RUNTIME_CONTROL_SECRET") {
        return Ok(None);
    }
    let Some(address) = config.env.get("ADX_RUNTIME_CONTROL_ADDRESS") else {
        return Ok(None);
    };
    let endpoint = Endpoint::from_shared(address.clone())
        .map_err(|_| Error::Invalid("invalid runtime control address".into()))?;
    let host = endpoint
        .uri()
        .host()
        .and_then(|host| host.parse::<std::net::Ipv4Addr>().ok())
        .ok_or_else(|| {
            Error::Invalid(
                "runtime control requires a node IPv4 address for network isolation".into(),
            )
        })?;
    let port = u32::from(endpoint.uri().port_u16().unwrap_or(80));
    Ok(Some(proto::NetworkEndpoint {
        cidr: format!("{host}/32"),
        port_range: Some(proto::PortRange {
            first: port,
            last: port,
        }),
        ..Default::default()
    }))
}

#[async_trait]
impl RuntimeDriver for Sandboxd {
    fn recovery_profile(&self, spec: &EnvironmentSpec) -> Result<BTreeMap<String, String>> {
        if !spec.image.is_empty()
            || spec.execution.rootfs.is_some()
            || spec
                .runtime_profile
                .as_ref()
                .is_some_and(|profile| Some(profile) != self.config.runtime_profile.as_ref())
        {
            return Ok(BTreeMap::new());
        }
        let Some(published) = self.recovery_profiles.get(&spec.runtime_class) else {
            return Ok(BTreeMap::new());
        };
        let rootfs = rootfs_identity(self.config.runtime_profile.as_ref())?;
        let observed =
            recovery::profiles(rootfs.as_ref(), &self.config.checkpoint_runtime_binaries)?;
        if observed.get(&spec.runtime_class) != Some(published) {
            return Err(Error::Unavailable(
                "recovery backend or host changed; a new node session is required".into(),
            ));
        }
        Ok(published.clone())
    }

    async fn sandbox_id(&self, runtime_id: &str) -> Option<String> {
        self.physical_id(runtime_id).await.ok().flatten()
    }
    fn runtime_rootfs_identity(&self) -> Option<crate::RuntimeRootfsIdentity> {
        self.rootfs_identity.clone()
    }

    // Keep RuntimeDriver's conservative input retention: sandboxd can enable
    // runsc background page loading independently of adxlet configuration.
    // Firecracker also retains its caller-owned incremental memory base.
    fn retains_capture_files(&self, runtime_class: &str) -> bool {
        runtime_class != "runsc"
    }

    async fn stats(&self, runtime_id: &str) -> Result<crate::metrics::RuntimeUsage> {
        let value = Sandboxd::stats(self, runtime_id).await?;
        Ok(crate::metrics::RuntimeUsage {
            cpu_usage_ns: value.cpu_usage_ns,
            memory_usage_bytes: value.memory_usage_bytes,
            memory_limit_bytes: value.memory_limit_bytes,
        })
    }
    async fn checkpoint_supported(&self, runtime: &str) -> Result<()> {
        match self.capabilities(runtime).await? {
            Some(info)
                if info.supports_checkpoint_restore
                    && !info.checkpoint_handoff_path.is_empty()
                    && !info.restore_env_path.is_empty() =>
            {
                Ok(())
            }
            _ => Err(Error::UnsupportedCapability(
                UnsupportedCapability::CheckpointRestore,
            )),
        }
    }
    async fn checkpoint(&self, runtime_id: &str, path: &Path, duration: Duration) -> Result<()> {
        self.capture_checkpoint(runtime_id, path, duration, false)
            .await
    }
    async fn checkpoint_running(
        &self,
        runtime_id: &str,
        path: &Path,
        duration: Duration,
    ) -> Result<()> {
        self.capture_checkpoint(runtime_id, path, duration, true)
            .await
    }
    async fn restore(
        &self,
        spec: &EnvironmentSpec,
        runtime_id: &str,
        generation: u64,
        devices: &[adx_core::scheduling::DeviceAllocation],
        path: &Path,
    ) -> Result<IpAddr> {
        self.restore_from(spec, runtime_id, generation, devices, path, None)
            .await
    }
    async fn restore_from(
        &self,
        spec: &EnvironmentSpec,
        runtime_id: &str,
        generation: u64,
        devices: &[adx_core::scheduling::DeviceAllocation],
        path: &Path,
        origin: Option<&adx_core::runtime::RuntimeIdentity>,
    ) -> Result<IpAddr> {
        let started = tokio::time::Instant::now();
        let request = self
            .restore_request(
                spec,
                runtime_id,
                generation,
                devices,
                crate::RestoreSource { path, origin },
            )
            .await?;
        let request_ms = started.elapsed().as_secs_f64() * 1_000.0;
        let start = tokio::time::Instant::now();
        let result = self.execute_start(runtime_id, request, None).await;
        adx_observability::info!(
            event = "environment_backend_restore_stages", environment_id = %spec.id,
            %runtime_id, request_ms, start_rpc_ms = start.elapsed().as_secs_f64() * 1_000.0,
            total_ms = started.elapsed().as_secs_f64() * 1_000.0,
            success = result.is_ok(), "backend restore stages completed"
        );
        result
    }
    async fn restore_from_with_deadline(
        &self,
        spec: &EnvironmentSpec,
        runtime_id: &str,
        generation: u64,
        devices: &[adx_core::scheduling::DeviceAllocation],
        source: crate::RestoreSource<'_>,
        deadline: tokio::time::Instant,
    ) -> Result<IpAddr> {
        let request = self
            .restore_request(spec, runtime_id, generation, devices, source)
            .await?;
        self.execute_start(runtime_id, request, Some(deadline))
            .await
    }
    async fn inventory(&self) -> Result<Vec<crate::RuntimeObservation>> {
        self.list_id("")
            .await?
            .into_iter()
            .filter(|s| s.labels.contains_key("adx.environment_id"))
            .map(|s| {
                let environment_id = s.labels["adx.environment_id"].clone();
                let tenant_id = s
                    .labels
                    .get("adx.tenant_id")
                    .cloned()
                    .ok_or_else(|| unavailable("managed runtime has no tenant"))?;
                let generation = s
                    .labels
                    .get("adx.generation")
                    .and_then(|v| v.parse::<u64>().ok())
                    .filter(|v| *v > 0)
                    .ok_or_else(|| unavailable("managed runtime has invalid identity"))?;
                Ok(crate::RuntimeObservation {
                    environment_id,
                    tenant_id,
                    generation,
                    runtime_id: s.id,
                    running: s.state == proto::SandboxState::Running as i32,
                })
            })
            .collect()
    }

    async fn is_running(&self, runtime_id: &str) -> Result<bool> {
        Sandboxd::is_running(self, runtime_id).await
    }
    async fn wait_exit(&self, runtime_id: &str) -> Result<adx_core::RuntimeFailure> {
        let id = self.physical_id(runtime_id).await?.ok_or(Error::NotFound)?;
        let result = self
            .client
            .clone()
            .wait(Request::new(proto::WaitRequest { id }))
            .await
            .map_err(unavailable)?
            .into_inner();
        Ok(runtime_failure(result))
    }
    async fn observe(&self, runtime_id: &str) -> Result<crate::RuntimeStatus> {
        Sandboxd::observe(self, runtime_id).await
    }
    async fn set_network_policy(
        &self,
        runtime_id: &str,
        policy: Option<&adx_core::execution::NetworkPolicy>,
        ports: &[u16],
        tunnel_http_port: Option<u16>,
    ) -> Result<()> {
        let id = self.physical_id(runtime_id).await?.ok_or(Error::NotFound)?;
        self.client
            .clone()
            .set_network_policy(self.request(proto::SetNetworkPolicyRequest {
                sandbox_id: id,
                network_policy: optional_network_policy(
                    policy,
                    ports,
                    tunnel_http_port,
                    &self.config,
                )?,
            }))
            .await
            .map_err(unavailable)?;
        Ok(())
    }
    async fn start(
        &self,
        spec: &EnvironmentSpec,
        runtime_id: &str,
        ownership_generation: u64,
        devices: &[adx_core::scheduling::DeviceAllocation],
    ) -> Result<IpAddr> {
        let request = self
            .execution_request(spec, runtime_id, ownership_generation, devices)
            .await?;
        self.execute_start(runtime_id, request, None).await
    }

    async fn start_with_deadline(
        &self,
        spec: &EnvironmentSpec,
        runtime_id: &str,
        generation: u64,
        devices: &[adx_core::scheduling::DeviceAllocation],
        deadline: tokio::time::Instant,
    ) -> Result<IpAddr> {
        let request = self
            .execution_request(spec, runtime_id, generation, devices)
            .await?;
        self.execute_start(runtime_id, request, Some(deadline))
            .await
    }

    async fn remove(&self, runtime_id: &str) -> Result<()> {
        let mut state = self.cell(runtime_id).lock_owned().await;
        let failed_start = matches!(*state, StartState::Failed | StartState::UncertainStart);
        if matches!(*state, StartState::UncertainStart) {
            // A failed create is terminal locally, not proof that backend work
            // stopped. Remove what is visible now; periodic inventory will find
            // late instances even after this controller or process is gone.
            *state = StartState::Failed;
        }
        if matches!(*state, StartState::Uncertain) {
            if self.list_id(runtime_id).await?.is_empty() {
                return Err(unavailable(
                    "cannot confirm cleanup while checkpoint outcome is unknown",
                ));
            }
            *state = if failed_start {
                StartState::Failed
            } else {
                StartState::Settled
            };
        }
        if let Some(physical) = self.physical_id(runtime_id).await? {
            match self
                .client
                .clone()
                .delete(self.request(proto::DeleteRequest {
                    id: physical,
                    timeout: 0,
                }))
                .await
            {
                Ok(_) => (),
                Err(error) if error.code() == Code::NotFound => (),
                Err(error) => return Err(unavailable(error)),
            }
        }
        if self
            .list_id(runtime_id)
            .await?
            .iter()
            .any(|x| x.id == runtime_id)
        {
            return Err(unavailable("runtime remains after Delete"));
        }
        self.backend_ids
            .lock()
            .expect("shared state lock poisoned")
            .remove(runtime_id);
        // A failed create cannot become successful through a retry of the same
        // execution. Keep its identity terminal so a later backend is cleanup-only.
        *state = if failed_start {
            StartState::Failed
        } else {
            StartState::Idle
        };
        Ok(())
    }
}

impl Sandboxd {
    async fn capture_checkpoint(
        &self,
        runtime_id: &str,
        path: &Path,
        duration: Duration,
        leave_running: bool,
    ) -> Result<()> {
        if !path.is_absolute() || duration.is_zero() {
            return Err(Error::Invalid(
                "absolute checkpoint directory and positive timeout required".into(),
            ));
        }
        let mut state = self.cell(runtime_id).lock_owned().await;
        if matches!(*state, StartState::Uncertain | StartState::UncertainStart) {
            return Err(unavailable("execution outcome requires reconciliation"));
        }
        if matches!(*state, StartState::Failed) {
            return Err(Error::Conflict);
        }
        let id = self.physical_id(runtime_id).await?.ok_or(Error::NotFound)?;
        let mut request = Request::new(proto::CheckpointRequest {
            id,
            checkpoint_dir: path
                .to_str()
                .ok_or_else(|| Error::Invalid("checkpoint path is not UTF-8".into()))?
                .into(),
            timeout_seconds: duration
                .as_secs()
                .try_into()
                .map_err(|_| Error::Invalid("checkpoint timeout overflow".into()))?,
            compress: false,
            leave_running,
            snapshot_type: "Full".into(),
        });
        // The caller passes the remaining operation budget. Do not add a new
        // transport allowance that would extend the user-visible deadline.
        request.set_timeout(duration);
        let mut client = self.client.clone();
        // Retain the operation guard on caller cancellation, as with Start.
        *state = StartState::Uncertain;
        tokio::spawn(async move {
            match client.checkpoint(request).await {
                Ok(_) => {
                    *state = StartState::Settled;
                    Ok(())
                }
                Err(error) => {
                    if matches!(
                        error.code(),
                        Code::InvalidArgument
                            | Code::Unimplemented
                            | Code::PermissionDenied
                            | Code::Unauthenticated
                    ) {
                        *state = StartState::Settled;
                    }
                    Err(unavailable(error))
                }
            }
        })
        .await
        .map_err(unavailable)?
    }
}

fn runtime_failure(result: proto::WaitResponse) -> adx_core::RuntimeFailure {
    let oom = result
        .message
        .contains("sandbox was oom-killed by the kernel");
    adx_core::RuntimeFailure {
        probe_failure: None,
        reason: if oom {
            adx_core::RuntimeFailureReason::OomKilled
        } else {
            adx_core::RuntimeFailureReason::Exited
        },
        exit_code: Some(result.exit_code),
        message: if oom {
            "environment backend was OOM-killed".into()
        } else {
            "environment backend exited".into()
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    #[test]
    fn field_four_is_the_environment_ip() {
        let decoded = proto::StartResponse::decode(&b"\x22\x0810.0.0.2"[..]).unwrap();
        assert_eq!(decoded.sandbox_ip, "10.0.0.2");
        assert_eq!(
            proto::StartResponse::decode(&b""[..]).unwrap().sandbox_ip,
            ""
        );
    }

    #[test]
    fn default_local_rootfs_keeps_explicit_workload_without_image_injection() {
        let profile: adx_core::runtime_profile::RuntimeProfile =
            serde_json::from_value(serde_json::json!({
                "rootfs":{"runtime_class":"runsc","type":"local","path":"/opt/adx/runtime.img"},
                "bootstrap":{"type":"erofs","root":"/opt/adx/runtime.img",
                    "target":"/__adx","entrypoint":["/__adx/usr/local/bin/adx-execd"]}
            }))
            .unwrap();
        let process = adx_core::execution::WorkloadProcess {
            commands: Some(vec![vec![
                "/opt/python/bin/python".into(),
                "-m".into(),
                "agentserver".into(),
            ]]),
            cwd: Some("/home/agentos".into()),
            ..Default::default()
        };
        let spec = EnvironmentSpec {
            id: "local-workload".into(),
            tenant_id: "tenant".into(),
            image: String::new(),
            runtime_class: "runsc".into(),
            runtime_profile: Some(profile.clone()),
            run_policy: None,
            envd: None,
            compatibility: None,
            snapshot_id: None,
            lifecycle: Default::default(),
            env: Default::default(),
            scheduling: Default::default(),
            resources: adx_core::Resources {
                cpu_millis: 1000,
                memory_bytes: 512 * 1024 * 1024,
                disk_bytes: 0,
            },
            priority: 0,
            execution: adx_core::execution::ExecutionOptions {
                process: Some(process.clone()),
                ..Default::default()
            },
        };
        let request = start_request(
            &spec,
            "local-workload-1",
            1,
            &[],
            &Config {
                runtime_profile: Some(profile),
                ..Config::default()
            },
        )
        .unwrap();
        assert_eq!(request.runtime, "runsc");
        assert_eq!(
            request.rootfs.unwrap().source,
            Some(proto::rootfs_config::Source::Path(
                "/opt/adx/runtime.img".into()
            ))
        );
        assert!(request.inject_entrypoint.is_empty());
        assert!(!request.envs.contains_key("ADX_IMAGE_PROCESS_CONFIG"));
        assert_eq!(
            serde_json::from_str::<adx_core::execution::WorkloadProcess>(
                &request.envs[adx_core::execution::WORKLOAD_PROCESS_ENV]
            )
            .unwrap(),
            process
        );
    }

    #[test]
    fn deployment_envd_payload_is_enabled_without_compatibility_identity() {
        let profile: adx_core::runtime_profile::RuntimeProfile = serde_json::from_value(serde_json::json!({
            "rootfs":{"runtime_class":"runc","type":"local","path":"/opt/adx/runtime.img","readonly":false},
            "bootstrap":{"type":"erofs","root":"/opt/adx/runtime.img","target":"/__adx","entrypoint":["/__adx/usr/local/bin/adx-execd"]},
            "env":{"ADX_ENVD_PATH":"/usr/local/bin/envd","ADX_ENVD_SHA256":"a".repeat(64),"ADX_ENVD_DEFAULT_USER":"user","ADX_ENVD_DEFAULT_WORKDIR":"/home/user","ADX_ENVD_FREEZE_ON_CHECKPOINT":"false"}
        })).unwrap();
        let mut spec = EnvironmentSpec {
            run_policy: None,
            envd: None,
            compatibility: None,
            runtime_profile: Some(profile.clone()),
            snapshot_id: None,
            lifecycle: Default::default(),
            env: [
                ("ADX_ENVD_FREEZE_ON_CHECKPOINT".into(), "true".into()),
                ("ADX_ENVD_PATH".into(), "/untrusted/envd".into()),
                ("ADX_ENVD_VERSION".into(), "untrusted".into()),
                ("ADX_ENVD_ACCESS_TOKEN".into(), "untrusted".into()),
            ]
            .into(),
            scheduling: Default::default(),
            id: "compat-switch".into(),
            tenant_id: "tenant".into(),
            image: String::new(),
            runtime_class: "runc".into(),
            resources: adx_core::Resources {
                cpu_millis: 1000,
                memory_bytes: 512 * 1024 * 1024,
                disk_bytes: 0,
            },
            priority: 0,
            execution: Default::default(),
        };
        let config = Config {
            runtime_profile: Some(profile),
            ..Config::default()
        };
        let native = start_request(&spec, "compat-switch-1", 1, &[], &config).unwrap();
        assert!(!native.envs.contains_key("ADX_ENVD_PATH"));
        assert!(!native.envs.contains_key("ADX_ENVD_ACCESS_TOKEN"));
        assert!(!native.envs.contains_key("ADX_ENVD_FREEZE_ON_CHECKPOINT"));
        assert!(!native.envs.contains_key("ADX_ENVD_VERSION"));
        spec.envd = Some(adx_core::envd::EnvdServiceConfig {
            version: "0.9.0".into(),
            access_token: Some("t".repeat(32)),
            gateway_auth: false,
            ingress_sources: Vec::new(),
        });
        let compatible = start_request(&spec, "compat-switch-2", 2, &[], &config).unwrap();
        assert_eq!(compatible.envs["ADX_ENVD_PATH"], "/usr/local/bin/envd");
        assert_eq!(compatible.envs["ADX_ENVD_VERSION"], "0.9.0");
        assert!(spec.compatibility.is_none());
        assert_eq!(compatible.envs["ADX_ENVD_ACCESS_TOKEN"], "t".repeat(32));
        assert_eq!(compatible.envs["ADX_ENVD_FREEZE_ON_CHECKPOINT"], "false");
        assert!(!compatible.envs.contains_key("ADX_ENVD_GATEWAY_AUTH"));
        spec.runtime_profile = None;
        assert!(start_request(&spec, "missing-profile-1", 1, &[], &Config::default()).is_err());
        let directory = tempfile::tempdir().unwrap();
        let mut profile = config.runtime_profile.unwrap();
        profile.rootfs.path = directory.path().join("root.img").to_string_lossy().into();
        profile.bootstrap.root = profile.rootfs.path.clone();
        std::fs::write(&profile.rootfs.path, b"first").unwrap();
        let first = rootfs_identity(Some(&profile)).unwrap().unwrap();
        std::fs::write(&profile.rootfs.path, b"second").unwrap();
        assert_ne!(first, rootfs_identity(Some(&profile)).unwrap().unwrap());
    }

    #[test]
    fn protected_ingress_is_generic_and_cannot_be_overridden_by_published_ports() {
        use adx_core::execution::{
            NetworkAction, NetworkDirection, NetworkPeer, NetworkPolicy, NetworkProtocol,
            NetworkRule, TrafficMode, TrafficPolicy,
        };
        let protected = vec![NetworkRule {
            action: NetworkAction::Deny,
            direction: NetworkDirection::Ingress,
            protocol: NetworkProtocol::Tcp,
            peer: NetworkPeer::default(),
            environment_port: 8081,
            environment_port_range: None,
            priority: 10,
        }];
        let mut spec: EnvironmentSpec = serde_json::from_value(serde_json::json!({
            "id":"private-service", "tenant_id":"tenant", "runtime_class":"runsc",
            "image":"image", "priority":0,
            "resources":{"cpu_millis":1000,"memory_bytes":1024,"disk_bytes":0},
        }))
        .unwrap();
        spec.execution.protected_ingress = protected.clone();
        spec.execution.network = Some(NetworkPolicy {
            traffic: Some(TrafficPolicy {
                ingress_default_action: NetworkAction::Allow,
                egress_default_action: NetworkAction::Allow,
                rules: protected,
                mode: TrafficMode::Stateful,
            }),
            dns: None,
        });
        assert!(validate_protected_ingress(&spec).is_ok());
        spec.execution.ports.push(8081);
        assert!(validate_protected_ingress(&spec).is_err());
        spec.execution.ports.clear();
        spec.runtime_class = "runc".into();
        assert!(validate_protected_ingress(&spec).is_err());
        spec.runtime_class = "runsc".into();
        spec.execution
            .network
            .as_mut()
            .unwrap()
            .traffic
            .as_mut()
            .unwrap()
            .rules[0]
            .action = NetworkAction::Allow;
        assert!(validate_protected_ingress(&spec).is_err());
        spec.execution.network = None;
        assert!(validate_protected_ingress(&spec).is_err());
    }

    #[test]
    fn request_preserves_execution_identity_image_and_resource_units() {
        let spec = EnvironmentSpec {
            run_policy: None,
            envd: None,
            compatibility: None,
            runtime_profile: None,
            snapshot_id: None,
            lifecycle: Default::default(),
            env: [
                ("USER_VALUE".into(), "preserved".into()),
                ("ADX_ENVIRONMENT_ID".into(), "spoofed-by-spec".into()),
                ("ADX_RESTORE_ORIGIN".into(), "spoofed-origin".into()),
            ]
            .into(),
            scheduling: Default::default(),
            id: "i".into(),
            tenant_id: "t".into(),
            image: "image:tag".into(),
            runtime_class: "runsc".into(),
            resources: adx_core::Resources {
                cpu_millis: 1500,
                memory_bytes: 2 * 1024 * 1024 * 1024,
                disk_bytes: 5 * 1024 * 1024 * 1024,
            },
            priority: 0,
            execution: Default::default(),
        };
        let default_request = start_request(&spec, "i-7", 7, &[], &Config::default()).unwrap();
        assert!(!default_request.envs.contains_key("EXECD_HTTP_ONLY"));
        assert!(
            default_request.ports.is_empty(),
            "ADX forwarding must not publish host DNAT ports"
        );
        assert_eq!(default_request.envs["EXECD_HTTP_PORT"], "50090");
        let config = Config {
            env: HashMap::from([
                ("ADX_ENVIRONMENT_ID".into(), "spoofed".into()),
                ("USER_ENV".into(), "kept".into()),
            ]),
            ..Config::default()
        };
        let request = start_request(&spec, "i-7", 7, &[], &config).unwrap();
        assert!(!request.envs.contains_key("ADX_RESTORE_ORIGIN"));
        assert_eq!(request.envs["ADX_ENVIRONMENT_ID"], "i");
        assert_eq!(request.envs["ADX_RUNTIME_ID"], "i-7");
        assert_eq!(request.envs["ADX_OWNERSHIP_GENERATION"], "7");
        assert_eq!(request.envs["USER_ENV"], "kept");
        assert_eq!(request.envs["USER_VALUE"], "preserved");
        assert!(start_request(&spec, "i-7", 0, &[], &config).is_err());
        assert!(request.sandbox_id.is_empty());
        assert_eq!(request.labels["adx.runtime_id"], "i-7");
        assert_eq!(request.labels["adx.generation"], "7");
        assert_eq!(request.metric_labels["environment_id"], "i");
        assert_eq!(request.metric_labels["runtime_id"], "i-7");
        assert_eq!(request.metric_labels["tenantid"], "t");
        assert_eq!(request.runtime, "runsc");
        assert_eq!(request.resources["CPU"], 1500.0);
        assert_eq!(request.resources["Memory"], 2048.0);
        assert_eq!(request.writable_layer_limit_bytes, 5 * 1024 * 1024 * 1024);
        assert_eq!(
            request.rootfs.unwrap().source,
            Some(proto::rootfs_config::Source::ImageUrl("image:tag".into()))
        );
    }

    #[test]
    fn node_peer_aliases_preserve_requested_access_without_a_ceiling() {
        use adx_core::execution::{
            DnsPolicy, NetworkAction, NetworkDirection, NetworkPeer, NetworkPolicy,
            NetworkProtocol, NetworkRule, TrafficMode, TrafficPolicy,
        };
        let policy = NetworkPolicy {
            traffic: Some(TrafficPolicy {
                ingress_default_action: NetworkAction::Deny,
                egress_default_action: NetworkAction::Deny,
                mode: TrafficMode::Stateful,
                rules: vec![
                    NetworkRule {
                        action: NetworkAction::Allow,
                        direction: NetworkDirection::Egress,
                        protocol: NetworkProtocol::Tcp,
                        peer: NetworkPeer {
                            domain: "ingress.node.invalid".into(),
                            port: 8080,
                            ..Default::default()
                        },
                        environment_port: 0,
                        environment_port_range: None,
                        priority: 300,
                    },
                    NetworkRule {
                        action: NetworkAction::Allow,
                        direction: NetworkDirection::Egress,
                        protocol: NetworkProtocol::Tcp,
                        peer: NetworkPeer {
                            domain: "model.example".into(),
                            port: 18046,
                            ..Default::default()
                        },
                        environment_port: 0,
                        environment_port_range: None,
                        priority: 300,
                    },
                ],
            }),
            dns: Some(DnsPolicy {
                default_action: NetworkAction::Deny,
                rules: Vec::new(),
            }),
        };
        for bridge in ["10.88.4.1/32", "10.99.0.1/32"] {
            let config = Config {
                network_peer_aliases: BTreeMap::from([(
                    "ingress.node.invalid".into(),
                    bridge.parse().unwrap(),
                )]),
                platform_ingress_sources: Some(vec![bridge.parse().unwrap()]),
                ..Default::default()
            };
            // Creation and policy updates share this translation; replay on a new
            // node must use that node's address without changing the stored spec.
            let translated = environment_network_policy(&policy, &[18092], None, &config).unwrap();
            let traffic = translated.traffic.unwrap();
            assert_eq!(
                traffic.egress_default_action,
                proto::NetworkPolicyAction::Deny as i32
            );
            assert_eq!(
                traffic.ingress_default_action,
                proto::NetworkPolicyAction::Deny as i32
            );
            let ingress = traffic.rules[0].peer.as_ref().unwrap();
            assert_eq!(ingress.cidr, bridge);
            assert!(ingress.domain.is_empty());
            assert_eq!(ingress.port, 8080);
            let model = traffic.rules[1].peer.as_ref().unwrap();
            assert_eq!(model.domain, "model.example");
            assert_eq!(model.port, 18046);
            assert_eq!(traffic.rules[1].priority, 300);
            assert_eq!(
                translated.dns.unwrap().default_action,
                proto::NetworkPolicyAction::Deny as i32
            );
            let platform_rules = &traffic.rules[2..];
            assert_eq!(platform_rules.len(), 2);
            assert_eq!(
                platform_rules[0].direction,
                proto::NetworkDirection::Both as i32
            );
            assert_eq!(
                platform_rules[0].sandbox_port_range.as_ref().unwrap().first,
                50090
            );
            assert_eq!(
                platform_rules[1].direction,
                proto::NetworkDirection::Ingress as i32
            );
            assert_eq!(
                platform_rules[1].sandbox_port_range.as_ref().unwrap().first,
                18092
            );
            for rule in platform_rules {
                assert_eq!(rule.peer.as_ref().unwrap().cidr, bridge);
                assert_eq!(rule.protocol, proto::NetworkProtocol::Tcp as i32);
            }
            assert!(optional_network_policy(None, &[], None, &config)
                .unwrap()
                .is_none());
            let mut malformed = policy.clone();
            malformed.traffic.as_mut().unwrap().rules[0].peer.cidr = "192.0.2.0/24".into();
            assert!(environment_network_policy(&malformed, &[], None, &config).is_err());
        }
        assert_eq!(
            policy.traffic.unwrap().rules[0].peer.domain,
            "ingress.node.invalid"
        );
    }

    #[test]
    fn source_bounded_workload_ports_do_not_allow_outbound_source_port_bypass() {
        use adx_core::execution::{NetworkAction, NetworkPolicy, TrafficMode, TrafficPolicy};
        let mut policy = NetworkPolicy {
            traffic: Some(TrafficPolicy {
                ingress_default_action: NetworkAction::Deny,
                egress_default_action: NetworkAction::Deny,
                rules: Vec::new(),
                mode: TrafficMode::Stateful,
            }),
            dns: None,
        };
        let config = Config {
            platform_ingress_sources: Some(vec!["10.88.4.1/32".parse().unwrap()]),
            ..Default::default()
        };
        let translated =
            environment_network_policy(&policy, &[18092], Some(8766), &config).unwrap();
        let rules = translated.traffic.unwrap().rules;
        assert_eq!(rules.len(), 3);
        let expected = [
            (50090, proto::NetworkDirection::Both),
            (18092, proto::NetworkDirection::Ingress),
            (8765, proto::NetworkDirection::Ingress),
        ];
        for (rule, (port, direction)) in rules.iter().zip(expected) {
            assert_eq!(rule.direction, direction as i32);
            assert_eq!(rule.peer.as_ref().unwrap().cidr, "10.88.4.1/32");
            assert_eq!(rule.protocol, proto::NetworkProtocol::Tcp as i32);
            let range = rule.sandbox_port_range.as_ref().unwrap();
            assert_eq!((range.first, range.last), (port, port));
        }
        policy.traffic.as_mut().unwrap().mode = TrafficMode::Stateless;
        assert!(environment_network_policy(&policy, &[], None, &config).is_err());
        assert!(environment_network_policy(&policy, &[], None, &Config::default()).is_ok());
    }
    #[test]
    fn runtime_control_credentials_are_scoped_and_host_secret_is_not_injected() {
        let mut spec = EnvironmentSpec {
            run_policy: None,
            envd: None,
            compatibility: None,
            id: "i".into(),
            tenant_id: "t".into(),
            image: "image".into(),
            runtime_class: "runsc".into(),
            resources: adx_core::Resources {
                cpu_millis: 100,
                memory_bytes: 1024,
                disk_bytes: 1024,
            },
            runtime_profile: None,
            snapshot_id: None,
            lifecycle: Default::default(),
            env: Default::default(),
            scheduling: Default::default(),
            priority: 0,
            execution: Default::default(),
        };
        spec.env.insert(
            "ADX_RUNTIME_CONTROL_ADDRESS".into(),
            "http://attacker:1".into(),
        );
        spec.env
            .insert("ADX_RUNTIME_CONTROL_SECRET".into(), "user-secret".into());
        spec.env
            .insert("ADX_RUNTIME_CONTROL_TOKEN".into(), "user-token".into());
        let request = start_request(&spec, "i-7", 7, &[], &Config::default()).unwrap();
        assert!(!request.envs.contains_key("ADX_RUNTIME_CONTROL_ADDRESS"));
        assert!(!request.envs.contains_key("ADX_RUNTIME_CONTROL_TOKEN"));
        assert!(!request.envs.contains_key("ADX_RUNTIME_CONTROL_SECRET"));
        let secret = "0123456789abcdef0123456789abcdef";
        let config = Config {
            env: HashMap::from([
                (
                    "ADX_RUNTIME_CONTROL_ADDRESS".into(),
                    "http://node:19003".into(),
                ),
                ("ADX_RUNTIME_CONTROL_SECRET".into(), secret.into()),
            ]),
            ..Config::default()
        };
        let request = start_request(&spec, "i-7", 7, &[], &config).unwrap();
        let identity = adx_core::runtime::RuntimeIdentity {
            environment_id: spec.id.clone(),
            runtime_id: "i-7".into(),
            ownership_generation: 7,
        };
        assert_eq!(
            request.envs["ADX_RUNTIME_CONTROL_ADDRESS"],
            "http://node:19003"
        );
        adx_protocol::runtime_stream::verify_token(
            secret.as_bytes(),
            &identity,
            &request.envs["ADX_RUNTIME_CONTROL_TOKEN"],
        )
        .unwrap();
        assert!(!request.envs.contains_key("ADX_RUNTIME_CONTROL_SECRET"));
    }
    #[test]
    fn workload_probe_configuration_is_node_owned_and_preserves_init_budget() {
        let workload = EnvironmentSpec {
            run_policy: None,
            envd: None,
            compatibility: None,
            execution: adx_core::execution::ExecutionOptions {
                inherit_entrypoint: true,
                rootfs: Some(adx_core::execution::Rootfs {
                    readonly: false,
                    source: adx_core::execution::StorageSource::Image("server".into()),
                }),
                init_timeout_seconds: 120,
                readiness_probe: Some(adx_core::probes::ProbeSpec {
                    action: adx_core::probes::ProbeAction::TcpSocket { port: 8080 },
                    initial_delay_seconds: 0,
                    period_seconds: 2,
                    timeout_seconds: 1,
                    failure_threshold: 1,
                }),
                ..Default::default()
            },
            id: "i".into(),
            tenant_id: "t".into(),
            image: "image".into(),
            runtime_class: "runsc".into(),
            resources: adx_core::Resources {
                cpu_millis: 100,
                memory_bytes: 1024,
                disk_bytes: 1024,
            },
            runtime_profile: None,
            snapshot_id: None,
            lifecycle: Default::default(),
            env: Default::default(),
            scheduling: Default::default(),
            priority: 0,
        };
        let mut config = Config::default();
        config
            .env
            .insert(adx_core::probes::CONFIG_ENV.into(), "spoofed".into());
        let request = start_request(&workload, "i-7", 7, &[], &config).unwrap();
        let probes: adx_core::probes::ProbeConfig =
            serde_json::from_str(&request.envs[adx_core::probes::CONFIG_ENV]).unwrap();
        assert_eq!(probes.init_timeout_seconds, 120);
        assert_eq!(probes.readiness, workload.execution.readiness_probe);
        let mut plain = workload;
        plain.execution.readiness_probe = None;
        let without_probes = start_request(&plain, "i-8", 8, &[], &config).unwrap();
        assert!(!without_probes
            .envs
            .contains_key(adx_core::probes::CONFIG_ENV));
    }
}
