//! sandboxd PR #56 adapter. The RPC task outlives a canceled caller so cleanup
//! cannot race a still-running Start. Ambiguous transport failures retain capacity.
use crate::RuntimeBackend;
use adx_core::{Error, InstanceSpec, Result};
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

pub mod proto {
    tonic::include_proto!("runtime.v1");
}
use proto::sandbox_service_client::SandboxServiceClient;

#[derive(Clone)]
pub struct Config {
    pub runtime_environment: Option<adx_core::environment::RuntimeEnvironment>,
    pub command: Vec<String>,
    pub env: HashMap<String, String>,
    pub cwd: String,
    pub rpc_timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            runtime_environment: None,
            command: vec!["/usr/local/bin/rrt-runtime".into()],
            env: HashMap::from([("RRT_HTTP_PORT".into(), "50090".into())]),
            cwd: "/".into(),
            rpc_timeout: Duration::from_secs(30),
        }
    }
}

enum StartState {
    Idle,
    Started(IpAddr),
    Settled,
    Uncertain,
}
type Cell = Arc<tokio::sync::Mutex<StartState>>;

#[derive(Clone)]
pub struct Sandboxd {
    client: SandboxServiceClient<Channel>,
    config: Config,
    starts: Arc<Mutex<BTreeMap<String, Cell>>>,
    backend_ids: Arc<Mutex<BTreeMap<String, String>>>,
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
    pub async fn connect(path: PathBuf, config: Config) -> Result<Self> {
        if (config.command.is_empty() && config.runtime_environment.is_none())
            || config.rpc_timeout.is_zero()
        {
            return Err(Error::Invalid(
                "sandboxd command and positive RPC timeout are required".into(),
            ));
        }
        if let Some(e) = &config.runtime_environment {
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
        spec: &InstanceSpec,
        runtime_id: &str,
        generation: u64,
        devices: &[adx_core::scheduling::DeviceAllocation],
    ) -> Result<Request<proto::StartRequest>> {
        let mut config = self.config.clone();
        // Runtime capability metadata owns the guest paths. Never inherit caller
        // values that could redirect the privileged checkpoint handoff.
        config
            .env
            .insert("ADX_CHECKPOINT_HANDOFF_FILE".into(), String::new());
        config.env.insert("ADX_ENV_FILE".into(), String::new());
        if let Some(info) = self.capabilities(&spec.runtime).await? {
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
        Ok(self.request(start_request(
            spec, runtime_id, generation, devices, &config,
        )?))
    }
    async fn execute_start(
        &self,
        runtime_id: &str,
        request: Request<proto::StartRequest>,
    ) -> Result<IpAddr> {
        let mut state = self.cell(runtime_id).lock_owned().await;
        if let StartState::Started(ip) = *state {
            return Ok(ip);
        }
        if matches!(*state, StartState::Uncertain) {
            return Err(unavailable("start outcome requires reconciliation"));
        }
        *state = StartState::Uncertain;
        let mut client = self.client.clone();
        let backend_ids = self.backend_ids.clone();
        let logical_id = runtime_id.to_string();
        tokio::spawn(async move {
            let response = match client.start(request).await {
                Ok(response) => response.into_inner(),
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
                    return Err(unavailable(error));
                }
            };
            // A completed response settles Start, even when its payload reports failure.
            *state = StartState::Settled;
            if !response.id.is_empty() {
                backend_ids
                    .lock()
                    .expect("shared state lock poisoned")
                    .insert(logical_id, response.id.clone());
            }
            if response.code != 0 {
                return Err(unavailable(response.message));
            }
            if response.id.is_empty() {
                *state = StartState::Uncertain;
                return Err(unavailable("Start returned no backend identity"));
            }
            let ip: IpAddr = response
                .sandbox_ip
                .parse()
                .map_err(|_| unavailable("Start did not return a valid sandbox IP"))?;
            if ip.is_unspecified() || ip.is_loopback() || ip.is_multicast() {
                return Err(unavailable("Start returned a non-routable sandbox IP"));
            }
            *state = StartState::Started(ip);
            Ok(ip)
        })
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
        let mut sandboxes = match response {
            Ok(response) => response.into_inner().sandboxes,
            Err(error) if physical.is_some() && error.code() == Code::NotFound => {
                return Ok(Vec::new())
            }
            Err(error) => return Err(unavailable(error)),
        };
        let mut discovered = BTreeMap::new();
        for sandbox in &mut sandboxes {
            if !sandbox.labels.contains_key("adx.instance_id") {
                continue;
            }
            let instance = &sandbox.labels["adx.instance_id"];
            let generation = sandbox
                .labels
                .get("adx.generation")
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
                .ok_or_else(|| unavailable("managed runtime has no valid generation"))?;
            let logical = sandbox
                .labels
                .get("adx.runtime_id")
                .filter(|id| adx_core::valid_runtime_id(instance, generation, id))
                .ok_or_else(|| unavailable("managed runtime has invalid execution labels"))?;
            if sandbox.id.is_empty()
                || instance.is_empty()
                || (!logical_id.is_empty() && logical != logical_id)
                || discovered
                    .insert(logical.clone(), sandbox.id.clone())
                    .is_some()
            {
                return Err(unavailable("ambiguous managed runtime identity"));
            }
            sandbox.id = logical.clone();
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
        Ok(sandboxes)
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

    pub async fn has_managed_instances(&self) -> Result<bool> {
        Ok(self
            .list_id("")
            .await?
            .iter()
            .any(|s| s.labels.contains_key("adx.instance_id")))
    }

    pub async fn is_running(&self, id: &str) -> Result<bool> {
        Ok(self
            .list_id(id)
            .await?
            .iter()
            .any(|x| x.id == id && x.state == proto::SandboxState::Running as i32))
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
    spec: &InstanceSpec,
    runtime_id: &str,
    ownership_generation: u64,
    devices: &[adx_core::scheduling::DeviceAllocation],
    config: &Config,
) -> Result<proto::StartRequest> {
    spec.validate()?;
    adx_core::scheduling::validate_device_assignment(&spec.scheduling.devices, devices)?;
    if runtime_id.is_empty()
        || ownership_generation == 0
        || (config.command.is_empty() && config.runtime_environment.is_none())
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
        spec.runtime_environment.as_ref(),
        config.runtime_environment.as_ref(),
    ) {
        return Err(Error::Invalid(
            "instance runtime environment differs from this node deployment".into(),
        ));
    }
    let environment = spec.runtime_environment.as_ref();
    let mut envs: HashMap<String, String> = spec.env.clone().into_iter().collect();
    if let Some(e) = environment {
        envs.extend(e.env.clone());
    }
    // Deployment configuration owns control ports, tokens, and execution identity.
    envs.extend(config.env.clone());
    envs.remove("ADX_RESTORE_ORIGIN");
    envs.insert("ADX_INSTANCE_ID".into(), spec.id.clone());
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
    let options = &spec.sandbox;
    let mut mounts = options
        .mounts
        .iter()
        .map(sandbox_mount)
        .collect::<Result<Vec<_>>>()?;
    if options.rootfs.is_some() || !spec.image.is_empty() {
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
    let image_process_config = environment.map_or(
        adx_core::environment::DEFAULT_IMAGE_PROCESS_CONFIG,
        |value| value.bootstrap.image_process_config.as_str(),
    );
    if options.inherit_entrypoint {
        envs.insert(
            "ADX_IMAGE_PROCESS_CONFIG".into(),
            image_process_config.into(),
        );
    }
    let cpu_limit = options.limits.cpu_millis.max(spec.resources.cpu_millis);
    let memory_limit = options.limits.memory_bytes.max(spec.resources.memory_bytes);
    let disk_limit = options.limits.disk_bytes.max(spec.resources.disk_bytes);
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
        runtime: spec.runtime.clone(),
        rootfs: Some(if let Some(rootfs) = &options.rootfs {
            sandbox_rootfs(rootfs)?
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
        resources: HashMap::from([
            ("CPU".into(), cpu_limit as f64),
            ("Memory".into(), memory_limit as f64 / 1_048_576.0),
        ]),
        labels: HashMap::from([
            ("adx.instance_id".into(), spec.id.clone()),
            ("adx.tenant_id".into(), spec.tenant_id.clone()),
            ("adx.runtime_id".into(), runtime_id.into()),
            ("adx.generation".into(), ownership_generation.to_string()),
        ]),
        writable_layer_limit_bytes: disk_limit,
        extra_config: options.extra_config.clone(),
        network_policy: options
            .network
            .as_ref()
            .map(|policy| sandbox_network_policy(policy, &options.ports)),
        inject_entrypoint: if options.inherit_entrypoint {
            image_process_config.into()
        } else {
            String::new()
        },
        ..Default::default()
    })
}

fn deployment_environment_accepts(
    requested: Option<&adx_core::environment::RuntimeEnvironment>,
    configured: Option<&adx_core::environment::RuntimeEnvironment>,
) -> bool {
    match (requested, configured) {
        (None, None) => true,
        (Some(requested), Some(configured)) => {
            // Instance rootfs settings are an overlay on the trusted node
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

fn sandbox_s3(value: &adx_core::sandbox::S3Source) -> proto::S3Config {
    proto::S3Config {
        endpoint: value.endpoint.clone(),
        bucket: value.bucket.clone(),
        object: value.object.clone(),
        access_key_id: value.access_key_id.clone(),
        access_key_secret: value.access_key_secret.clone(),
    }
}

fn sandbox_rootfs(value: &adx_core::sandbox::Rootfs) -> Result<proto::RootfsConfig> {
    use adx_core::sandbox::StorageSource;
    let (kind, source) = match &value.source {
        StorageSource::Image(value) => (
            proto::RootfsSrcType::Image,
            proto::rootfs_config::Source::ImageUrl(value.clone()),
        ),
        StorageSource::S3(value) => (
            proto::RootfsSrcType::S3,
            proto::rootfs_config::Source::S3Config(sandbox_s3(value)),
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

fn sandbox_mount(value: &adx_core::sandbox::Mount) -> Result<proto::Mount> {
    use adx_core::sandbox::StorageSource;
    let source = match &value.source {
        StorageSource::Image(value) => proto::mount::Source::ImageUrl(value.clone()),
        StorageSource::S3(value) => proto::mount::Source::S3Config(sandbox_s3(value)),
        StorageSource::Local(value) => proto::mount::Source::HostPath(value.clone()),
    };
    Ok(proto::Mount {
        r#type: value.kind.clone(),
        target: value.target.clone(),
        options: value.options.clone(),
        source: Some(source),
    })
}

fn sandbox_network_policy(
    value: &adx_core::sandbox::NetworkPolicy,
    ports: &[u16],
) -> proto::NetworkPolicy {
    use adx_core::sandbox as model;
    let action = |value| match value {
        model::NetworkAction::Allow => proto::NetworkPolicyAction::Allow as i32,
        model::NetworkAction::Deny => proto::NetworkPolicyAction::Deny as i32,
    };
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
                sandbox_port: rule.sandbox_port,
                sandbox_port_range: rule.sandbox_port_range.as_ref().map(|range| {
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
        for port in std::iter::once(50090u16).chain(ports.iter().copied()) {
            rules.push(proto::TrafficRule {
                action: proto::NetworkPolicyAction::Allow as i32,
                direction: proto::NetworkDirection::Ingress as i32,
                protocol: proto::NetworkProtocol::Tcp as i32,
                peer: Some(proto::NetworkEndpoint::default()),
                // sandboxd schema v2 forbids the legacy scalar
                // `sandbox_port`. An exact port is represented as a one-value
                // range, including platform-owned RRT and published-port
                // exceptions.
                sandbox_port_range: Some(proto::PortRange {
                    first: u32::from(port),
                    last: u32::from(port),
                }),
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
    proto::NetworkPolicy {
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
    }
}

#[async_trait]
impl RuntimeBackend for Sandboxd {
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
            _ => Err(Error::Invalid(
                "runtime does not provide checkpoint/restore handoff".into(),
            )),
        }
    }
    async fn checkpoint(&self, runtime_id: &str, path: &Path, duration: Duration) -> Result<()> {
        if !path.is_absolute() || duration.is_zero() {
            return Err(Error::Invalid(
                "absolute checkpoint directory and positive timeout required".into(),
            ));
        }
        let mut state = self.cell(runtime_id).lock_owned().await;
        if matches!(*state, StartState::Uncertain) {
            return Err(unavailable("execution outcome requires reconciliation"));
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
            leave_running: false,
            snapshot_type: "Full".into(),
        });
        request.set_timeout(duration + self.config.rpc_timeout);
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
    async fn restore(
        &self,
        spec: &InstanceSpec,
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
        spec: &InstanceSpec,
        runtime_id: &str,
        generation: u64,
        devices: &[adx_core::scheduling::DeviceAllocation],
        path: &Path,
        origin: Option<&adx_core::runtime::RuntimeIdentity>,
    ) -> Result<IpAddr> {
        self.checkpoint_supported(&spec.runtime).await?;
        if !path.is_absolute() {
            return Err(Error::Invalid("absolute checkpoint path required".into()));
        }
        let mut request = self
            .execution_request(spec, runtime_id, generation, devices)
            .await?;
        if let Some(origin) = origin {
            let context = adx_core::runtime::RuntimeRestore {
                target: adx_core::runtime::RuntimeIdentity {
                    instance_id: spec.id.clone(),
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
        self.execute_start(runtime_id, request).await
    }
    async fn inventory(&self) -> Result<Vec<crate::RuntimeObservation>> {
        self.list_id("")
            .await?
            .into_iter()
            .filter(|s| s.labels.contains_key("adx.instance_id"))
            .map(|s| {
                let instance_id = s.labels["adx.instance_id"].clone();
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
                    instance_id,
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
    async fn set_network_policy(
        &self,
        runtime_id: &str,
        policy: Option<&adx_core::sandbox::NetworkPolicy>,
        ports: &[u16],
    ) -> Result<()> {
        let id = self.physical_id(runtime_id).await?.ok_or(Error::NotFound)?;
        self.client
            .clone()
            .set_network_policy(self.request(proto::SetNetworkPolicyRequest {
                sandbox_id: id,
                network_policy: policy.map(|value| sandbox_network_policy(value, ports)),
            }))
            .await
            .map_err(unavailable)?;
        Ok(())
    }
    async fn start(
        &self,
        spec: &InstanceSpec,
        runtime_id: &str,
        ownership_generation: u64,
        devices: &[adx_core::scheduling::DeviceAllocation],
    ) -> Result<IpAddr> {
        let request = self
            .execution_request(spec, runtime_id, ownership_generation, devices)
            .await?;
        self.execute_start(runtime_id, request).await
    }

    async fn remove(&self, runtime_id: &str) -> Result<()> {
        let mut state = self.cell(runtime_id).lock_owned().await;
        if matches!(*state, StartState::Uncertain) {
            return Err(unavailable(
                "cannot confirm cleanup while Start outcome is unknown",
            ));
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
        *state = StartState::Idle;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    #[test]
    fn field_four_is_the_sandbox_ip() {
        let decoded = proto::StartResponse::decode(&b"\x22\x0810.0.0.2"[..]).unwrap();
        assert_eq!(decoded.sandbox_ip, "10.0.0.2");
        assert_eq!(
            proto::StartResponse::decode(&b""[..]).unwrap().sandbox_ip,
            ""
        );
    }

    #[test]
    fn request_preserves_execution_identity_image_and_resource_units() {
        let spec = InstanceSpec {
            runtime_environment: None,
            snapshot_id: None,
            lifecycle: Default::default(),
            env: [
                ("USER_VALUE".into(), "preserved".into()),
                ("ADX_INSTANCE_ID".into(), "spoofed-by-spec".into()),
                ("ADX_RESTORE_ORIGIN".into(), "spoofed-origin".into()),
            ]
            .into(),
            scheduling: Default::default(),
            id: "i".into(),
            tenant_id: "t".into(),
            image: "image:tag".into(),
            runtime: "runsc".into(),
            resources: adx_core::Resources {
                cpu_millis: 1500,
                memory_bytes: 2 * 1024 * 1024 * 1024,
                disk_bytes: 5 * 1024 * 1024 * 1024,
            },
            priority: 0,
            sandbox: Default::default(),
        };
        let default_request = start_request(&spec, "i-7", 7, &[], &Config::default()).unwrap();
        assert!(!default_request.envs.contains_key("RRT_HTTP_ONLY"));
        assert_eq!(default_request.envs["RRT_HTTP_PORT"], "50090");
        let config = Config {
            env: HashMap::from([
                ("ADX_INSTANCE_ID".into(), "spoofed".into()),
                ("USER_ENV".into(), "kept".into()),
            ]),
            ..Config::default()
        };
        let request = start_request(&spec, "i-7", 7, &[], &config).unwrap();
        assert!(!request.envs.contains_key("ADX_RESTORE_ORIGIN"));
        assert_eq!(request.envs["ADX_INSTANCE_ID"], "i");
        assert_eq!(request.envs["ADX_RUNTIME_ID"], "i-7");
        assert_eq!(request.envs["ADX_OWNERSHIP_GENERATION"], "7");
        assert_eq!(request.envs["USER_ENV"], "kept");
        assert_eq!(request.envs["USER_VALUE"], "preserved");
        assert!(start_request(&spec, "i-7", 0, &[], &config).is_err());
        assert!(request.sandbox_id.is_empty());
        assert_eq!(request.labels["adx.runtime_id"], "i-7");
        assert_eq!(request.labels["adx.generation"], "7");
        assert_eq!(request.runtime, "runsc");
        assert_eq!(request.resources["CPU"], 1500.0);
        assert_eq!(request.resources["Memory"], 2048.0);
        assert_eq!(request.writable_layer_limit_bytes, 5 * 1024 * 1024 * 1024);
        assert_eq!(
            request.rootfs.unwrap().source,
            Some(proto::rootfs_config::Source::ImageUrl("image:tag".into()))
        );
    }
}
