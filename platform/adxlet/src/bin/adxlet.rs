use adx_core::scheduling::NODE_HOST_UUID_LABEL;
use adx_process::{read_config, shutdown};
use adx_protocol::control as pb;
use adx_transport::rpc::{RpcChannel, RpcClient};
use adx_transport::tls::TlsFiles;
use adxlet::resources::{PressureGate, PressurePolicy, PressureResources, ResourceSource};
use adxlet::{
    jiuwenbox::Jiuwenbox,
    readiness::ExecdReadiness,
    routes::UdsRoutes,
    rpc::{CoordinatorStateSink, NodeRpc},
    runtime_logs::{RuntimeLogPolicy, RuntimeLogs},
    runtime_router::RuntimeRouter,
    sandboxd::{connect_when_ready, Config as RuntimeConfig},
    Adxlet,
};
use serde::Deserialize;
use std::{
    collections::{BTreeMap, HashMap},
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

const DMI_HOST_UUID_PATH: &str = "/sys/class/dmi/id/product_uuid";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    node_id: String,
    #[serde(default)]
    labels: HashMap<String, String>,
    #[serde(default)]
    storage_mounts: std::collections::BTreeMap<String, adx_core::volumes::StorageMount>,
    listen: SocketAddr,
    advertised_address: String,
    proxy_address: String,
    #[serde(default)]
    coordinator_address: Option<String>,
    #[serde(default)]
    discovery: Option<Discovery>,
    tls: TlsFiles,
    #[serde(default)]
    admin_socket: Option<PathBuf>,
    #[serde(default)]
    sandboxd_socket: Option<PathBuf>,
    #[serde(default)]
    jiuwenbox: Option<adxlet::jiuwenbox::Config>,
    #[serde(default)]
    runtime_logs: RuntimeLogPolicy,
    proxy_socket: PathBuf,
    #[serde(default)]
    proxy_mode: adxlet::proxy::ProxyMode,
    #[serde(default)]
    capacity_file: Option<PathBuf>,
    #[serde(default)]
    resource_source: Option<ResourceSource>,
    #[serde(default)]
    pressure: Option<PressureConfig>,
    #[serde(default)]
    metrics_listen: Option<SocketAddr>,
    #[serde(default)]
    execd_health_failure_threshold: Option<u32>,
    #[serde(default)]
    checkpoint_dir: Option<PathBuf>,
    #[serde(default)]
    checkpoint_storage: Option<adxlet::checkpoint::StorageConfig>,
    #[serde(default)]
    degradation_journal: Option<PathBuf>,
    report_interval_seconds: u64,
    #[serde(default = "metrics_interval")]
    metrics_interval_seconds: u64,
    rpc_timeout_seconds: u64,
    #[serde(default = "runtime_ready_timeout")]
    runtime_ready_timeout_seconds: u64,
    execd_port: u16,
    #[serde(default)]
    runtime_control: Option<adxlet::control_stream::StreamConfig>,
    #[serde(default)]
    runtime_profile: Option<adx_core::runtime_profile::RuntimeProfile>,
    #[serde(default)]
    checkpoint_runtime_binaries: BTreeMap<String, PathBuf>,
    #[serde(default)]
    execd_command: Vec<String>,
    #[serde(default)]
    execd_env: HashMap<String, String>,
    #[serde(default)]
    network_baseline: Option<adxlet::network_baseline::NetworkBaseline>,
    #[serde(default)]
    network_peer_aliases: BTreeMap<String, ipnet::Ipv4Net>,
    #[serde(default)]
    platform_ingress_sources: Option<Vec<ipnet::Ipv4Net>>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Discovery {
    redis_url: String,
    namespace: String,
}
#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct PressureConfig {
    memory_enabled: bool,
    disk_enabled: bool,
    disk_path: Option<PathBuf>,
    thresholds: PressurePolicy,
    pause: PressurePauseConfig,
}

impl Default for PressureConfig {
    fn default() -> Self {
        Self {
            memory_enabled: true,
            disk_enabled: true,
            disk_path: None,
            thresholds: PressurePolicy::default(),
            pause: PressurePauseConfig::default(),
        }
    }
}

impl PressureConfig {
    fn resources(&self) -> PressureResources {
        PressureResources {
            memory_enabled: self.memory_enabled,
            disk_enabled: self.disk_enabled,
        }
    }

    fn gate(&self) -> adx_core::Result<Option<PressureGate>> {
        if !self.resources().any_enabled() {
            return Ok(None);
        }
        if self.disk_enabled && self.disk_path.is_none() {
            return Err(adx_core::Error::Invalid(
                "pressure disk_path is required when disk_enabled is true".into(),
            ));
        }
        PressureGate::with_resources(self.thresholds.clone(), self.resources()).map(Some)
    }
}
#[derive(Deserialize, Clone)]
#[serde(default, deny_unknown_fields)]
struct PressurePauseConfig {
    enabled: bool,
    min_idle_seconds: u64,
    freshness_seconds: u64,
    budget: u32,
    in_flight_cap: usize,
    escalate_checkpoint_storage: adxlet::EscalationMode,
}
impl Default for PressurePauseConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            min_idle_seconds: 300,
            freshness_seconds: 90,
            budget: 1,
            in_flight_cap: 1,
            escalate_checkpoint_storage: adxlet::EscalationMode::default(),
        }
    }
}
impl From<PressurePauseConfig> for adxlet::PressurePauseOptions {
    fn from(config: PressurePauseConfig) -> Self {
        Self {
            enabled: config.enabled,
            min_idle_seconds: config.min_idle_seconds,
            freshness_seconds: config.freshness_seconds,
            budget: config.budget,
            in_flight_cap: config.in_flight_cap,
            escalate_checkpoint_storage: config.escalate_checkpoint_storage,
        }
    }
}
fn metrics_interval() -> u64 {
    15
}
fn runtime_ready_timeout() -> u64 {
    120
}
fn execd_ready_poll_interval() -> Duration {
    Duration::from_millis(10)
}

fn read_host_uuid(path: &Path) -> std::io::Result<uuid::Uuid> {
    let value = std::fs::read_to_string(path)?;
    let id = value.trim().parse::<uuid::Uuid>().map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "malformed DMI product UUID",
        )
    })?;
    if id.is_nil() || id.as_u128() == u128::MAX {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "placeholder DMI product UUID",
        ));
    }
    Ok(id)
}
async fn sample(
    source: &ResourceSource,
    runtime: &RuntimeRouter,
    timeout: Duration,
) -> adx_core::Result<(adxlet::resources::Observation, Duration)> {
    if matches!(source, ResourceSource::Sandboxd { .. }) {
        tokio::time::timeout(timeout, runtime.check_health())
            .await
            .map_err(|_| {
                adx_core::Error::Unavailable("sandboxd health check timed out".into())
            })??;
    }
    source.sample(timeout).await
}

async fn successor_bootstrap(
    parent: libc::pid_t,
) -> Result<Option<adx_protocol::process_bootstrap::ProcessBootstrap>, Box<dyn std::error::Error>> {
    use tokio::io::AsyncReadExt;
    let Some(path) = std::env::var_os("ADX_SUPERVISOR_PROCESS_BOOTSTRAP_SOCKET") else {
        return Ok(None);
    };
    let socket = tokio::net::UnixStream::connect(PathBuf::from(path)).await?;
    let peer = socket.peer_cred()?;
    // SAFETY: geteuid takes no pointers and only reads the kernel's effective user identity.
    let uid = unsafe { libc::geteuid() };
    // SAFETY: getppid takes no pointers and only reads the kernel's current parent identity.
    let current_parent = unsafe { libc::getppid() };
    if !adx_process::launch_parent_matches(parent, current_parent, peer.pid()) || peer.uid() != uid
    {
        return Err("bootstrap must be served by launch parent".into());
    }
    let mut bytes = Vec::new();
    tokio::time::timeout(
        Duration::from_secs(30),
        socket.take(4097).read_to_end(&mut bytes),
    )
    .await??;
    if bytes.len() > 4096 {
        return Err("bootstrap identity too large".into());
    }
    let bootstrap: adx_protocol::process_bootstrap::ProcessBootstrap =
        serde_json::from_slice(&bytes)?;
    if bootstrap.owner.epoch == 0
        || bootstrap.owner.node_id.is_empty()
        || bootstrap.owner.local_task_id.is_empty()
        || bootstrap.node_session.is_empty()
        || bootstrap.deadline_unix_millis <= adx_core::observation::now_millis()?
    {
        return Err("invalid or expired handoff bootstrap".into());
    }
    Ok(Some(bootstrap))
}

fn coordinator_channel(
    transport: &RpcClient,
    address: &str,
    timeout: Duration,
) -> Result<RpcChannel, std::io::Error> {
    // ForwardCreate can wait for new capacity beyond the ordinary RPC budget.
    // Response deadlines belong to each call; keep connection setup bounded.
    Ok(transport.wrap(
        transport
            .endpoint(address)?
            .connect_timeout(timeout)
            .connect_lazy(),
    ))
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::process::ExitCode {
    match run().await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            let configuration = error.is::<adx_process::ConfigurationError>();
            eprintln!("Adxlet initialization failed: {error}");
            std::process::ExitCode::from(if configuration {
                adx_process::CONFIGURATION_EXIT_CODE
            } else {
                1
            })
        }
    }
}

fn validate_config(
    config: &Config,
) -> Result<(ResourceSource, Option<PressureGate>), adx_process::ConfigurationError> {
    use adx_process::ConfigurationError;
    if let Some(baseline) = &config.network_baseline {
        baseline
            .validate()
            .map_err(|_| ConfigurationError("invalid network baseline configuration"))?;
    }
    adxlet::network_baseline::validate_peer_aliases(&config.network_peer_aliases)
        .map_err(|_| ConfigurationError("invalid network peer aliases"))?;
    if config
        .platform_ingress_sources
        .as_ref()
        .is_some_and(|sources| sources.is_empty())
    {
        return Err(ConfigurationError(
            "platform_ingress_sources must not be empty",
        ));
    }
    if config.sandboxd_socket.is_none() && config.jiuwenbox.is_none() {
        return Err(ConfigurationError(
            "configure sandboxd_socket and/or jiuwenbox",
        ));
    }
    if config.runtime_control.is_none() {
        return Err(ConfigurationError(
            "runtime_control is required for event-driven lifecycle observations",
        ));
    }
    for mount in config.storage_mounts.values() {
        mount
            .validate()
            .map_err(|_| ConfigurationError("invalid storage mount configuration"))?;
    }
    config
        .runtime_logs
        .validate()
        .map_err(|_| ConfigurationError("invalid runtime log policy"))?;
    if config.node_id.is_empty()
        || config.report_interval_seconds == 0
        || config.metrics_interval_seconds == 0
        || config.rpc_timeout_seconds == 0
        || config.runtime_ready_timeout_seconds == 0
    {
        return Err(ConfigurationError(
            "node identity and positive intervals required",
        ));
    }
    if !matches!(
        (&config.coordinator_address, &config.discovery),
        (Some(_), None) | (None, Some(_))
    ) {
        return Err(ConfigurationError(
            "configure exactly one of coordinator_address and discovery",
        ));
    }
    if config.checkpoint_dir.is_some() && config.checkpoint_storage.is_some() {
        return Err(ConfigurationError(
            "checkpoint_dir and checkpoint_storage are mutually exclusive",
        ));
    }
    if config.checkpoint_dir.is_none()
        && config.checkpoint_storage.is_none()
        && config
            .execd_env
            .get("ADX_EXECD_CONTROL_SOCKET_PATH")
            .is_some_and(|value| !value.is_empty())
    {
        return Err(ConfigurationError(
            "checkpoint socket requires checkpoint storage",
        ));
    }
    let source = match (config.capacity_file.clone(), config.resource_source.clone()) {
        (Some(path), None) => ResourceSource::File { path },
        (None, Some(source)) => source,
        _ => {
            return Err(ConfigurationError(
                "configure exactly one of capacity_file and resource_source",
            ))
        }
    };
    let gate = match &config.pressure {
        Some(pressure) => pressure
            .gate()
            .map_err(|_| ConfigurationError("invalid pressure thresholds"))?,
        None => None,
    };
    Ok((source, gate))
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    // Capture before discovery/reconciliation can wait or the parent can exit.
    // SAFETY: getppid has no pointer arguments or side effects.
    let launch_parent_pid = unsafe { libc::getppid() };
    let config: Config = read_config()?;
    let (source, mut pressure_gate) = validate_config(&config)?;
    let embedded_relay_config = if config.proxy_mode == adxlet::proxy::ProxyMode::Embedded {
        adx_process::resource::raise_nofile_soft_limit_from_env().map_err(
            |error| -> Box<dyn std::error::Error> {
                if adx_process::resource::is_configuration_error(&error) {
                    adx_process::ConfigurationError("invalid embedded Relay FD limit configuration")
                        .into()
                } else {
                    error.into()
                }
            },
        )?;
        Some(data_plane_gateway::config::RelayConfig::from_env().map_err(
            |error| -> Box<dyn std::error::Error> {
                if error.is_configuration_error() {
                    adx_process::ConfigurationError(
                        "invalid embedded Relay environment configuration",
                    )
                    .into()
                } else {
                    error.into()
                }
            },
        )?)
    } else {
        None
    };
    let successor = successor_bootstrap(launch_parent_pid).await?;
    if successor.is_some() && config.proxy_mode != adxlet::proxy::ProxyMode::Standalone {
        return Err("process successor requires standalone Relay".into());
    }
    let mut preserve =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::user_defined1())?;
    let runtime_logs = RuntimeLogs::new(config.runtime_logs.clone())?;
    // Embedded Proxy and Adxlet share this process-wide subscriber and
    // tracer provider; standalone Proxy initializes the same library itself.
    let _logging_guard = adx_observability::logging::init("adxlet", false)?;
    let (server_tls, client_tls, peers) = config
        .tls
        .load_rpc(adx_protocol::auth::Principal::Node(config.node_id.clone()))?;
    let timeout = Duration::from_secs(config.rpc_timeout_seconds);
    let listener = tokio::net::TcpListener::bind(config.listen).await?;
    let discovery = match (&config.coordinator_address, &config.discovery) {
        (Some(_), None) => None,
        (None, Some(d)) => Some(adx_discovery::RedisDiscovery::new(
            &d.redis_url,
            &d.namespace,
            timeout,
        )?),
        _ => return Err("configure exactly one of coordinator_address and discovery".into()),
    };
    let mut endpoint = loop {
        if let Some(address) = &config.coordinator_address {
            break address.clone();
        }
        match discovery
            .as_ref()
            .expect("missing Coordinator address implies configured discovery")
            .lookup()
            .await
        {
            Ok(found) => break found.address,
            Err(_) => {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(config.report_interval_seconds)) => {}
                    _ = shutdown() => return Ok(()),
                }
            }
        }
    };
    let channel = coordinator_channel(&client_tls, &endpoint, timeout)?;
    let session_id = successor
        .as_ref()
        .map(|bootstrap| bootstrap.node_session.clone())
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    if successor
        .as_ref()
        .is_some_and(|bootstrap| bootstrap.owner.node_id != config.node_id)
    {
        return Err("handoff owner node differs from configuration".into());
    }
    // Leave part of the operation deadline for a durable fallback after RPC timeout.
    let commit_timeout = if config.degradation_journal.is_some() {
        timeout / 2
    } else {
        timeout
    };
    let sink = Arc::new(
        CoordinatorStateSink::with_rpc_channel(channel.clone(), commit_timeout)?
            .with_session(session_id.clone()),
    );
    let journal = config
        .degradation_journal
        .map(|path| Arc::new(adxlet::journal::JournalSink::new(path, sink.clone())));
    let state_sink: Arc<dyn adxlet::StateSink> = match &journal {
        Some(journal) => journal.clone(),
        None => sink.clone(),
    };
    let mut coordinator = pb::coordinator_service_client::CoordinatorServiceClient::new(channel);
    let mut env = config.execd_env;
    if config.checkpoint_dir.is_some() || config.checkpoint_storage.is_some() {
        env.entry("ADX_EXECD_CONTROL_SOCKET_PATH".into())
            .or_insert_with(|| "/run/adx".into());
    } else if env
        .get("ADX_EXECD_CONTROL_SOCKET_PATH")
        .is_some_and(|v| !v.is_empty())
    {
        return Err("checkpoint socket requires checkpoint storage".into());
    }
    env.insert("EXECD_HTTP_PORT".into(), config.execd_port.to_string());
    let token = env.get("EXECD_HTTP_TOKEN").cloned();
    let stream_setup = config
        .runtime_control
        .as_ref()
        .map(|config| -> adx_core::Result<_> {
            let secret = config.load_key()?;
            env.insert(
                "ADX_RUNTIME_CONTROL_ADDRESS".into(),
                config.advertised_address.clone(),
            );
            // Host-only input to the Start adapter; removed before runtime injection.
            env.insert(
                "ADX_RUNTIME_CONTROL_SECRET".into(),
                String::from_utf8(secret.clone()).map_err(|_| {
                    adx_core::Error::Invalid("runtime control key must be UTF-8".into())
                })?,
            );
            Ok((
                config.listen,
                adxlet::control_stream::RuntimeControlHub::new(secret, timeout)?,
            ))
        })
        .transpose()?;
    let runtime_config = RuntimeConfig {
        runtime_profile: config.runtime_profile,
        checkpoint_runtime_binaries: config.checkpoint_runtime_binaries,
        command: config.execd_command,
        env,
        cwd: "/".into(),
        rpc_timeout: timeout,
        runtime_logs: Some(config.runtime_logs.clone()),
        network_baseline: config.network_baseline,
        network_peer_aliases: config.network_peer_aliases,
        platform_ingress_sources: config.platform_ingress_sources,
    };
    let jiuwenbox = match config.jiuwenbox {
        Some(backend) => Some(Jiuwenbox::connect(backend, runtime_config.clone()).await?),
        None => None,
    };
    let sandboxd = match config.sandboxd_socket {
        Some(socket) => Some(tokio::select! {
            result = connect_when_ready(socket, runtime_config,
                Duration::from_secs(config.runtime_ready_timeout_seconds),
                Duration::from_secs(config.report_interval_seconds)) => result?,
            _ = shutdown() => return Ok(()),
        }),
        None => None,
    };
    let runtime = Arc::new(RuntimeRouter::new(sandboxd, jiuwenbox)?);
    // The sandboxd network plugin may create the configured bridge during
    // initialization. Bind only after backend readiness, before admitting work.
    let stream_listener = if let Some((listen, _)) = &stream_setup {
        Some(tokio::net::TcpListener::bind(listen).await?)
    } else {
        None
    };
    let mut control =
        adxlet::runtime_control::RuntimeControlClient::new(config.execd_port, timeout)?;
    if let Some(token) = &token {
        control = control.with_token(token)?;
    }
    if let Some((_, hub)) = &stream_setup {
        control = control.with_stream(hub.clone());
    }
    let readiness = ExecdReadiness::with_client(
        runtime.clone(),
        control.clone(),
        execd_ready_poll_interval(),
        timeout,
        timeout,
    )?;
    // Only the route client follows a supervisor-selected generation. Stable
    // upgrade-control, activity and admin anchors remain in the original config.
    let route_target = std::env::var_os("ADX_SUPERVISOR_ACTIVE_RELAY_SOCKET")
        .map(PathBuf::from)
        .unwrap_or_else(|| config.proxy_socket.clone());
    let routes = Arc::new(UdsRoutes::new(route_target, timeout)?);
    let mut manager = Adxlet::new(
        config.node_id.clone(),
        runtime.clone(),
        Arc::new(readiness),
        routes.clone(),
        state_sink,
    )
    .with_runtime_control(control.clone())?
    .with_upgrade_control(
        config.proxy_socket.with_file_name("upgrade-control.json"),
        session_id.clone(),
    )?
    .with_operation_timeout(timeout)?
    .with_health_check(config.execd_health_failure_threshold)?;
    let checkpoint_storage = match (config.checkpoint_dir, config.checkpoint_storage) {
        (Some(root), None) => Some(adxlet::checkpoint::StorageConfig::Local { root }),
        (None, storage) => storage,
        (Some(_), Some(_)) => {
            return Err("configure only one of checkpoint_dir and checkpoint_storage".into())
        }
    };
    let mut checkpoint_scopes = Vec::new();
    if let Some(storage) = checkpoint_storage {
        let storage = storage.build_for_node(config.node_id.clone(), session_id.clone())?;
        checkpoint_scopes.extend(storage.shared_scope().map(str::to_owned));
        manager = manager
            .with_checkpointing(storage, Arc::new(control.clone()))?
            .with_snapshot_catalog(sink.clone())?;
    }
    if let Some(pressure) = config.pressure.as_ref().filter(|_| pressure_gate.is_some()) {
        manager = manager.with_pressure_pause_options(pressure.pause.clone().into())?;
        manager.mark_pressure_configured(pressure.resources());
    }
    let manager = Arc::new(manager);
    if successor.is_some() || std::env::var("ADX_NODE_UPGRADE_ISOLATED").as_deref() == Ok("true") {
        manager.isolate_upgrade_bootstrap()?;
    }
    manager.pause_lifecycle().await;
    let (o, valid) = loop {
        match sample(&source, &runtime, timeout).await {
            Ok(sample) => break sample,
            Err(error) => {
                adx_observability::warn!("waiting for first valid capacity observation: {error}");
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(config.report_interval_seconds)) => {}
                    _ = shutdown() => return Ok(()),
                }
            }
        }
    };
    manager.update_capacity(o.capacity, valid)?;
    manager.update_devices(o.devices.clone(), valid)?;
    let embedded = if config.proxy_mode == adxlet::proxy::ProxyMode::Embedded {
        Some(
            adxlet::proxy::EmbeddedProxy::start(
                embedded_relay_config.ok_or("embedded Relay configuration missing")?,
                &config.proxy_socket,
            )
            .await?,
        )
    } else {
        None
    };
    let mut labels = config.labels;
    labels.retain(|key, _| !key.starts_with("ADX_RECOVERY_") && key != "ADX_CHECKPOINT_SCOPE");
    labels.extend(runtime.recovery_labels());
    if let Some(scope) = checkpoint_scopes.first() {
        labels.insert("ADX_CHECKPOINT_SCOPE".into(), scope.clone());
    }
    labels.remove(NODE_HOST_UUID_LABEL);
    match read_host_uuid(Path::new(DMI_HOST_UUID_PATH)) {
        Ok(host_uuid) => {
            labels.insert(NODE_HOST_UUID_LABEL.into(), host_uuid.to_string());
        }
        Err(error) => adx_observability::warn!(
            node_id = %config.node_id,
            path = DMI_HOST_UUID_PATH,
            %error,
            "worker host UUID unavailable; registration will preserve any existing scheduling pause"
        ),
    }
    if let Some(alias) = manager.checkpoint_storage_alias() {
        labels.insert("ADX_CHECKPOINT_STORAGE".into(), alias.into());
    }
    let registration = pb::RegisterNodeRequest {
        storage_mounts: Default::default(),
        node_id: config.node_id.clone(),
        node_address: config.advertised_address,
        proxy_address: config.proxy_address,
        capacity: Some(o.capacity.into()),
        devices: o.devices.into_iter().map(Into::into).collect(),
        labels,
        accepting_allocations: false,
        session_id: session_id.clone(),
        heartbeat_sequence: 0,
        reconciling: true,
        runtime_classes: vec![],
        checkpoint_scopes,
        pressure: None,
        pressure_disposition: Default::default(),
    };
    // Bind before registration so dispatch cannot race an unbound address.
    let node_rpc = NodeRpc::new(manager.clone(), peers, session_id.clone())
        .with_local_creation((*sink).clone());
    let successor_ready = std::sync::atomic::AtomicBool::new(successor.is_none());
    let claim_retries = async {
        let mut interval =
            tokio::time::interval(Duration::from_secs(config.report_interval_seconds));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            if successor_ready.load(std::sync::atomic::Ordering::Acquire)
                && !manager.process_sealing()
            {
                node_rpc.retry_local_claims().await;
            }
        }
    };
    let mut builder = tonic::transport::Server::builder();
    if let Some(tls) = server_tls {
        builder = builder.tls_config(tls)?;
    }
    let server = builder
        .add_service(pb::node_service_server::NodeServiceServer::new(
            node_rpc.clone(),
        ))
        .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener));
    tokio::pin!(server);
    let process_channel = coordinator_channel(&client_tls, &endpoint, timeout)?;
    let (process_channels, current_process_channel) =
        tokio::sync::watch::channel(process_channel.clone());
    let admin = async {
        while !successor_ready.load(std::sync::atomic::Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        if let Some(path) = &config.admin_socket {
            adxlet::admin::serve_with_handoff(
                manager.clone(),
                path,
                routes.clone(),
                config.proxy_mode,
                launch_parent_pid,
                current_process_channel.clone(),
            )
            .await?;
        } else {
            std::future::pending::<()>().await;
        }
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    };
    let report = async {
        let mut last = registration.clone();
        let mut valid_until = tokio::time::Instant::now() + valid;
        // Resource/backend observation is not on the heartbeat critical path.
        // Bound each sample to one reporting period and keep the last successful
        // capacity during outages. Heartbeats continue independently.
        let sample_timeout = timeout.min(Duration::from_secs(config.report_interval_seconds));
        let mut interval =
            tokio::time::interval(Duration::from_secs(config.report_interval_seconds));
        let mut maintenance_completed = successor.is_none();
        let successor_deadline = match &successor {
            Some(bootstrap) => Some(
                tokio::time::Instant::now()
                    + Duration::from_millis(
                        bootstrap
                            .deadline_unix_millis
                            .saturating_sub(adx_core::observation::now_millis()?),
                    ),
            ),
            None => None,
        };
        loop {
            manager.wait_for_reconciliation(&mut interval).await;
            if let Some(discovery) = &discovery {
                match discovery.lookup().await {
                    Ok(found) if found.address != endpoint => {
                        let channel = coordinator_channel(&client_tls, &found.address, timeout)?;
                        process_channels.send_replace(channel.clone());
                        sink.reconnect(channel.clone())?;
                        coordinator =
                            pb::coordinator_service_client::CoordinatorServiceClient::new(channel);
                        endpoint = found.address;
                    }
                    // Existing channel may remain useful while Redis is unavailable.
                    _ => (),
                }
            }
            if !maintenance_completed {
                if let Some(bootstrap) = &successor {
                    let identity = adx_protocol::relay::AdxletProcessRequest {
                        rollout_id: bootstrap.owner.rollout_id.clone(),
                        local_task_id: bootstrap.owner.local_task_id.clone(),
                        epoch: bootstrap.owner.epoch,
                        ..Default::default()
                    };
                    if !successor_ready.load(std::sync::atomic::Ordering::Acquire)
                        && adx_core::observation::now_millis()? >= bootstrap.deadline_unix_millis
                    {
                        continue;
                    }
                    let query = pb::MaintenanceReleaseRequest {
                        owner: Some(bootstrap.owner.clone()),
                        node_session: last.session_id.clone(),
                    };
                    let channel = current_process_channel.borrow().clone();
                    let result = tokio::time::timeout(
                        timeout,
                        pb::node_handoff_service_client::NodeHandoffServiceClient::new(channel)
                            .query_release(query),
                    )
                    .await;
                    if successor_ready.load(std::sync::atomic::Ordering::Acquire) {
                        if let Ok(Ok(response)) = result {
                            let receipt = response.into_inner();
                            if receipt.owner.as_ref() == Some(&bootstrap.owner)
                                && manager
                                    .complete_successor_maintenance(&identity, &receipt)
                                    .await
                                    .is_ok()
                            {
                                maintenance_completed = true;
                                // Do not reuse this period's pre-release capacity report.
                                continue;
                            }
                        }
                    }
                    let cycle_deadline =
                        if successor_ready.load(std::sync::atomic::Ordering::Acquire) {
                            tokio::time::Instant::now() + timeout
                        } else {
                            successor_deadline.ok_or("successor deadline absent")?
                        };
                    match tokio::time::timeout_at(
                        cycle_deadline,
                        adxlet::node_reconciliation::reconcile_successor(
                            &mut coordinator,
                            &mut last,
                            &manager,
                            &routes,
                            &identity,
                            timeout,
                        ),
                    )
                    .await
                    {
                        Ok(Ok(())) => {
                            successor_ready.store(true, std::sync::atomic::Ordering::Release)
                        }
                        Ok(Err(error)) => {
                            adx_observability::warn!(
                                node_id = %last.node_id,
                                node_session = %last.session_id,
                                rollout = %identity.rollout_id,
                                local_task = %identity.local_task_id,
                                epoch = identity.epoch,
                                %error,
                                "protected successor handshake will retry"
                            );
                        }
                        Err(_) => {
                            adx_observability::warn!("protected successor observation timed out; retaining handshake state");
                        }
                    }
                    continue;
                }
            }
            if manager.process_sealing() {
                last.accepting_allocations = false;
                last.heartbeat_sequence = last
                    .heartbeat_sequence
                    .checked_add(1)
                    .ok_or("heartbeat sequence exhausted")?;
                if let Err(error) = coordinator.register_node(last.clone()).await {
                    adx_observability::warn!(%error, "sealed node heartbeat unavailable");
                }
                continue;
            }
            let mut r = last.clone();
            // Observe storage and runtime capabilities concurrently so storage
            // does not add another timeout period to the heartbeat path.
            let (mounts, classes) = tokio::join!(
                tokio::time::timeout(
                    sample_timeout,
                    adxlet::managed_mounts::mounted_storage(config.storage_mounts.clone()),
                ),
                tokio::time::timeout(sample_timeout, runtime.available_runtime_classes()),
            );
            // A failed storage observation withdraws only volume eligibility.
            r.storage_mounts = match mounts {
                Ok(Ok(mounts)) => mounts
                    .into_iter()
                    .map(|(id, mount)| (id, mount.into()))
                    .collect(),
                _ => Default::default(),
            };
            // A failed runtime observation closes new admission, while accepted
            // executions and heartbeats continue through reconciliation.
            r.runtime_classes = match classes {
                Ok(Ok(classes)) => classes,
                _ => vec![],
            };
            manager.update_runtime_classes(r.runtime_classes.clone())?;
            if let Ok((o, valid)) = sample(&source, &runtime, sample_timeout).await {
                valid_until = tokio::time::Instant::now() + valid;
                manager.update_capacity(o.capacity, valid)?;
                manager.update_devices(o.devices.clone(), valid)?;
                r.capacity = Some(o.capacity.into());
                r.devices = o.devices.into_iter().map(Into::into).collect();
            }
            if let (Some(config), Some(gate)) = (&config.pressure, &mut pressure_gate) {
                let path = config.disk_path.clone();
                let resources = config.resources();
                let update = match tokio::task::spawn_blocking(move || {
                    adxlet::resources::pressure(resources, path.as_deref())
                })
                .await
                {
                    Ok(Ok(pressure)) => gate.update(pressure),
                    Ok(Err(error)) => {
                        adx_observability::warn!(%error, "pressure sample failed; pressure gate is open");
                        gate.fail()
                    }
                    Err(error) => {
                        adx_observability::warn!(%error, "pressure sampling task failed; pressure gate is open");
                        gate.fail()
                    }
                };
                manager.set_pressure_state(&update);
                // Heartbeat field: direct observation-point fill, same
                // discipline as capacity/devices/storage_mounts above. The
                // producer holds the state it just published, so the report
                // and the board cannot drift.
                r.pressure = adxlet::resources::report_from(&update.state);
            }
            last = r;
            // Startup and every reporting period use the same complete authority
            // cycle for every configured runtime. The cycle publishes readiness
            // immediately after success, before waiting for the next report tick.
            match adxlet::node_reconciliation::reconcile_node(
                &mut coordinator,
                &mut last,
                &manager,
                journal.as_deref(),
                timeout,
                Duration::from_secs(config.report_interval_seconds),
                valid_until,
            )
            .await
            {
                Ok(()) => (),
                Err(error) => {
                    adx_observability::warn!(accepting_allocations=manager.accepting_allocations(), %error, "authoritative node reconciliation incomplete")
                }
            }
        }
        #[allow(unreachable_code)]
        Ok::<(), Box<dyn std::error::Error>>(())
    };
    let metrics = async {
        if let Some(bind) = config.metrics_listen {
            adxlet::metrics::serve(manager.clone(), bind).await?;
        } else {
            std::future::pending::<()>().await;
        }
        Ok::<(), std::io::Error>(())
    };
    let monitoring = async {
        let mut interval =
            tokio::time::interval(Duration::from_secs(config.metrics_interval_seconds));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            if !successor_ready.load(std::sync::atomic::Ordering::Acquire)
                || manager.process_sealing()
            {
                continue;
            }
            if let Err(error) = manager.sample_metrics().await {
                adx_observability::warn!("environment observation incomplete: {error}");
            }
        }
    };
    let log_gc = async {
        let mut interval =
            tokio::time::interval(Duration::from_secs(runtime_logs.interval_seconds()));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            if !successor_ready.load(std::sync::atomic::Ordering::Acquire)
                || manager.process_sealing()
            {
                continue;
            }
            let Some(active) = manager.retained_runtime_ids() else {
                continue;
            };
            let logs = runtime_logs.clone();
            match tokio::task::spawn_blocking(move || {
                logs.collect(&active, std::time::SystemTime::now())
            })
            .await
            {
                Ok(Ok(_)) => (),
                Ok(Err(error)) => adx_observability::warn!("runtime log GC failed: {error}"),
                Err(error) => adx_observability::warn!("runtime log GC task failed: {error}"),
            }
        }
    };
    adx_observability::info!("adxlet RPC listener ready");
    let control_server = async {
        if let (Some(listener), Some((_, hub))) = (stream_listener, &stream_setup) {
            hub.clone().serve(listener).await?;
        } else {
            std::future::pending::<()>().await;
        }
        Ok::<(), tonic::transport::Error>(())
    };
    let services = async {
        tokio::select! {
            result = control_server => result?,
            result = &mut server => result?,
            result = report => result?,
            result = metrics => result?,
            _ = claim_retries => {}
            _ = monitoring => {}
            _ = log_gc => {}
            result = admin => result.map_err(|error| -> Box<dyn std::error::Error> { error })?,
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    };
    let shutdown = async {
        tokio::select! {
            _ = preserve.recv() => adxlet::proxy::ShutdownMode::Preserve,
            _ = shutdown() => adxlet::proxy::ShutdownMode::Drain,
        }
    };
    let mode = adxlet::proxy::serve_until_shutdown(&manager, embedded, services, shutdown).await?;
    if let Err(error) = adxlet::node_reconciliation::finish_shutdown(
        mode,
        &mut coordinator,
        &registration,
        timeout.min(Duration::from_secs(5)),
    )
    .await
    {
        adx_observability::warn!(%error, "planned node departure unconfirmed; heartbeat expiry remains active");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_only_pressure_configuration_omits_disk_path() {
        let config = serde_json::from_value::<PressureConfig>(serde_json::json!({
            "memory_enabled": true,
            "disk_enabled": false,
            "thresholds": {"memory_high_percent": 90, "memory_low_percent": 80}
        }))
        .expect("memory-only pressure needs no disk path");
        assert!(config.memory_enabled);
        assert!(!config.disk_enabled);
        assert!(config.disk_path.is_none());
        assert!(config.gate().unwrap().is_some());
    }

    #[test]
    fn disabled_pressure_configuration_omits_disk_path() {
        let config = serde_json::from_value::<PressureConfig>(serde_json::json!({
            "memory_enabled": false,
            "disk_enabled": false
        }))
        .expect("disabled pressure needs no disk path");
        assert!(config.gate().unwrap().is_none());
    }

    #[test]
    fn default_pressure_configuration_enables_both_resources_and_requires_disk_path() {
        let config: PressureConfig = serde_json::from_value(serde_json::json!({
            "disk_path": "/opt/adx/data"
        }))
        .unwrap();
        assert!(config.memory_enabled && config.disk_enabled);
        assert!(config.gate().unwrap().is_some());
        let missing_path: PressureConfig = serde_json::from_value(serde_json::json!({})).unwrap();
        assert!(missing_path.gate().is_err());
        let missing_path: PressureConfig = serde_json::from_value(serde_json::json!({
            "memory_enabled": false, "disk_enabled": true
        }))
        .unwrap();
        assert!(missing_path.gate().is_err());
    }

    #[test]
    fn dmi_host_uuid_is_normalized() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("product_uuid");
        std::fs::write(&path, "ABCDEF12-3456-4789-ABCD-EF1234567890\n").unwrap();
        assert_eq!(
            read_host_uuid(&path).unwrap().to_string(),
            "abcdef12-3456-4789-abcd-ef1234567890"
        );
    }

    #[test]
    fn dmi_host_uuid_distinguishes_missing_and_invalid_values() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("product_uuid");
        assert_eq!(
            read_host_uuid(&path).unwrap_err().kind(),
            std::io::ErrorKind::NotFound
        );
        for value in [
            "not a UUID",
            "00000000-0000-0000-0000-000000000000",
            "ffffffff-ffff-ffff-ffff-ffffffffffff",
        ] {
            std::fs::write(&path, value).unwrap();
            assert_eq!(
                read_host_uuid(&path).unwrap_err().kind(),
                std::io::ErrorKind::InvalidData
            );
        }
    }

    // A single-method service probes the production Coordinator channel without
    // requiring Redis or reproducing Coordinator scheduling in this fixture.
    struct DelayedResponse;

    #[tonic::async_trait]
    impl pb::auth_service_server::AuthService for DelayedResponse {
        async fn verify_api_key(
            &self,
            _: tonic::Request<pb::VerifyApiKeyRequest>,
        ) -> Result<tonic::Response<pb::VerifyApiKeyResponse>, tonic::Status> {
            tokio::time::sleep(Duration::from_millis(250)).await;
            Ok(tonic::Response::new(pb::VerifyApiKeyResponse::default()))
        }
    }

    #[tokio::test]
    async fn coordinator_channel_honors_long_and_short_request_deadlines() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(pb::auth_service_server::AuthServiceServer::new(
                    DelayedResponse,
                ))
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    async {
                        let _ = stopped.await;
                    },
                )
                .await
                .unwrap();
        });
        let transport = RpcClient::network(adx_protocol::auth::Principal::Node("n".into()));
        let channel =
            coordinator_channel(&transport, &address.to_string(), Duration::from_millis(100))
                .unwrap();
        let mut client = pb::auth_service_client::AuthServiceClient::new(channel);

        // ForwardCreate already supplies its full remaining create budget.
        // The channel must allow a response later than the ordinary RPC cap.
        let mut long = tonic::Request::new(pb::VerifyApiKeyRequest::default());
        long.set_timeout(Duration::from_secs(2));
        let long_result = client.verify_api_key(long).await;

        // Removing the channel cap must still respect each short RPC deadline.
        let mut short = tonic::Request::new(pb::VerifyApiKeyRequest::default());
        short.set_timeout(Duration::from_millis(50));
        let short_result = client.verify_api_key(short).await;
        let _ = stop.send(());
        server.await.unwrap();

        assert!(
            long_result.is_ok(),
            "long request was cut short: {long_result:?}"
        );
        let error = short_result.unwrap_err();
        assert!(
            matches!(
                error.code(),
                tonic::Code::Cancelled | tonic::Code::DeadlineExceeded
            ),
            "{error}"
        );
    }

    #[test]
    fn jiuwenbox_example_configures_a_node_without_sandboxd() {
        let config: Config = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../build/config/examples/node-jiuwenbox.json"
        )))
        .unwrap();
        assert!(config.sandboxd_socket.is_none());
        config.jiuwenbox.as_ref().unwrap().validate().unwrap();
        assert!(matches!(
            config.resource_source,
            Some(ResourceSource::Auto { .. })
        ));
        assert!(config.checkpoint_dir.is_none());
    }

    #[test]
    fn execd_readiness_poll_keeps_startup_jitter_below_twenty_milliseconds() {
        assert!(execd_ready_poll_interval() <= Duration::from_millis(20));
    }
}
